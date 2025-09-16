// Copyright Rouven Bauer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::{Debug, Display, Formatter};
use std::num::FpCategory;

/// Vector types to be exchanged with the DBMS.
///
/// Vector types require the server to support Bolt protocol version 6.0 or higher.
///
/// # Examples
/// ```
/// use neo4j::value::vector::Vector;
/// use neo4j::ValueSend;
///
/// let vec_f64 = ValueSend::Vector(vec![1.0, 2.0, 3.0].into());
/// assert_eq!(vec_f64, ValueSend::Vector(Vector::F64(vec![1.0, 2.0, 3.0])));
///
/// let vec_f32 = ValueSend::Vector(vec![1.0f32, 2.0, 3.0].into());
/// assert_eq!(vec_f32, ValueSend::Vector(Vector::F32(vec![1.0, 2.0, 3.0])));
///
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Vector {
    F64(Vec<f64>),
    F32(Vec<f32>),
    I64(Vec<i64>),
    I32(Vec<i32>),
    I16(Vec<i16>),
    I8(Vec<i8>),
}

impl Vector {
    pub(crate) fn eq_data(&self, other: &Self) -> bool {
        trait FloatBits: Copy {
            type Out: Eq;

            fn to_bits(self) -> Self::Out;
        }

        impl FloatBits for f64 {
            type Out = u64;

            fn to_bits(self) -> Self::Out {
                self.to_bits()
            }
        }

        impl FloatBits for f32 {
            type Out = u32;

            fn to_bits(self) -> Self::Out {
                self.to_bits()
            }
        }

        fn float_bits_vec_eq<T: FloatBits>(v1: &[T], v2: &[T]) -> bool {
            if v1.len() != v2.len() {
                return false;
            }
            let mut zip_iter = v1.iter().zip(v2.iter());
            zip_iter.all(|(v1, v2)| v1.to_bits() == v2.to_bits())
        }

        match self {
            Self::F64(v1) => matches!(other, Self::F64(v2) if float_bits_vec_eq(v1, v2)),
            Self::F32(v1) => matches!(other, Self::F32(v2) if float_bits_vec_eq(v1, v2)),
            Self::I64(v1) => matches!(other, Self::I64(v2) if v1 == v2),
            Self::I32(v1) => matches!(other, Self::I32(v2) if v1 == v2),
            Self::I16(v1) => matches!(other, Self::I16(v2) if v1 == v2),
            Self::I8(v1) => matches!(other, Self::I8(v2) if v1 == v2),
        }
    }
}

impl Display for Vector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("vector([")?;
        let (dim, dtype) = match self {
            Vector::F64(vec) => {
                fn display_f64(v: &f64) -> Option<&'static str> {
                    match v.classify() {
                        FpCategory::Nan => Some("NaN"),
                        FpCategory::Infinite => match v.is_sign_positive() {
                            true => Some("Infinity"),
                            false => Some("-Infinity"),
                        },
                        _ => None,
                    }
                }
                format_transformed_vec_elements(vec, display_f64, f)?;
                (vec.len(), "FLOAT NOT NULL")
            }
            Vector::F32(vec) => {
                fn display_f32(v: &f32) -> Option<&'static str> {
                    match v.classify() {
                        FpCategory::Nan => Some("NaN"),
                        FpCategory::Infinite => match v.is_sign_positive() {
                            true => Some("Infinity"),
                            false => Some("-Infinity"),
                        },
                        _ => None,
                    }
                }
                format_transformed_vec_elements(vec, display_f32, f)?;
                (vec.len(), "FLOAT32 NOT NULL")
            }
            Vector::I64(vec) => {
                format_vec_elements(vec, f)?;
                (vec.len(), "INTEGER NOT NULL")
            }
            Vector::I32(vec) => {
                format_vec_elements(vec, f)?;
                (vec.len(), "INTEGER32 NOT NULL")
            }
            Vector::I16(vec) => {
                format_vec_elements(vec, f)?;
                (vec.len(), "INTEGER16 NOT NULL")
            }
            Vector::I8(vec) => {
                format_vec_elements(vec, f)?;
                (vec.len(), "INTEGER8 NOT NULL")
            }
        };
        f.write_str("], ")?;
        Display::fmt(&dim, f)?;
        f.write_str(", ")?;
        f.write_str(dtype)?;
        f.write_str(")")
    }
}

fn format_transformed_vec_elements<T: Debug, R: Display>(
    elements: &[T],
    mut transform: impl FnMut(&T) -> Option<R>,
    f: &mut Formatter<'_>,
) -> std::fmt::Result {
    fn fmt<T: Debug, R: Display>(
        e: &T,
        mut transform: impl FnMut(&T) -> Option<R>,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        match transform(e) {
            None => Debug::fmt(e, f),
            Some(e) => Display::fmt(&e, f),
        }
    }

    let mut elements = elements.iter();
    let Some(first) = elements.next() else {
        return Ok(());
    };
    fmt(first, &mut transform, f)?;
    elements.try_for_each(|next| {
        f.write_str(", ")?;
        fmt(next, &mut transform, f)?;
        Ok(())
    })?;
    Ok(())
}

fn format_vec_elements<T: Debug>(elements: &[T], f: &mut Formatter<'_>) -> std::fmt::Result {
    let mut elements = elements.iter();
    let Some(first) = elements.next() else {
        return Ok(());
    };
    Debug::fmt(first, f)?;
    elements.try_for_each(|next| {
        f.write_str(", ")?;
        Debug::fmt(next, f)?;
        Ok(())
    })?;
    Ok(())
}

impl From<Vec<f64>> for Vector {
    fn from(value: Vec<f64>) -> Self {
        Vector::F64(value)
    }
}

impl TryFrom<Vector> for Vec<f64> {
    type Error = Vector;

    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::F64(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Vector {
    #[inline]
    pub fn as_vec_f64(&self) -> Option<&Vec<f64>> {
        match self {
            Vector::F64(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_vec_f64_mut(&mut self) -> Option<&mut Vec<f64>> {
        match self {
            Vector::F64(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_vec_f64(self) -> Result<Vec<f64>, Self> {
        self.try_into()
    }
}

impl From<Vec<f32>> for Vector {
    fn from(value: Vec<f32>) -> Self {
        Vector::F32(value)
    }
}

impl TryFrom<Vector> for Vec<f32> {
    type Error = Vector;

    #[inline]
    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::F32(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Vector {
    #[inline]
    pub fn as_vec_f32(&self) -> Option<&Vec<f32>> {
        match self {
            Vector::F32(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_vec_f32_mut(&mut self) -> Option<&mut Vec<f32>> {
        match self {
            Vector::F32(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_vec_f32(self) -> Result<Vec<f32>, Self> {
        self.try_into()
    }
}

impl From<Vec<i64>> for Vector {
    fn from(value: Vec<i64>) -> Self {
        Vector::I64(value)
    }
}

impl TryFrom<Vector> for Vec<i64> {
    type Error = Vector;

    #[inline]
    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::I64(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Vector {
    #[inline]
    pub fn as_vec_i64(&self) -> Option<&Vec<i64>> {
        match self {
            Vector::I64(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_vec_i64_mut(&mut self) -> Option<&mut Vec<i64>> {
        match self {
            Vector::I64(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_vec_i64(self) -> Result<Vec<i64>, Self> {
        self.try_into()
    }
}

impl From<Vec<i32>> for Vector {
    fn from(value: Vec<i32>) -> Self {
        Vector::I32(value)
    }
}

impl TryFrom<Vector> for Vec<i32> {
    type Error = Vector;

    #[inline]
    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::I32(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Vector {
    #[inline]
    pub fn as_vec_i32(&self) -> Option<&Vec<i32>> {
        match self {
            Vector::I32(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_vec_i32_mut(&mut self) -> Option<&mut Vec<i32>> {
        match self {
            Vector::I32(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_vec_i32(self) -> Result<Vec<i32>, Self> {
        self.try_into()
    }
}

impl From<Vec<i16>> for Vector {
    fn from(value: Vec<i16>) -> Self {
        Vector::I16(value)
    }
}

impl TryFrom<Vector> for Vec<i16> {
    type Error = Vector;

    #[inline]
    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::I16(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Vector {
    #[inline]
    pub fn as_vec_i16(&self) -> Option<&Vec<i16>> {
        match self {
            Vector::I16(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_vec_i16_mut(&mut self) -> Option<&mut Vec<i16>> {
        match self {
            Vector::I16(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_vec_i16(self) -> Result<Vec<i16>, Self> {
        self.try_into()
    }
}

impl From<Vec<i8>> for Vector {
    fn from(value: Vec<i8>) -> Self {
        Vector::I8(value)
    }
}

impl TryFrom<Vector> for Vec<i8> {
    type Error = Vector;

    #[inline]
    fn try_from(value: Vector) -> Result<Self, Self::Error> {
        match value {
            Vector::I8(v) => Ok(v),
            _ => Err(value),
        }
    }
}

impl Vector {
    #[inline]
    pub fn as_vec_i8(&self) -> Option<&Vec<i8>> {
        match self {
            Vector::I8(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    pub fn as_vec_i8_mut(&mut self) -> Option<&mut Vec<i8>> {
        match self {
            Vector::I8(v) => Some(v),
            _ => None,
        }
    }

    #[inline]
    #[allow(clippy::result_large_err)]
    pub fn try_into_vec_i8(self) -> Result<Vec<i8>, Self> {
        self.try_into()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use rstest::rstest;

    #[rstest]
    // empty vectors
    #[case::i8_empty(
        Vector::I8(vec!()),
        "vector([], 0, INTEGER8 NOT NULL)",
    )]
    #[case::i16_empty(
        Vector::I16(vec!()),
        "vector([], 0, INTEGER16 NOT NULL)",
    )]
    #[case::i32_empty(
        Vector::I32(vec!()),
        "vector([], 0, INTEGER32 NOT NULL)",
    )]
    #[case::i64_empty(
        Vector::I64(vec!()),
        "vector([], 0, INTEGER NOT NULL)",
    )]
    #[case::f32_empty(
        Vector::F32(vec!()),
        "vector([], 0, FLOAT32 NOT NULL)",
    )]
    #[case::f64_empty(
        Vector::F64(vec!()),
        "vector([], 0, FLOAT NOT NULL)",
    )]
    // vectors with a few elements
    #[case::i8_some(
        Vector::I8(vec!(1, 2, 3, 4)),
        "vector([1, 2, 3, 4], 4, INTEGER8 NOT NULL)",
    )]
    #[case::i16_some(
        Vector::I16(vec!(1, 2, 3, 4)),
        "vector([1, 2, 3, 4], 4, INTEGER16 NOT NULL)",
    )]
    #[case::i32_some(
        Vector::I32(vec!(1, 2, 3, 4)),
        "vector([1, 2, 3, 4], 4, INTEGER32 NOT NULL)",
    )]
    #[case::i64_some(
        Vector::I64(vec!(1, 2, 3, 4)),
        "vector([1, 2, 3, 4], 4, INTEGER NOT NULL)",
    )]
    #[case::f32_some(
        Vector::F32(vec!(1.2, 3.4, 5.6, 7.8)),
        "vector([1.2, 3.4, 5.6, 7.8], 4, FLOAT32 NOT NULL)",
    )]
    #[case::f64_some(
        Vector::F64(vec!(1.2, 3.4, 5.6, 7.8)),
        "vector([1.2, 3.4, 5.6, 7.8], 4, FLOAT NOT NULL)",
    )]
    // i8 edge cases
    #[case::i8_min(
        Vector::I8(vec![i8::MIN]),
        "vector([-128], 1, INTEGER8 NOT NULL)",
    )]
    #[case::i8_zero(
        Vector::I8(vec![0]),
        "vector([0], 1, INTEGER8 NOT NULL)",
    )]
    #[case::i8_max(
        Vector::I8(vec![i8::MAX]),
        "vector([127], 1, INTEGER8 NOT NULL)",
    )]
    // i16 edge cases
    #[case::i16_min(
        Vector::I16(vec![i16::MIN]),
        "vector([-32768], 1, INTEGER16 NOT NULL)",
    )]
    #[case::i16_zero(
        Vector::I16(vec![0]),
        "vector([0], 1, INTEGER16 NOT NULL)",
    )]
    #[case::i16_max(
        Vector::I16(vec![i16::MAX]),
        "vector([32767], 1, INTEGER16 NOT NULL)",
    )]
    // i32 edge cases
    #[case::i32_min(
        Vector::I32(vec![i32::MIN]),
        "vector([-2147483648], 1, INTEGER32 NOT NULL)",
    )]
    #[case::i32_zero(
        Vector::I32(vec![0]),
        "vector([0], 1, INTEGER32 NOT NULL)",
    )]
    #[case::i32_max(
        Vector::I32(vec![i32::MAX]),
        "vector([2147483647], 1, INTEGER32 NOT NULL)",
    )]
    // i64 edge cases
    #[case::i64_min(
        Vector::I64(vec![i64::MIN]),
        "vector([-9223372036854775808], 1, INTEGER NOT NULL)",
    )]
    #[case::i64_zero(
        Vector::I64(vec![0]),
        "vector([0], 1, INTEGER NOT NULL)",
    )]
    #[case::i64_max(
        Vector::I64(vec![i64::MAX]),
        "vector([9223372036854775807], 1, INTEGER NOT NULL)",
    )]
    // f32 edge cases
    // NaN
    #[case::f32_nan(
        Vector::F32(vec![f32::NAN]),
        "vector([NaN], 1, FLOAT32 NOT NULL)",
    )]
    #[case::f32_neg_nan(
        Vector::F32(vec![-f32::NAN]),
        "vector([NaN], 1, FLOAT32 NOT NULL)",
    )]
    #[case::f32_nan_payload(
        Vector::F32(vec![f32::from_bits(0x7fc00011)]),
        "vector([NaN], 1, FLOAT32 NOT NULL)",
    )]
    #[case::f32_nan_payload(
        Vector::F32(vec![f32::from_bits(0x7f800001)]),
        "vector([NaN], 1, FLOAT32 NOT NULL)",
    )]
    // ±inf
    #[case::f32_inf(
        Vector::F32(vec![f32::INFINITY]),
        "vector([Infinity], 1, FLOAT32 NOT NULL)",
    )]
    #[case::f32_neg_inf(
        Vector::F32(vec![f32::NEG_INFINITY]),
        "vector([-Infinity], 1, FLOAT32 NOT NULL)",
    )]
    // ±0.0
    #[case::f32_zero(
        Vector::F32(vec![0.0]),
        "vector([0.0], 1, FLOAT32 NOT NULL)",
    )]
    #[case::f32_neg_zero(
        Vector::F32(vec![-0.0]),
        "vector([-0.0], 1, FLOAT32 NOT NULL)",
    )]
    // smallest normal
    #[case::f32_min(
        Vector::F32(vec![f32::from_bits(0x00800000)]),
        "vector([1.1754944e-38], 1, FLOAT32 NOT NULL)",
    )]
    #[case::f32_neg_min(
        Vector::F32(vec![f32::from_bits(0x80800000)]),
        "vector([-1.1754944e-38], 1, FLOAT32 NOT NULL)",
    )]
    // subnormal
    #[case::f32_subnormal(
        Vector::F32(vec![f32::from_bits(0x00000001)]),
        "vector([1e-45], 1, FLOAT32 NOT NULL)",
    )]
    #[case::f32_neg_subnormal(
        Vector::F32(vec![f32::from_bits(0x80000001)]),
        "vector([-1e-45], 1, FLOAT32 NOT NULL)",
    )]
    // largest normal
    #[case::f32_max(
        Vector::F32(vec![f32::from_bits(0x7f7fffff)]),
        "vector([3.4028235e38], 1, FLOAT32 NOT NULL)",
    )]
    #[case::f32_neg_max(
        Vector::F32(vec![f32::from_bits(0xff7fffff)]),
        "vector([-3.4028235e38], 1, FLOAT32 NOT NULL)",
    )]
    // f64 edge cases
    // NaN
    #[case::f64_nan(
        Vector::F64(vec![f64::NAN]),
        "vector([NaN], 1, FLOAT NOT NULL)",
    )]
    #[case::f64_neg_nan(
        Vector::F64(vec![-f64::NAN]),
        "vector([NaN], 1, FLOAT NOT NULL)",
    )]
    #[case::f64_nan_payload(
        Vector::F64(vec![f64::from_bits(0x7ff8000000000011)]),
        "vector([NaN], 1, FLOAT NOT NULL)",
    )]
    #[case::f64_nan_payload(
        Vector::F64(vec![f64::from_bits(0x7ff0000100000001)]),
        "vector([NaN], 1, FLOAT NOT NULL)",
    )]
    // ±inf
    #[case::f64_inf(
        Vector::F64(vec![f64::INFINITY]),
        "vector([Infinity], 1, FLOAT NOT NULL)",
    )]
    #[case::f64_neg_inf(
        Vector::F64(vec![f64::NEG_INFINITY]),
        "vector([-Infinity], 1, FLOAT NOT NULL)",
    )]
    // ±0.0
    #[case::f64_zero(
        Vector::F64(vec![0.0]),
        "vector([0.0], 1, FLOAT NOT NULL)",
    )]
    #[case::f64_neg_zero(
        Vector::F64(vec![-0.0]),
        "vector([-0.0], 1, FLOAT NOT NULL)",
    )]
    // smallest normal
    #[case::f64_min(
        Vector::F64(vec![f64::from_bits(0x0010000000000000)]),
        "vector([2.2250738585072014e-308], 1, FLOAT NOT NULL)",
    )]
    #[case::f64_neg_min(
        Vector::F64(vec![f64::from_bits(0x8010000000000000)]),
        "vector([-2.2250738585072014e-308], 1, FLOAT NOT NULL)",
    )]
    // subnormal
    #[case::f64_subnormal(
        Vector::F64(vec![f64::from_bits(0x0000000000000001)]),
        "vector([5e-324], 1, FLOAT NOT NULL)",
    )]
    #[case::f64_neg_subnormal(
        Vector::F64(vec![f64::from_bits(0x8000000000000001)]),
        "vector([-5e-324], 1, FLOAT NOT NULL)",
    )]
    // largest normal
    #[case::f64_max(
        Vector::F64(vec![f64::from_bits(0x7fefffffffffffff)]),
        "vector([1.7976931348623157e308], 1, FLOAT NOT NULL)",
    )]
    #[case::f64_neg_max(
        Vector::F64(vec![f64::from_bits(0xffefffffffffffff)]),
        "vector([-1.7976931348623157e308], 1, FLOAT NOT NULL)",
    )]
    fn test_vector_display(#[case] vector: Vector, #[case] expected: &str) {
        let display = format!("{vector}");
        assert_eq!(&display, expected);
    }
}
