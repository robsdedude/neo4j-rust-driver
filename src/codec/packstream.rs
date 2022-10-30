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

use std::collections::HashMap;

use thiserror::Error;

#[derive(Error, Debug)]
#[error("{reason}")]
pub struct PackStreamError {
    reason: String,
}

impl From<&str> for PackStreamError {
    fn from(reason: &str) -> Self {
        PackStreamError {
            reason: String::from(reason),
        }
    }
}

impl From<String> for PackStreamError {
    fn from(reason: String) -> Self {
        PackStreamError { reason }
    }
}

#[derive(Debug, PartialEq)]
pub enum PackStreamValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<PackStreamValue>),
    Dictionary(HashMap<String, PackStreamValue>),
    Structure(PackStreamStructure),
}

macro_rules! impl_into_pack_stream {
    ( $pack_stream_ty:expr, $($ty:ty),* ) => {
        $(
            impl From<$ty> for PackStreamValue {
                fn from(value: $ty) -> Self {
                    $pack_stream_ty(value.into())
                }
            }
        )*
    };
}

impl_into_pack_stream!(PackStreamValue::Boolean, bool);
impl_into_pack_stream!(PackStreamValue::Integer, i8, i16, i32, i64);
impl_into_pack_stream!(PackStreamValue::Float, f64);
impl_into_pack_stream!(PackStreamValue::Bytes, Vec<u8>);
impl_into_pack_stream!(PackStreamValue::String, String);
impl_into_pack_stream!(PackStreamValue::List, Vec<PackStreamValue>);
impl_into_pack_stream!(PackStreamValue::Dictionary, HashMap<String, PackStreamValue>);
impl_into_pack_stream!(PackStreamValue::Structure, PackStreamStructure);

#[derive(Debug, PartialEq)]
pub struct PackStreamStructure {
    pub tag: u8,
    pub size: u8,
    pub fields: Vec<PackStreamValue>,
}

pub fn decode(stream: &mut impl Iterator<Item = u8>) -> Result<PackStreamValue, PackStreamError> {
    // TODO: proper errors?
    let marker = stream.next().ok_or("no marker found")?;
    if marker == 0xC0 {
        Ok(PackStreamValue::Null)
    } else if marker == 0xC2 {
        Ok(PackStreamValue::Boolean(false))
    } else if marker == 0xC3 {
        Ok(PackStreamValue::Boolean(true))
    } else if 0xF0 <= marker || marker <= 0x7F {
        Ok(PackStreamValue::Integer(i8::from_be_bytes([marker]).into()))
    } else if marker == 0xC8 {
        decode_i8(stream)
    } else if marker == 0xC9 {
        decode_i16(stream)
    } else if marker == 0xCA {
        decode_i32(stream)
    } else if marker == 0xCB {
        decode_i64(stream)
    } else if marker == 0xC1 {
        decode_f64(stream)
    } else if marker == 0xCC {
        decode_bytes_u8(stream)
    } else if marker == 0xCD {
        decode_bytes_u16(stream)
    } else if marker == 0xCE {
        decode_bytes_u32(stream)
    } else if 0x80 <= marker && marker <= 0x8F {
        let size = marker - 0x80;
        decode_string(stream, size as usize)
    } else if marker == 0xD0 {
        decode_string_u8(stream)
    } else if marker == 0xD1 {
        decode_string_u16(stream)
    } else if marker == 0xD2 {
        decode_string_u32(stream)
    } else {
        Err(PackStreamError::from(format!("unknown marker {}", marker)))
    }
}

macro_rules! primitive_decoder {
    ( $name:ident, $primitive_t:ty, $size:expr ) => {
        fn $name(stream: &mut impl Iterator<Item = u8>) -> Result<PackStreamValue, PackStreamError> {
            let mut buffer = [0; $size];
            for byte in buffer.iter_mut() {
                *byte = stream.next().ok_or(stringify!(not enough data after $primitive_t marker))?
            }
            let value = <$primitive_t>::from_be_bytes(buffer);
            Ok(value.into())
        }
    };
}

primitive_decoder!(decode_i8, i8, 1);
primitive_decoder!(decode_i16, i16, 2);
primitive_decoder!(decode_i32, i32, 4);
primitive_decoder!(decode_i64, i64, 8);
primitive_decoder!(decode_f64, f64, 8);

macro_rules! bytes_decoder {
    ( $name:ident, $header_t:ty, $size:expr ) => {
        fn $name(
            stream: &mut impl Iterator<Item = u8>,
        ) -> Result<PackStreamValue, PackStreamError> {
            let mut size_buffer = [0; $size];
            for byte in size_buffer.iter_mut() {
                *byte = stream.next().ok_or("incomplete bytes size")?;
            }
            let size = <$header_t>::from_be_bytes(size_buffer);
            if (size as u128).saturating_mul(8) >= usize::MAX as u128 {
                panic!("server wants to send more bytes than are addressable")
            }
            let mut bytes = Vec::with_capacity(size as usize);
            for _ in 0..size {
                bytes.push(stream.next().ok_or("less bytes than announced")?);
            }
            Ok(bytes.into())
        }
    };
}

bytes_decoder!(decode_bytes_u8, u8, 1);
bytes_decoder!(decode_bytes_u16, u16, 2);
bytes_decoder!(decode_bytes_u32, u32, 4);

macro_rules! string_decoder {
    ( $name:ident, $header_t:ty, $size:expr ) => {
        fn $name(
            stream: &mut impl Iterator<Item = u8>,
        ) -> Result<PackStreamValue, PackStreamError> {
            let mut size_buffer = [0; $size];
            for byte in size_buffer.iter_mut() {
                *byte = stream.next().ok_or("incomplete string size")?;
            }
            let size = <$header_t>::from_be_bytes(size_buffer);
            if (size as u128).saturating_mul(8) >= usize::MAX as u128 {
                panic!("server wants to send more string bytes than are addressable")
            }
            decode_string(stream, size as usize)
        }
    };
}

string_decoder!(decode_string_u8, u8, 1);
string_decoder!(decode_string_u16, u16, 2);
string_decoder!(decode_string_u32, u32, 4);

fn decode_string(
    stream: &mut impl Iterator<Item = u8>,
    size: usize,
) -> Result<PackStreamValue, PackStreamError> {
    let mut bytes = Vec::with_capacity(size);
    for _ in 0..size {
        bytes.push(stream.next().ok_or("less string bytes than announced")?);
    }
    let str = String::from_utf8_lossy(bytes.as_slice()).into_owned();
    Ok(str.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(vec![0xC0], PackStreamValue::Null)]
    fn test_null(#[case] input: Vec<u8>, #[case] output: PackStreamValue) {
        dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();
        assert_eq!(result, output);
        let input: Vec<_> = input.collect();
        assert_eq!(input, vec![]);
    }

    #[rstest]
    #[case(vec![0xC2], PackStreamValue::Boolean(false))]
    #[case(vec![0xC3], PackStreamValue::Boolean(true))]
    fn test_bool(#[case] input: Vec<u8>, #[case] output: PackStreamValue) {
        dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();
        assert_eq!(result, output);
        let input: Vec<_> = input.collect();
        assert_eq!(input, vec![]);
    }

    #[rstest]
    #[case(vec![0xF0], PackStreamValue::Integer(-16))]
    #[case(vec![0xFF], PackStreamValue::Integer(-1))]
    #[case(vec![0x00], PackStreamValue::Integer(0))]
    #[case(vec![0x01], PackStreamValue::Integer(1))]
    #[case(vec![0x7F], PackStreamValue::Integer(127))]
    #[case(vec![0xC8, 0x80], PackStreamValue::Integer(-128))]
    #[case(vec![0xC8, 0xD6], PackStreamValue::Integer(-42))]
    #[case(vec![0xC8, 0x00], PackStreamValue::Integer(0))]
    #[case(vec![0xC8, 0x2A], PackStreamValue::Integer(42))]
    #[case(vec![0xC8, 0x7F], PackStreamValue::Integer(127))]
    #[case(vec![0xC9, 0x80, 0x00], PackStreamValue::Integer(-32768))]
    #[case(vec![0xC9, 0xFF, 0x80], PackStreamValue::Integer(-128))]
    #[case(vec![0xC9, 0xFF, 0xD6], PackStreamValue::Integer(-42))]
    #[case(vec![0xC9, 0x00, 0x00], PackStreamValue::Integer(0))]
    #[case(vec![0xC9, 0x00, 0x2A], PackStreamValue::Integer(42))]
    #[case(vec![0xC9, 0x00, 0x7F], PackStreamValue::Integer(127))]
    #[case(vec![0xC9, 0x7F, 0xFF], PackStreamValue::Integer(32767))]
    #[case(vec![0xCA, 0x80, 0x00, 0x00, 0x00], PackStreamValue::Integer(-2147483648))]
    #[case(vec![0xCA, 0xFF, 0xFF, 0x80, 0x00], PackStreamValue::Integer(-32768))]
    #[case(vec![0xCA, 0xFF, 0xFF, 0xFF, 0x80], PackStreamValue::Integer(-128))]
    #[case(vec![0xCA, 0xFF, 0xFF, 0xFF, 0xD6], PackStreamValue::Integer(-42))]
    #[case(vec![0xCA, 0x00, 0x00, 0x00, 0x00], PackStreamValue::Integer(0))]
    #[case(vec![0xCA, 0x00, 0x00, 0x00, 0x2A], PackStreamValue::Integer(42))]
    #[case(vec![0xCA, 0x00, 0x00, 0x00, 0x7F], PackStreamValue::Integer(127))]
    #[case(vec![0xCA, 0x00, 0x00, 0x7F, 0xFF], PackStreamValue::Integer(32767))]
    #[case(vec![0xCA, 0x7F, 0xFF, 0xFF, 0xFF], PackStreamValue::Integer(2147483647))]
    #[case(vec![0xCB, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], PackStreamValue::Integer(-9223372036854775808))]
    #[case(vec![0xCB, 0xFF, 0xFF, 0xFF, 0xFF, 0x80, 0x00, 0x00, 0x00], PackStreamValue::Integer(-2147483648))]
    #[case(vec![0xCB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80, 0x00], PackStreamValue::Integer(-32768))]
    #[case(vec![0xCB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80], PackStreamValue::Integer(-128))]
    #[case(vec![0xCB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD6], PackStreamValue::Integer(-42))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], PackStreamValue::Integer(0))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A], PackStreamValue::Integer(42))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7F], PackStreamValue::Integer(127))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF], PackStreamValue::Integer(32767))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF, 0xFF, 0xFF], PackStreamValue::Integer(2147483647))]
    #[case(vec![0xCB, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], PackStreamValue::Integer(9223372036854775807))]
    fn test_integer(#[case] input: Vec<u8>, #[case] output: PackStreamValue) {
        dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();
        assert_eq!(result, output);
        let input: Vec<_> = input.collect();
        assert_eq!(input, vec![]);
    }

    #[rstest]
    #[case(vec![0xC1, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], 0.)]
    #[case(vec![0xC1, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], -0.)]
    #[case(vec![0xC1, 0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], f64::INFINITY)]
    #[case(vec![0xC1, 0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], f64::NEG_INFINITY)]
    #[case(vec![0xC1, 0x7F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], f64::NAN)]
    #[case(vec![0xC1, 0x3F, 0xF3, 0xAE, 0x14, 0x7A, 0xE1, 0x47, 0xAE], 1.23)]
    fn test_float(#[case] input: Vec<u8>, #[case] output: f64) {
        dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();
        if output.is_nan() {
            match result {
                PackStreamValue::Float(result) => assert!(result.is_nan()),
                _ => panic!("output was not float"),
            }
        } else {
            assert_eq!(result, PackStreamValue::Float(output));
        }
        let input: Vec<_> = input.collect();
        assert_eq!(input, vec![]);
    }

    fn damn_long_vec(header: Option<Vec<u8>>, size: usize) -> Vec<u8> {
        let mut res = match header {
            Some(mut header) => {
                header.reserve(size);
                header
            }
            None => Vec::with_capacity(size),
        };
        for i in 0..size {
            res.push(i as u8);
        }
        res
    }

    #[rstest]
    #[case(vec![0xCC, 0x00], vec![])]
    #[case(vec![0xCC, 0x01, 0x00], vec![0x00])]
    #[case(vec![0xCC, 0x01, 0x42], vec![0x42])]
    #[case(vec![0xCC, 0x01, 0xFF], vec![0xFF])]
    #[case(damn_long_vec(Some(vec![0xCC, 0xFF]), 0xFF), damn_long_vec(None, 0xFF))]
    #[case(vec![0xCD, 0x00, 0x00], vec![])]
    #[case(vec![0xCD, 0x00, 0x01, 0xFF], vec![0xFF])]
    #[case(damn_long_vec(Some(vec![0xCD, 0xFF, 0xFF]), 0xFFFF), damn_long_vec(None, 0xFFFF))]
    #[case(vec![0xCE, 0x00, 0x00, 0x00, 0x00], vec![])]
    #[case(vec![0xCE, 0x00, 0x00, 0x00, 0x01, 0xFF], vec![0xFF])]
    // We won't!!!111 That's almost 43 GB
    // #[case(damn_long_vec(Some(vec![0xCD, 0xFF, 0xFF, 0xFF, 0xFF]), 0xFFFFFFFF), damn_long_vec(None, 0xFFFFFFFF))]
    // 17 MB will have to suffice
    #[case(damn_long_vec(Some(vec![0xCE, 0x00, 0xFE, 0xFF, 0xFF]), 0x00FEFFFF), damn_long_vec(None, 0x00FEFFFF))]
    fn test_bytes(#[case] input: Vec<u8>, #[case] output: Vec<u8>) {
        if input.len() < 50 {
            dbg!(&input);
        }
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();

        assert_eq!(result, PackStreamValue::Bytes(output));
        let input: Vec<_> = input.collect();
        assert_eq!(input, vec![]);
    }

    #[rstest]
    #[case(vec![0x80], "")]
    #[case(vec![0x81, 0x41], "A")]
    #[case(vec![0x84, 0xF0, 0x9F, 0xA4, 0x98], "🤘")]
    #[case(vec![0xD0, 0x1A, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4A, 0x4B, 0x4C,
                0x4D, 0x4E, 0x4F, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A],
           "ABCDEFGHIJKLMNOPQRSTUVWXYZ")]
    #[case(vec![0xD0, 0x12, 0x47, 0x72, 0xC3, 0xB6, 0xC3, 0x9F, 0x65, 0x6E, 0x6D, 0x61, 0xC3, 0x9F,
                0x73, 0x74, 0xC3, 0xA4, 0x62, 0x65],
           "Größenmaßstäbe")]
    #[case(vec![0xD1, 0x00, 0x01, 0x41], "A")]
    #[case(vec![0xD2, 0x00, 0x00, 0x00, 0x01, 0x41], "A")]
    fn test_string(#[case] input: Vec<u8>, #[case] output: &str) {
        dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();

        assert_eq!(result, PackStreamValue::String(String::from(output)));
    }

    #[rstest]
    #[case(vec![], "no marker found")]
    // TODO: cover all error cases
    fn test_error(#[case] input: Vec<u8>, #[case] error: &'static str) {
        dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).expect_err("expected to fail");

        dbg!(error);
        dbg!(&result.reason);
        assert!(result.reason.to_lowercase().contains(error));
        dbg!(format!("{}", result.reason));
        assert!(format!("{}", result.reason).to_lowercase().contains(error));
        let input: Vec<_> = input.collect();
        assert_eq!(input, vec![]);
    }
}
