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

use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::u8;

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
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<Value>),
    Dictionary(HashMap<String, Value>),
    Structure(Structure),
}

#[derive(Debug, PartialEq)]
pub struct Structure {
    pub size: u8,
    pub tag: u8,
    pub fields: Vec<Value>,
}

pub fn decode(stream: &mut impl Iterator<Item = u8>) -> Result<Value, PackStreamError> {
    // TODO: proper errors?
    let marker = stream.next().ok_or("no marker found")?;
    if marker == 0xC0 {
        Ok(Value::Null)
    } else if marker == 0xC2 {
        Ok(Value::Boolean(false))
    } else if marker == 0xC3 {
        Ok(Value::Boolean(true))
    } else if 0xF0 <= marker || marker <= 0x7F {
        Ok(Value::Integer(i8::from_be_bytes([marker]).into()))
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
    } else {
        Err(PackStreamError::from(format!("unknown marker {}", marker)))
    }
}

macro_rules! primitive_decoder {
    ( $name:ident, $primitive_t:ty, $size:expr, $value_t:expr ) => {
        fn $name(stream: &mut impl Iterator<Item = u8>) -> Result<Value, PackStreamError> {
            let mut buffer = [0; $size];
            for i in 0..$size {
                buffer[i] = stream.next().ok_or(stringify!(not enough data after $primitive_t marker))?
            }
            let int = <$primitive_t>::from_be_bytes(buffer);
            Ok($value_t(int.into()))
        }
    };
}

primitive_decoder!(decode_i8, i8, 1, Value::Integer);
primitive_decoder!(decode_i16, i16, 2, Value::Integer);
primitive_decoder!(decode_i32, i32, 4, Value::Integer);
primitive_decoder!(decode_i64, i64, 8, Value::Integer);
primitive_decoder!(decode_f64, f64, 8, Value::Float);

macro_rules! bytes_decoder {
    ( $name:ident, $header_t:ty, $size:expr ) => {
        fn $name(stream: &mut impl Iterator<Item = u8>) -> Result<Value, PackStreamError> {
            let mut size_buffer = [0; $size];
            for i in 0..$size {
                size_buffer[i] = stream.next().ok_or("incomplete bytes size")?;
            }
            let size = <$header_t>::from_be_bytes(size_buffer);
            if (size as u128).saturating_mul(8) >= usize::MAX as u128 {
                panic!("server wants to send more bytes than are addressable")
            }
            let mut bytes = Vec::with_capacity(size as usize);
            for _ in 0..size {
                bytes.push(stream.next().ok_or("less bytes than announced")?);
            }
            Ok(Value::Bytes(bytes))
        }
    };
}

bytes_decoder!(decode_bytes_u8, u8, 1);
bytes_decoder!(decode_bytes_u16, u16, 2);
bytes_decoder!(decode_bytes_u32, u32, 4);

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(vec![0xC0], Value::Null)]
    fn test_null(#[case] input: Vec<u8>, #[case] output: Value) {
        dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();
        assert_eq!(result, output);
        assert_eq!(input.collect::<Vec<_>>(), vec![]);
    }

    #[rstest]
    #[case(vec![0xC2], Value::Boolean(false))]
    #[case(vec![0xC3], Value::Boolean(true))]
    fn test_bool(#[case] input: Vec<u8>, #[case] output: Value) {
        dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();
        assert_eq!(result, output);
        assert_eq!(input.collect::<Vec<_>>(), vec![]);
    }

    #[rstest]
    #[case(vec![0xF0], Value::Integer(-16))]
    #[case(vec![0xFF], Value::Integer(-1))]
    #[case(vec![0x00], Value::Integer(0))]
    #[case(vec![0x01], Value::Integer(1))]
    #[case(vec![0x7F], Value::Integer(127))]
    #[case(vec![0xC8, 0x80], Value::Integer(-128))]
    #[case(vec![0xC8, 0xD6], Value::Integer(-42))]
    #[case(vec![0xC8, 0x00], Value::Integer(0))]
    #[case(vec![0xC8, 0x2A], Value::Integer(42))]
    #[case(vec![0xC8, 0x7F], Value::Integer(127))]
    #[case(vec![0xC9, 0x80, 0x00], Value::Integer(-32768))]
    #[case(vec![0xC9, 0xFF, 0x80], Value::Integer(-128))]
    #[case(vec![0xC9, 0xFF, 0xD6], Value::Integer(-42))]
    #[case(vec![0xC9, 0x00, 0x00], Value::Integer(0))]
    #[case(vec![0xC9, 0x00, 0x2A], Value::Integer(42))]
    #[case(vec![0xC9, 0x00, 0x7F], Value::Integer(127))]
    #[case(vec![0xC9, 0x7F, 0xFF], Value::Integer(32767))]
    #[case(vec![0xCA, 0x80, 0x00, 0x00, 0x00], Value::Integer(-2147483648))]
    #[case(vec![0xCA, 0xFF, 0xFF, 0x80, 0x00], Value::Integer(-32768))]
    #[case(vec![0xCA, 0xFF, 0xFF, 0xFF, 0x80], Value::Integer(-128))]
    #[case(vec![0xCA, 0xFF, 0xFF, 0xFF, 0xD6], Value::Integer(-42))]
    #[case(vec![0xCA, 0x00, 0x00, 0x00, 0x00], Value::Integer(0))]
    #[case(vec![0xCA, 0x00, 0x00, 0x00, 0x2A], Value::Integer(42))]
    #[case(vec![0xCA, 0x00, 0x00, 0x00, 0x7F], Value::Integer(127))]
    #[case(vec![0xCA, 0x00, 0x00, 0x7F, 0xFF], Value::Integer(32767))]
    #[case(vec![0xCA, 0x7F, 0xFF, 0xFF, 0xFF], Value::Integer(2147483647))]
    #[case(vec![0xCB, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], Value::Integer(-9223372036854775808))]
    #[case(vec![0xCB, 0xFF, 0xFF, 0xFF, 0xFF, 0x80, 0x00, 0x00, 0x00], Value::Integer(-2147483648))]
    #[case(vec![0xCB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80, 0x00], Value::Integer(-32768))]
    #[case(vec![0xCB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80], Value::Integer(-128))]
    #[case(vec![0xCB, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xD6], Value::Integer(-42))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00], Value::Integer(0))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A], Value::Integer(42))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7F], Value::Integer(127))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF], Value::Integer(32767))]
    #[case(vec![0xCB, 0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF, 0xFF, 0xFF], Value::Integer(2147483647))]
    #[case(vec![0xCB, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], Value::Integer(9223372036854775807))]
    fn test_integer(#[case] input: Vec<u8>, #[case] output: Value) {
        dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();
        assert_eq!(result, output);
        assert_eq!(input.collect::<Vec<_>>(), vec![]);
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
                Value::Float(result) => assert!(result.is_nan()),
                _ => panic!("output was not float"),
            }
        } else {
            assert_eq!(result, Value::Float(output));
        }
        assert_eq!(input.collect::<Vec<_>>(), vec![]);
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
        // dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).unwrap();
        assert_eq!(result, Value::Bytes(output));
        assert_eq!(input.collect::<Vec<_>>(), vec![]);
    }

    #[rstest]
    #[case(vec![], "no marker found")]
    // TODO: cover all error cases
    fn test_error(#[case] input: Vec<u8>, #[case] error: &'static str) {
        // dbg!(&input);
        let mut input = input.into_iter();
        let result = decode(&mut input).expect_err("expected to fail");

        // dbg!(error);
        // dbg!(result.reason);
        assert!(result.reason.to_lowercase().contains(error));
        // dbg!(format!("{}", result.reason));
        assert!(format!("{}", result.reason).to_lowercase().contains(error));
        assert_eq!(input.collect::<Vec<_>>(), vec![]);
    }
}
