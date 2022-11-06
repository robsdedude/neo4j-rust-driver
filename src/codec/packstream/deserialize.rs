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

use super::error::PackStreamError;
use std::collections::HashMap;
use std::error::Error;
use std::io::Read;

pub trait PackStreamDeserialize {
    type Value: Sized;

    fn load_null() -> Self::Value;
    fn load_bool(b: bool) -> Self::Value;
    fn load_int(i: i64) -> Self::Value;
    fn load_float(f: f64) -> Self::Value;
    fn load_bytes(b: Vec<u8>) -> Self::Value;
    fn load_string(s: String) -> Self::Value;
    fn load_list(l: Vec<Self::Value>) -> Self::Value;
    fn load_dict(d: HashMap<String, Self::Value>) -> Self::Value;
    fn load_struct(tag: u8, fields: Vec<Self::Value>) -> Self::Value;
}

pub trait PackStreamDeserializer {
    type Error: Error;

    fn load<S: PackStreamDeserialize>(&mut self) -> Result<S::Value, Self::Error>;
    fn load_string(&mut self) -> Result<String, Self::Error>;
}

pub struct PackStreamDeserializerImpl<'a, W: Read> {
    reader: &'a mut W,
}

impl<'a, W: Read + 'a> PackStreamDeserializerImpl<'a, W> {
    pub fn new(reader: &'a mut W) -> PackStreamDeserializerImpl<'a, W> {
        PackStreamDeserializerImpl { reader }
    }

    fn decode_i8(reader: &mut impl Read) -> Result<i8, PackStreamError> {
        let mut buffer = [0; 1];
        reader.read_exact(&mut buffer)?;
        let value = i8::from_be_bytes(buffer);
        Ok(value)
    }

    fn decode_i16(reader: &mut impl Read) -> Result<i16, PackStreamError> {
        let mut buffer = [0; 2];
        reader.read_exact(&mut buffer)?;
        let value = i16::from_be_bytes(buffer);
        Ok(value)
    }

    fn decode_i32(reader: &mut impl Read) -> Result<i32, PackStreamError> {
        let mut buffer = [0; 4];
        reader.read_exact(&mut buffer)?;
        let value = i32::from_be_bytes(buffer);
        Ok(value)
    }

    fn decode_i64(reader: &mut impl Read) -> Result<i64, PackStreamError> {
        let mut buffer = [0; 8];
        reader.read_exact(&mut buffer)?;
        let value = i64::from_be_bytes(buffer);
        Ok(value)
    }

    fn decode_f64(reader: &mut impl Read) -> Result<f64, PackStreamError> {
        let mut buffer = [0; 8];
        reader.read_exact(&mut buffer)?;
        let value = f64::from_be_bytes(buffer);
        Ok(value)
    }

    fn decode_u8(reader: &mut impl Read) -> Result<u8, PackStreamError> {
        let mut buffer = [0; 1];
        reader.read_exact(&mut buffer)?;
        let value = u8::from_be_bytes(buffer);
        Ok(value)
    }

    fn decode_u16(reader: &mut impl Read) -> Result<u16, PackStreamError> {
        let mut buffer = [0; 2];
        reader.read_exact(&mut buffer)?;
        let value = u16::from_be_bytes(buffer);
        Ok(value)
    }

    fn decode_u32(reader: &mut impl Read) -> Result<u32, PackStreamError> {
        let mut buffer = [0; 4];
        reader.read_exact(&mut buffer)?;
        let value = u32::from_be_bytes(buffer);
        Ok(value)
    }

    fn decode_bytes(reader: &mut impl Read, size: usize) -> Result<Vec<u8>, PackStreamError> {
        let mut bytes = Vec::new();
        bytes.resize(size, 0);
        reader.read_exact(bytes.as_mut_slice())?;
        Ok(bytes)
    }

    fn decode_string(reader: &mut impl Read, size: usize) -> Result<String, PackStreamError> {
        let bytes = Self::decode_bytes(reader, size)?;
        Ok(String::from_utf8_lossy(bytes.as_slice()).into_owned())
    }

    fn decode_list<S: PackStreamDeserialize, D: PackStreamDeserializer>(
        serializer: &mut D,
        size: usize,
    ) -> Result<Vec<S::Value>, D::Error> {
        let mut list = Vec::with_capacity(size);
        for _ in 0..size {
            list.push(serializer.load::<S>()?);
        }
        Ok(list)
    }

    fn decode_dict<S: PackStreamDeserialize, D: PackStreamDeserializer>(
        serializer: &mut D,
        size: usize,
    ) -> Result<HashMap<String, S::Value>, D::Error> {
        let mut dict = HashMap::with_capacity(size);
        for _ in 0..size {
            let key = serializer.load_string()?;
            let value = serializer.load::<S>()?;
            dict.insert(key, value);
        }
        Ok(dict)
    }
}

impl<'a, W: Read> PackStreamDeserializer for PackStreamDeserializerImpl<'a, W> {
    type Error = PackStreamError;

    fn load<S: PackStreamDeserialize>(&mut self) -> Result<S::Value, Self::Error> {
        let mut marker = [0; 1];
        self.reader.read_exact(&mut marker)?;
        let marker = marker[0];
        if marker == 0xC0 {
            Ok(S::load_null())
        } else if marker == 0xC2 {
            Ok(S::load_bool(false))
        } else if marker == 0xC3 {
            Ok(S::load_bool(true))
        } else if 0xF0 <= marker || marker <= 0x7F {
            Ok(S::load_int(i8::from_be_bytes([marker]).into()))
        } else if marker == 0xC8 {
            Ok(S::load_int(Self::decode_i8(self.reader)?.into()))
        } else if marker == 0xC9 {
            Ok(S::load_int(Self::decode_i16(self.reader)?.into()))
        } else if marker == 0xCA {
            Ok(S::load_int(Self::decode_i32(self.reader)?.into()))
        } else if marker == 0xCB {
            Ok(S::load_int(Self::decode_i64(self.reader)?))
        } else if marker == 0xC1 {
            Ok(S::load_float(Self::decode_f64(self.reader)?))
        } else if marker == 0xCC {
            let size = Self::decode_u8(self.reader)?;
            Ok(S::load_bytes(Self::decode_bytes(self.reader, size.into())?))
        } else if marker == 0xCD {
            let size = Self::decode_u16(self.reader)?;
            Ok(S::load_bytes(Self::decode_bytes(self.reader, size.into())?))
        } else if marker == 0xCE {
            if usize::BITS < 32 {
                Err("server wants to send more dict elements than are addressable".into())
            } else {
                let size = Self::decode_u32(self.reader)?;
                let bytes = Self::decode_bytes(self.reader, size as usize)?;
                Ok(S::load_bytes(bytes))
            }
        } else if 0x80 <= marker && marker <= 0x8F {
            let size = marker - 0x80;
            let string = Self::decode_string(self.reader, size.into())?;
            Ok(S::load_string(string))
        } else if marker == 0xD0 {
            let size = Self::decode_u8(self.reader)?;
            let string = Self::decode_string(self.reader, size.into())?;
            Ok(S::load_string(string))
        } else if marker == 0xD1 {
            let size = Self::decode_u16(self.reader)?;
            let string = Self::decode_string(self.reader, size.into())?;
            Ok(S::load_string(string))
        } else if marker == 0xD2 {
            if usize::BITS < 32 {
                return Err("server wants to send more string bytes than are addressable".into());
            }
            let size = Self::decode_u32(self.reader)?;
            let string = Self::decode_string(self.reader, size as usize)?;
            Ok(S::load_string(string))
        } else if 0x90 <= marker && marker <= 0x9F {
            let size = marker - 0x90;
            let list = Self::decode_list::<S, Self>(self, size.into())?;
            Ok(S::load_list(list))
        } else if marker == 0xD4 {
            let size = Self::decode_u8(self.reader)?;
            let list = Self::decode_list::<S, Self>(self, size.into())?;
            Ok(S::load_list(list))
        } else if marker == 0xD5 {
            let size = Self::decode_u16(self.reader)?;
            let list = Self::decode_list::<S, Self>(self, size.into())?;
            Ok(S::load_list(list))
        } else if marker == 0xD6 {
            if usize::BITS < 32 {
                return Err("server wants to send more string bytes than are addressable".into());
            }
            let size = Self::decode_u32(self.reader)?;
            let list = Self::decode_list::<S, Self>(self, size as usize)?;
            Ok(S::load_list(list))
        } else if 0xA0 <= marker && marker <= 0xAF {
            let size = marker - 0xA0;
            let dict = Self::decode_dict::<S, Self>(self, size.into())?;
            Ok(S::load_dict(dict))
        } else if marker == 0xD8 {
            let size = Self::decode_u8(self.reader)?;
            let dict = Self::decode_dict::<S, Self>(self, size.into())?;
            Ok(S::load_dict(dict))
        } else if marker == 0xD9 {
            let size = Self::decode_u16(self.reader)?;
            let dict = Self::decode_dict::<S, Self>(self, size.into())?;
            Ok(S::load_dict(dict))
        } else if marker == 0xDA {
            if usize::BITS < 32 {
                return Err("server wants to send more dict elements than are addressable".into());
            }
            let size = Self::decode_u32(self.reader)?;
            let dict = Self::decode_dict::<S, Self>(self, size as usize)?;
            Ok(S::load_dict(dict))
        } else if 0xB0 <= marker && marker <= 0xBF {
            let size = marker - 0xB0;
            let tag = Self::decode_u8(self.reader)?;
            let fields = Self::decode_list::<S, Self>(self, size.into())?;
            Ok(S::load_struct(tag, fields))
        } else {
            Err(PackStreamError::from(format!("unknown marker {marker}")))
        }
    }

    fn load_string(&mut self) -> Result<String, Self::Error> {
        let mut marker = [0; 1];
        self.reader.read_exact(&mut marker)?;
        let marker = marker[0];
        if 0x80 <= marker && marker <= 0x8F {
            let size = marker - 0x80;
            Self::decode_string(self.reader, size.into())
        } else if marker == 0xD0 {
            let size = Self::decode_u8(self.reader)?;
            Self::decode_string(self.reader, size.into())
        } else if marker == 0xD1 {
            let size = Self::decode_u16(self.reader)?;
            Self::decode_string(self.reader, size.into())
        } else {
            Err("Expected string".into())
        }
    }
}
