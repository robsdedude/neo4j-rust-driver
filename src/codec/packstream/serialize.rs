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
use std::io::Write;

pub trait PackStreamSerialize {
    fn serialize<S: PackStreamSerializer>(&self, serializer: &mut S) -> Result<(), S::Error>;
}

pub trait PackStreamSerializer {
    type Error: Error;

    fn write_null(&mut self) -> Result<(), Self::Error>;
    fn write_bool(&mut self, b: bool) -> Result<(), Self::Error>;
    fn write_int(&mut self, i: i64) -> Result<(), Self::Error>;
    fn write_float(&mut self, f: f64) -> Result<(), Self::Error>;
    fn write_bytes(&mut self, b: &[u8]) -> Result<(), Self::Error>;
    fn write_string(&mut self, s: &str) -> Result<(), Self::Error>;
    fn write_list<S: PackStreamSerialize>(&mut self, l: &[S]) -> Result<(), Self::Error>;
    fn write_dict<S: PackStreamSerialize>(
        &mut self,
        d: &HashMap<String, S>,
    ) -> Result<(), Self::Error>;
    fn write_struct<S: PackStreamSerialize>(
        &mut self,
        tag: u8,
        fields: &[S],
    ) -> Result<(), Self::Error>;
}

pub struct PackStreamSerializerImpl<'a, W: Write> {
    writer: &'a mut W,
}

impl<'a, W: Write> PackStreamSerializerImpl<'a, W> {
    pub fn new(writer: &'a mut W) -> PackStreamSerializerImpl<'a, W> {
        PackStreamSerializerImpl { writer }
    }
}

impl<'a, W: Write> PackStreamSerializer for PackStreamSerializerImpl<'a, W> {
    type Error = PackStreamError;

    fn write_null(&mut self) -> Result<(), Self::Error> {
        self.writer.write_all(&[0xC0])?;
        Ok(())
    }

    fn write_bool(&mut self, b: bool) -> Result<(), Self::Error> {
        self.writer.write_all(match b {
            false => &[0xC2],
            true => &[0xC3],
        })?;
        Ok(())
    }

    fn write_int(&mut self, i: i64) -> Result<(), Self::Error> {
        if -16 <= i && i <= 127 {
            self.writer.write_all(&i8::to_be_bytes(i as i8))?;
        } else if -128 <= i && i <= 127 {
            self.writer.write_all(&[0xC8])?;
            self.writer.write_all(&i8::to_be_bytes(i as i8))?;
        } else if -32_768 <= i && i <= 32_767 {
            self.writer.write_all(&[0xC9])?;
            self.writer.write_all(&i16::to_be_bytes(i as i16))?;
        } else if -2_147_483_648 <= i && i <= 2_147_483_647 {
            self.writer.write_all(&[0xCA])?;
            self.writer.write_all(&i32::to_be_bytes(i as i32))?;
        } else {
            self.writer.write_all(&[0xCB])?;
            self.writer.write_all(&i64::to_be_bytes(i as i64))?;
        }
        Ok(())
    }

    fn write_float(&mut self, f: f64) -> Result<(), Self::Error> {
        self.writer.write_all(&[0xC1])?;
        self.writer.write_all(&f64::to_be_bytes(f))?;
        Ok(())
    }

    fn write_bytes(&mut self, b: &[u8]) -> Result<(), Self::Error> {
        let size = b.len();
        if size <= 255 {
            self.writer.write_all(&[0xCC])?;
            self.writer.write_all(&u8::to_be_bytes(size as u8))?;
        } else if size <= 65_535 {
            self.writer.write_all(&[0xCD])?;
            self.writer.write_all(&u16::to_be_bytes(size as u16))?;
        } else if size <= 4_294_967_295 {
            self.writer.write_all(&[0xCE])?;
            self.writer.write_all(&u32::to_be_bytes(size as u32))?;
        } else {
            return Err("bytes exceed max size of 4,294,967,295".into());
        }
        self.writer.write_all(b)?;
        Ok(())
    }

    fn write_string(&mut self, s: &str) -> Result<(), Self::Error> {
        let bytes = s.as_bytes();
        let size = bytes.len();
        if size <= 15 {
            self.writer.write_all(&[0x80 + size as u8])?;
        } else if size <= 255 {
            self.writer.write_all(&[0xD0])?;
            self.writer.write_all(&u8::to_be_bytes(size as u8))?;
        } else if size <= 65_535 {
            self.writer.write_all(&[0xD1])?;
            self.writer.write_all(&u16::to_be_bytes(size as u16))?;
        } else if size <= 4_294_967_295 {
            self.writer.write_all(&[0xD2])?;
            self.writer.write_all(&u32::to_be_bytes(size as u32))?;
        } else {
            return Err("string exceeds max size of 4,294,967,295 bytes".into());
        }
        self.writer.write_all(bytes)?;
        Ok(())
    }

    fn write_list<S: PackStreamSerialize>(&mut self, l: &[S]) -> Result<(), Self::Error> {
        let size = l.len();
        if size <= 15 {
            self.writer.write_all(&[0x90 + size as u8])?;
        } else if size <= 255 {
            self.writer.write_all(&[0xD4])?;
            self.writer.write_all(&u8::to_be_bytes(size as u8))?;
        } else if size <= 65_535 {
            self.writer.write_all(&[0xD5])?;
            self.writer.write_all(&u16::to_be_bytes(size as u16))?;
        } else if size <= 4_294_967_295 {
            self.writer.write_all(&[0xD6])?;
            self.writer.write_all(&u32::to_be_bytes(size as u32))?;
        } else {
            return Err("list exceeds max size of 4,294,967,295".into());
        }
        for value in l {
            value.serialize(self)?;
        }
        Ok(())
    }

    fn write_dict<S: PackStreamSerialize>(
        &mut self,
        d: &HashMap<String, S>,
    ) -> Result<(), Self::Error> {
        let size = d.len();
        if size <= 15 {
            self.writer.write_all(&[0xA0 + size as u8])?;
        } else if size <= 255 {
            self.writer.write_all(&[0xD8])?;
            self.writer.write_all(&u8::to_be_bytes(size as u8))?;
        } else if size <= 65_535 {
            self.writer.write_all(&[0xD9])?;
            self.writer.write_all(&u16::to_be_bytes(size as u16))?;
        } else if size <= 4_294_967_295 {
            self.writer.write_all(&[0xDA])?;
            self.writer.write_all(&u32::to_be_bytes(size as u32))?;
        } else {
            return Err("list exceeds max size of 4,294,967,295".into());
        }
        for (key, value) in d {
            self.write_string(key)?;
            value.serialize(self)?;
        }
        Ok(())
    }

    fn write_struct<S: PackStreamSerialize>(
        &mut self,
        tag: u8,
        fields: &[S],
    ) -> Result<(), Self::Error> {
        if fields.len() > 15 {
            return Err("structure exceeds max number of fields (15)".into());
        }
        self.writer.write_all(&[0xB0 + fields.len() as u8, tag])?;
        for field in fields.iter() {
            field.serialize(self)?;
        }
        Ok(())
    }
}
