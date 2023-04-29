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
use std::error::Error;
use std::fmt::{Debug, Display};
use std::io;
use std::io::Write;

use usize_cast::FromUsize;

use super::super::BoltStructTranslator;
use super::error::PackStreamSerializeError;

pub trait PackStreamSerialize: Debug {
    fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        bolt: &B,
    ) -> Result<(), S::Error>;
}

pub trait PackStreamSerializer {
    type Error: Error;

    fn error(&self, message: String) -> Result<(), Self::Error>;

    fn write_null(&mut self) -> Result<(), Self::Error>;
    fn write_bool(&mut self, b: bool) -> Result<(), Self::Error>;
    fn write_int(&mut self, i: i64) -> Result<(), Self::Error>;
    fn write_float(&mut self, f: f64) -> Result<(), Self::Error>;
    fn write_bytes(&mut self, b: &[u8]) -> Result<(), Self::Error>;
    fn write_string(&mut self, s: &str) -> Result<(), Self::Error>;
    fn write_list_header(&mut self, size: u64) -> Result<(), Self::Error>;
    fn write_list<S: PackStreamSerialize, B: BoltStructTranslator>(
        &mut self,
        bolt: &B,
        l: &[S],
    ) -> Result<(), Self::Error>;
    fn write_dict_header(&mut self, size: u64) -> Result<(), Self::Error>;
    fn write_dict<S: PackStreamSerialize, B: BoltStructTranslator, K: AsRef<str>>(
        &mut self,
        bolt: &B,
        d: &HashMap<K, S>,
    ) -> Result<(), Self::Error>;
    fn write_struct_header(&mut self, tag: u8, size: u8) -> Result<(), Self::Error>;
    fn write_struct<S: PackStreamSerialize, B: BoltStructTranslator>(
        &mut self,
        bolt: &B,
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
    type Error = PackStreamSerializeError;

    fn error(&self, message: String) -> Result<(), Self::Error> {
        Err(message.into())
    }

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
        if (-16..=127).contains(&i) {
            self.writer.write_all(&i8::to_be_bytes(i as i8))?;
        } else if (-128..=127).contains(&i) {
            self.writer.write_all(&[0xC8])?;
            self.writer.write_all(&i8::to_be_bytes(i as i8))?;
        } else if (-32_768..=32_767).contains(&i) {
            self.writer.write_all(&[0xC9])?;
            self.writer.write_all(&i16::to_be_bytes(i as i16))?;
        } else if (-2_147_483_648..=2_147_483_647).contains(&i) {
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
        } else if size <= 2_147_483_647 {
            self.writer.write_all(&[0xCE])?;
            self.writer.write_all(&u32::to_be_bytes(size as u32))?;
        } else {
            return Err("bytes exceed max size of 2,147,483,647".into());
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
        } else if size <= 2_147_483_647 {
            self.writer.write_all(&[0xD2])?;
            self.writer.write_all(&u32::to_be_bytes(size as u32))?;
        } else {
            return Err("string exceeds max size of 2,147,483,647 bytes".into());
        }
        self.writer.write_all(bytes)?;
        Ok(())
    }

    fn write_list_header(&mut self, size: u64) -> Result<(), Self::Error> {
        if size <= 15 {
            self.writer.write_all(&[0x90 + size as u8])?;
        } else if size <= 255 {
            self.writer.write_all(&[0xD4])?;
            self.writer.write_all(&u8::to_be_bytes(size as u8))?;
        } else if size <= 65_535 {
            self.writer.write_all(&[0xD5])?;
            self.writer.write_all(&u16::to_be_bytes(size as u16))?;
        } else if size <= 2_147_483_647 {
            self.writer.write_all(&[0xD6])?;
            self.writer.write_all(&u32::to_be_bytes(size as u32))?;
        } else {
            return Err("list exceeds max size of 2,147,483,647".into());
        }
        Ok(())
    }

    fn write_list<S: PackStreamSerialize, B: BoltStructTranslator>(
        &mut self,
        bolt: &B,
        l: &[S],
    ) -> Result<(), Self::Error> {
        self.write_list_header(u64::from_usize(l.len()))?;
        for value in l {
            value.serialize(self, bolt)?;
        }
        Ok(())
    }
    fn write_dict_header(&mut self, size: u64) -> Result<(), Self::Error> {
        if size <= 15 {
            self.writer.write_all(&[0xA0 + size as u8])?;
        } else if size <= 255 {
            self.writer.write_all(&[0xD8])?;
            self.writer.write_all(&u8::to_be_bytes(size as u8))?;
        } else if size <= 65_535 {
            self.writer.write_all(&[0xD9])?;
            self.writer.write_all(&u16::to_be_bytes(size as u16))?;
        } else if size <= 2_147_483_647 {
            self.writer.write_all(&[0xDA])?;
            self.writer.write_all(&u32::to_be_bytes(size as u32))?;
        } else {
            return Err("list exceeds max size of 2,147,483,647".into());
        }
        Ok(())
    }

    fn write_dict<S: PackStreamSerialize, B: BoltStructTranslator, K: AsRef<str>>(
        &mut self,
        bolt: &B,
        d: &HashMap<K, S>,
    ) -> Result<(), Self::Error> {
        self.write_dict_header(u64::from_usize(d.len()))?;
        for (key, value) in d {
            self.write_string(key.as_ref())?;
            value.serialize(self, bolt)?;
        }
        Ok(())
    }
    fn write_struct_header(&mut self, tag: u8, size: u8) -> Result<(), Self::Error> {
        self.writer.write_all(&[0xB0 + size, tag])?;
        Ok(())
    }

    fn write_struct<S: PackStreamSerialize, B: BoltStructTranslator>(
        &mut self,
        bolt: &B,
        tag: u8,
        fields: &[S],
    ) -> Result<(), Self::Error> {
        self.write_struct_header(
            tag,
            fields
                .len()
                .try_into()
                .map_err(|_| "structure exceeds max number of fields (15)")?,
        )?;
        for field in fields.iter() {
            field.serialize(self, bolt)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct PackStreamSerializerDebugImpl {
    buff: String,
    stack: Vec<(&'static str, &'static str, u64)>,
}

impl PackStreamSerializerDebugImpl {
    pub fn new() -> Self {
        Self {
            buff: String::new(),
            stack: Vec::new(),
        }
    }

    #[inline]
    pub fn flush(&mut self) -> String {
        self.buff.split_off(0)
    }

    #[inline]
    fn format_display<D: Display>(&mut self, data: D) -> Result<(), io::Error> {
        self.buff += &format!("{}", data);
        self.handle_stack();
        Ok(())
    }

    #[inline]
    fn format_debug<D: Debug>(&mut self, data: D) -> Result<(), io::Error> {
        self.buff += &format!("{:?}", data);
        self.handle_stack();
        Ok(())
    }

    #[inline]
    fn handle_stack(&mut self) {
        while let Some((sep, terminal, size)) = self.stack.pop() {
            assert!(size > 0);
            let (new_sep, new_size) = match (sep, terminal, size) {
                (": ", "}", size) => (", ", size),
                (", ", "}", size) => (": ", size - 1),
                (sep, _, size) => (sep, size - 1),
            };
            if new_size > 0 {
                self.buff += sep;
                self.stack.push((new_sep, terminal, new_size));
                break;
            } else {
                self.buff += terminal;
            }
        }
    }
}

impl PackStreamSerializer for PackStreamSerializerDebugImpl {
    type Error = io::Error;

    fn error(&self, _: String) -> Result<(), Self::Error> {
        Ok(())
    }

    fn write_null(&mut self) -> Result<(), Self::Error> {
        self.format_display("null")
    }

    fn write_bool(&mut self, b: bool) -> Result<(), Self::Error> {
        self.format_debug(b)
    }

    fn write_int(&mut self, i: i64) -> Result<(), Self::Error> {
        self.format_display(i)
    }

    fn write_float(&mut self, f: f64) -> Result<(), Self::Error> {
        self.format_display(f)
    }

    fn write_bytes(&mut self, b: &[u8]) -> Result<(), Self::Error> {
        self.buff += &format!("bytes{:02X?}", b);
        self.handle_stack();
        Ok(())
    }

    fn write_string(&mut self, s: &str) -> Result<(), Self::Error> {
        self.format_debug(s)
    }

    fn write_list_header(&mut self, size: u64) -> Result<(), Self::Error> {
        if size > 0 {
            self.buff.push('[');
            self.stack.push((", ", "]", size));
        } else {
            self.buff += "[]";
            self.handle_stack();
        }
        Ok(())
    }

    fn write_list<S: PackStreamSerialize, B: BoltStructTranslator>(
        &mut self,
        bolt: &B,
        l: &[S],
    ) -> Result<(), Self::Error> {
        self.write_list_header(u64::from_usize(l.len())).unwrap();
        for value in l {
            value.serialize(self, bolt).unwrap();
        }
        Ok(())
    }

    fn write_dict_header(&mut self, size: u64) -> Result<(), Self::Error> {
        if size > 0 {
            self.buff.push('{');
            self.stack.push((": ", "}", size));
        } else {
            self.buff += "{}";
            self.handle_stack();
        }
        Ok(())
    }

    fn write_dict<S: PackStreamSerialize, B: BoltStructTranslator, K: AsRef<str>>(
        &mut self,
        bolt: &B,
        d: &HashMap<K, S>,
    ) -> Result<(), Self::Error> {
        self.write_dict_header(u64::from_usize(d.len())).unwrap();
        for (key, value) in d {
            self.write_string(key.as_ref()).unwrap();
            value.serialize(self, bolt).unwrap();
        }
        Ok(())
    }

    fn write_struct_header(&mut self, tag: u8, size: u8) -> Result<(), Self::Error> {
        if size > 0 {
            self.buff += &format!("Structure[{:#02X?}; {}](", tag, size);
            self.stack.push((", ", ")", size.into()));
        } else {
            self.buff += &format!("Structure[{:#02X?}; 0]()", tag);
            self.handle_stack();
        }
        Ok(())
    }

    fn write_struct<S: PackStreamSerialize, B: BoltStructTranslator>(
        &mut self,
        bolt: &B,
        tag: u8,
        fields: &[S],
    ) -> Result<(), Self::Error> {
        self.write_struct_header(tag, fields.len().try_into().unwrap_or(u8::MAX))
            .unwrap();
        for field in fields.iter() {
            field.serialize(self, bolt).unwrap();
        }
        Ok(())
    }
}

macro_rules! impl_packstream_serialize {
    ( $write_fn:ident, $($ty:ty),* ) => {
        $(
            impl PackStreamSerialize for $ty {
                fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
                    &self,
                    serializer: &mut S,
                    _: &B,
                ) -> Result<(), S::Error> {
                    serializer.$write_fn((*self).into())
                }
            }
        )*
    }
}

macro_rules! impl_packstream_serialize_as_ref {
    ( $write_fn:ident, $($ty:ty),* ) => {
        $(
            impl PackStreamSerialize for $ty {
                fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
                    &self,
                    serializer: &mut S,
                    _: &B,
                ) -> Result<(), S::Error> {
                    serializer.$write_fn(self.as_ref())
                }
            }
        )*
    }
}

impl_packstream_serialize!(write_bool, bool);
impl_packstream_serialize!(write_int, u8, u16, u32, i8, i16, i32, i64);
impl_packstream_serialize!(write_float, f32, f64);
impl_packstream_serialize!(write_string, &str);
impl_packstream_serialize_as_ref!(write_string, String);

impl<V: PackStreamSerialize> PackStreamSerialize for Option<V> {
    fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        bolt: &B,
    ) -> Result<(), S::Error> {
        match self {
            None => serializer.write_null(),
            Some(v) => v.serialize(serializer, bolt),
        }
    }
}

impl<V: PackStreamSerialize> PackStreamSerialize for [V] {
    fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        bolt: &B,
    ) -> Result<(), S::Error> {
        serializer.write_list(bolt, self)
    }
}

impl<V: PackStreamSerialize, K: AsRef<str> + Debug> PackStreamSerialize for HashMap<K, V> {
    fn serialize<S: PackStreamSerializer, B: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        bolt: &B,
    ) -> Result<(), S::Error> {
        serializer.write_dict(bolt, self)
    }
}
