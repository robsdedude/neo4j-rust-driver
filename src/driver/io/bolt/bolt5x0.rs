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
use std::fmt::{Debug, Formatter};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::ops::Deref;

use usize_cast::FromUsize;

use super::packstream::{PackStreamSerialize, PackStreamSerializer, PackStreamSerializerImpl};
use super::{
    Bolt, BoltResponse, BoltStructTranslator, Chunker, ResponseCallbacks, ResponseMessage,
};
use crate::{PackStreamDeserialize, Result, Value};

// pub struct Bolt5x0<R: Read, W: Write> {
//     message_buff: VecDeque<Vec<u8>>,
//     responses: VecDeque<BoltResponse>,
//     reader: R,
//     writer: W,
//     socket: Option<TcpStream>,
// }
//
// impl<R: Read, W: Write> Debug for Bolt5x0<R, W> {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         write!(
//             f,
//             "Bolt5x0 {{\n  message_buff: {:?}\n  responses: {:?}\n}}",
//             self.message_buff, self.responses
//         )
//     }
// }

// pub struct Bolt5x0;
//
// impl Bolt5x0 {
//     pub(crate) fn hello<R: Read, W: Write>(
//         bolt: &mut Bolt<R, W>,
//         user_agent: &str,
//         auth: &HashMap<String, Value>,
//         routing_context: Option<&HashMap<String, Value>>,
//     ) -> Result<()> {
//         let mut message_buff = Vec::new();
//         let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
//         let translator = Bolt5x0StructTranslator {};
//         serializer.write_struct_header(0x01, 1)?;
//         let extra_size = 1 + routing_context.map(HashMap::len).unwrap_or(0) + auth.len();
//         serializer.write_dict_header(u64::from_usize(extra_size))?;
//
//         serializer.write_string("user_agent")?;
//         serializer.write_string(user_agent)?;
//
//         if let Some(routing_context) = routing_context {
//             for (k, v) in routing_context {
//                 serializer.write_string(k)?;
//                 v.serialize(&mut serializer, &translator)?;
//             }
//         }
//
//         for (k, v) in auth {
//             serializer.write_string(k)?;
//             v.serialize(&mut serializer, &translator)?;
//         }
//
//         // let mut extra: HashMap<&str, &Value> = HashMap::with_capacity(2 + auth.len());
//         // extra.insert("user_agent", user_agent.into());
//         // for (k, v) in auth {
//         //     extra.insert(k.as_str(), v.into());
//         // }
//         // extra.insert("user_agent", user_agent.into());
//         // serializer.write_struct(&Bolt5x0StructTranslator {}, 0x01, &[extra])?;
//         bolt.message_buff.push_back(message_buff);
//
//         bolt.responses.append(BoltResponse::new(ResponseMessage::Hello, ))
//         Ok(())
//     }
//
//     pub(crate) fn goodbye<R: Read, W: Write>(bolt: &mut Bolt<R, W>) -> Result<()> {
//         let mut message_buff = Vec::new();
//         let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
//         serializer.write_struct_header(0x02, 0)?;
//         bolt.message_buff.push_back(message_buff);
//         Ok(())
//     }
//
//     pub(crate) fn reset<R: Read, W: Write>(bolt: &mut Bolt<R, W>) -> Result<()> {
//         let mut message_buff = Vec::new();
//         let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
//         serializer.write_struct_header(0x0F, 0)?;
//         bolt.message_buff.push_back(message_buff);
//         Ok(())
//     }
//
//     pub(crate) fn run<
//         R: Read,
//         W: Write,
//         K1: Deref<Target = str>,
//         S1: PackStreamSerialize,
//         K2: Deref<Target = str>,
//         S2: PackStreamSerialize,
//     >(
//         bolt: &mut Bolt<R, W>,
//         query: &str,
//         parameters: Option<&HashMap<K1, S1>>,
//         bookmarks: Option<&[String]>,
//         tx_timeout: Option<i64>,
//         tx_meta: Option<&HashMap<K2, S2>>,
//         mode: Option<&str>,
//         db: Option<&str>,
//         imp_user: Option<&str>,
//     ) -> Result<()> {
//         let mut message_buff = Vec::new();
//         let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
//         let translator = Bolt5x0StructTranslator {};
//         serializer.write_struct_header(0x10, 3)?;
//
//         serializer.write_string(query)?;
//
//         match parameters {
//             Some(parameters) => serializer.write_dict(&translator, parameters)?,
//             None => serializer.write_dict_header(0)?,
//         }
//
//         serializer.write_dict_header(
//             (bookmarks.map_or(false, |b| b.len() > 0) as u64)
//                 + (tx_timeout.is_some() as u64)
//                 + (tx_meta.map_or(false, |m| m.len() > 0) as u64)
//                 + (mode.is_some() as u64)
//                 + (db.is_some() as u64)
//                 + (imp_user.is_some() as u64),
//         )?;
//         if let Some(bookmarks) = bookmarks {
//             if bookmarks.len() > 0 {
//                 serializer.write_string("bookmarks")?;
//                 bookmarks.serialize(&mut serializer, &translator)?;
//             }
//         }
//         if let Some(tx_timeout) = tx_timeout {
//             serializer.write_string("tx_timeout")?;
//             tx_timeout.serialize(&mut serializer, &translator)?;
//         }
//         if let Some(tx_meta) = tx_meta {
//             if tx_meta.len() > 0 {
//                 serializer.write_string("tx_meta")?;
//                 tx_meta.serialize(&mut serializer, &translator)?;
//             }
//         }
//         if let Some(mode) = mode {
//             serializer.write_string("mode")?;
//             mode.serialize(&mut serializer, &translator)?;
//         }
//         if let Some(db) = db {
//             serializer.write_string("db")?;
//             db.serialize(&mut serializer, &translator)?;
//         }
//         if let Some(imp_user) = imp_user {
//             serializer.write_string("imp_user")?;
//             imp_user.serialize(&mut serializer, &translator)?;
//         }
//         bolt.message_buff.push_back(message_buff);
//         Ok(())
//     }
//
//     pub(crate) fn discard<R: Read, W: Write>(
//         bolt: &mut Bolt<R, W>,
//         n: i64,
//         qid: i64,
//     ) -> Result<()> {
//         let mut message_buff = Vec::new();
//         let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
//         serializer.write_struct_header(0x2F, 1)?;
//
//         serializer.write_dict_header(2)?;
//         serializer.write_string("n")?;
//         serializer.write_int(n)?;
//         serializer.write_string("qid")?;
//         serializer.write_int(qid)?;
//
//         bolt.message_buff.push_back(message_buff);
//         Ok(())
//     }
//
//     pub(crate) fn pull<R: Read, W: Write>(bolt: &mut Bolt<R, W>, n: i64, qid: i64) -> Result<()> {
//         let mut message_buff = Vec::new();
//         let mut serializer = PackStreamSerializerImpl::new(&mut message_buff);
//         serializer.write_struct_header(0x3F, 1)?;
//
//         serializer.write_dict_header(2)?;
//         serializer.write_string("n")?;
//         serializer.write_int(n)?;
//         serializer.write_string("qid")?;
//         serializer.write_int(qid)?;
//
//         bolt.message_buff.push_back(message_buff);
//         Ok(())
//     }
// }

pub(crate) struct Bolt5x0StructTranslator {}

impl BoltStructTranslator for Bolt5x0StructTranslator {
    fn serialize_point_2d<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        srid: i64,
        x: f64,
        y: f64,
    ) -> std::result::Result<(), S::Error> {
        serializer.write_struct(
            self,
            b'X',
            &[Value::Integer(srid), Value::Float(x), Value::Float(y)],
        )
    }

    fn serialize_point_3d<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        srid: i64,
        x: f64,
        y: f64,
        z: f64,
    ) -> std::result::Result<(), S::Error> {
        serializer.write_struct(
            self,
            b'Y',
            &[
                Value::Integer(srid),
                Value::Float(x),
                Value::Float(y),
                Value::Float(z),
            ],
        )
    }

    fn deserialize_struct<V: PackStreamDeserialize>(
        &self,
        tag: u8,
        fields: Vec<V::Value>,
    ) -> V::Value {
        match tag {
            b'X' => V::load_point_2d(fields),
            b'Y' => V::load_point_3d(fields),
            _ => V::load_broken(format!("Unknown struct tag {:#02X}", tag)),
        }
    }
}
