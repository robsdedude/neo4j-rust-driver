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

mod bolt4x4;
mod bolt5x0;
mod bolt_state;
mod chunk;
mod message;
pub(crate) mod message_parameters;
mod packstream;
mod response;
mod socket;

use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::ops::Deref;
use std::result;
use std::sync::Arc;
use std::time::{Duration, Instant};

use atomic_refcell::AtomicRefCell;
use enum_dispatch::enum_dispatch;
use log::Level::Trace;
use log::{debug, log_enabled, trace};
use rustls::ClientConfig;
use usize_cast::FromUsize;

use super::deadline::DeadlineIO;
use crate::{Address, Neo4jError, Result, ValueReceive, ValueSend};
use bolt4x4::{Bolt4x4, Bolt4x4StructTranslator};
use bolt5x0::{Bolt5x0, Bolt5x0StructTranslator};
use bolt_state::{BoltState, BoltStateTracker};
use chunk::{Chunker, Dechunker};
use message::BoltMessage;
use message_parameters::RunParameters;
use packstream::PackStreamSerializer;
pub(crate) use response::{
    BoltMeta, BoltRecordFields, BoltResponse, ResponseCallbacks, ResponseMessage,
};
pub(crate) use socket::{BufTcpStream, Socket};

macro_rules! debug_buf_start {
    ($name:ident) => {
        let mut $name: Option<String> = match log_enabled!(Level::Debug) {
            true => Some(String::new()),
            false => None,
        };
    };
}
pub(crate) use debug_buf_start;

macro_rules! debug_buf {
    ($name:ident, $($args:tt)+) => {
        if log_enabled!(Level::Debug) {
            $name.as_mut().unwrap().push_str(&format!($($args)*))
        };
    }
}
pub(crate) use debug_buf;

macro_rules! bolt_debug_extra {
    ($meta:expr, $local_port:expr) => {
        'a: {
            let meta = $meta;
            // ugly format because rust-fmt is broken
            let Ok(meta) = meta else {
                break 'a dbg_extra($local_port, Some("!!!!"));
            };
            let Some(ValueReceive::String(id)) = meta.get("connection_id") else {
                break 'a dbg_extra($local_port, None);
            };
            dbg_extra($local_port, Some(id))
        }
    };
}
pub(crate) use bolt_debug_extra;

macro_rules! debug_buf_end {
    ($bolt:expr, $name:ident) => {
        debug!(
            "{}{}",
            bolt_debug_extra!($bolt.meta.try_borrow(), $bolt.local_port),
            $name.as_ref().map(|s| s.as_str()).unwrap_or("")
        );
    };
}
pub(crate) use debug_buf_end;

macro_rules! bolt_debug {
    ($bolt:expr, $($args:tt)+) => {
        debug!(
            "{}{}",
            bolt_debug_extra!($bolt.meta.try_borrow(), $bolt.local_port),
            format!($($args)*)
        );
    };
}
pub(crate) use bolt_debug;

macro_rules! socket_debug {
    ($local_port:expr, $($args:tt)+) => {
        debug!(
            "{}{}",
            dbg_extra(Some($local_port), None),
            format!($($args)*)
        );
    };
}
use crate::error::ServerError;
pub(crate) use socket_debug;

fn dbg_extra(port: Option<u16>, bolt_id: Option<&str>) -> String {
    format!(
        "[#{:04X} {:<10}] ",
        port.unwrap_or(0),
        bolt_id.unwrap_or("")
    )
}

pub(crate) type TcpBolt = Bolt<Socket<BufTcpStream>>;

pub(crate) type OnServerErrorCb<'a, 'b> = Option<&'a mut (dyn FnMut(&ServerError) + 'b)>;

#[derive(Debug)]
pub(crate) struct Bolt<RW: Read + Write> {
    data: BoltData<RW>,
    protocol: BoltProtocolVersion,
}

impl<RW: Read + Write> Bolt<RW> {
    fn new(
        version: (u8, u8),
        stream: RW,
        socket: Option<TcpStream>,
        local_port: Option<u16>,
        address: Arc<Address>,
    ) -> Self {
        Self {
            data: BoltData::new(version, stream, socket, local_port, address),
            protocol: match version {
                (5, 0) => Bolt5x0::<Bolt5x0StructTranslator>::default().into(),
                (4, 4) => Bolt4x4::<Bolt4x4StructTranslator>::default().into(),
                _ => panic!("implement protocol for version {:?}", version),
            },
        }
    }

    pub(crate) fn closed(&self) -> bool {
        self.data.closed()
    }

    pub(crate) fn unexpectedly_closed(&self) -> bool {
        self.data.unexpectedly_closed()
    }

    pub(crate) fn protocol_version(&self) -> (u8, u8) {
        self.data.version
    }

    pub(crate) fn address(&self) -> Arc<Address> {
        Arc::clone(&self.data.address)
    }

    pub(crate) fn server_agent(&self) -> Arc<String> {
        Arc::clone(self.data.server_agent.deref().borrow().deref())
    }

    pub(crate) fn hello(
        &mut self,
        user_agent: &str,
        auth: &HashMap<String, ValueSend>,
        routing_context: Option<&HashMap<String, ValueSend>>,
    ) -> Result<()> {
        self.protocol
            .hello(&mut self.data, user_agent, auth, routing_context)
    }

    pub(crate) fn goodbye(&mut self) -> Result<()> {
        self.protocol.goodbye(&mut self.data)
    }
    pub(crate) fn reset(&mut self) -> Result<()> {
        self.protocol.reset(&mut self.data)
    }
    pub(crate) fn run<KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        parameters: RunParameters<KP, KM>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.protocol.run(&mut self.data, parameters, callbacks)
    }
    pub(crate) fn discard(&mut self, n: i64, qid: i64, callbacks: ResponseCallbacks) -> Result<()> {
        self.protocol.discard(&mut self.data, n, qid, callbacks)
    }
    pub(crate) fn pull(&mut self, n: i64, qid: i64, callbacks: ResponseCallbacks) -> Result<()> {
        self.protocol.pull(&mut self.data, n, qid, callbacks)
    }
    pub(crate) fn begin<K: Borrow<str> + Debug>(
        &mut self,
        bookmarks: Option<&[String]>,
        tx_timeout: Option<i64>,
        tx_metadata: Option<&HashMap<K, ValueSend>>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
    ) -> Result<()> {
        self.protocol.begin(
            &mut self.data,
            bookmarks,
            tx_timeout,
            tx_metadata,
            mode,
            db,
            imp_user,
        )
    }
    pub(crate) fn commit(&mut self, callbacks: ResponseCallbacks) -> Result<()> {
        self.protocol.commit(&mut self.data, callbacks)
    }
    pub(crate) fn rollback(&mut self) -> Result<()> {
        self.protocol.rollback(&mut self.data)
    }
    pub(crate) fn route(
        &mut self,
        routing_context: &HashMap<String, ValueSend>,
        bookmarks: Option<&[String]>,
        db: Option<&str>,
        imp_user: Option<&str>,
        callbacks: ResponseCallbacks,
    ) -> Result<()> {
        self.protocol.route(
            &mut self.data,
            routing_context,
            bookmarks,
            db,
            imp_user,
            callbacks,
        )
    }

    pub(crate) fn read_all(
        &mut self,
        deadline: Option<Instant>,
        mut on_server_error: OnServerErrorCb,
    ) -> Result<()> {
        let on_server_error_ref = &mut on_server_error;
        while self.expects_reply() {
            self.read_one(deadline, on_server_error_ref.as_deref_mut())?;
        }
        Ok(())
    }

    pub(crate) fn read_one(
        &mut self,
        deadline: Option<Instant>,
        on_server_error: OnServerErrorCb,
    ) -> Result<()> {
        let mut reader =
            DeadlineIO::new(&mut self.data.stream, deadline, self.data.socket.as_ref());
        let mut dechunker = Dechunker::new(&mut reader);
        let message_result: Result<BoltMessage<ValueReceive>> =
            BoltMessage::load(&mut dechunker, |r| self.protocol.load_value(r));
        drop(dechunker);
        let message_result = reader.rewrite_error(message_result);
        let message = self.wrap_read_result(message_result)?;
        self.protocol
            .handle_response(&mut self.data, message, on_server_error)
    }

    fn wrap_read_result<T>(&mut self, res: Result<T>) -> Result<T> {
        if let Err(err) = &res {
            bolt_debug!(self.data, "read failed: {err:?}");
            self.data.connection_state = ConnectionState::Broken;
            self.data
                .socket
                .as_ref()
                .map(|s| s.shutdown(Shutdown::Both));
        }
        res
    }

    pub(crate) fn write_all(&mut self, deadline: Option<Instant>) -> Result<()> {
        self.data.write_all(deadline)?;
        self.data.flush(deadline)
    }
    pub(crate) fn write_one(&mut self, deadline: Option<Instant>) -> Result<()> {
        self.data.write_one(deadline)?;
        self.data.flush(deadline)
    }
    pub(crate) fn has_buffered_message(&self) -> bool {
        self.data.has_buffered_message()
    }
    pub(crate) fn buffered_messages_len(&self) -> usize {
        self.data.buffered_messages_len()
    }
    pub(crate) fn expects_reply(&self) -> bool {
        self.data.expects_reply()
    }
    pub(crate) fn expected_reply_len(&self) -> usize {
        self.data.expected_reply_len()
    }
    pub(crate) fn needs_reset(&self) -> bool {
        self.data.needs_reset()
    }
}

#[enum_dispatch]
trait BoltProtocol: Debug {
    fn hello<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        user_agent: &str,
        auth: &HashMap<String, ValueSend>,
        routing_context: Option<&HashMap<String, ValueSend>>,
    ) -> Result<()>;
    fn goodbye<RW: Read + Write>(&mut self, data: &mut BoltData<RW>) -> Result<()>;
    fn reset<RW: Read + Write>(&mut self, data: &mut BoltData<RW>) -> Result<()>;
    fn run<RW: Read + Write, KP: Borrow<str> + Debug, KM: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        parameters: RunParameters<KP, KM>,
        callbacks: ResponseCallbacks,
    ) -> Result<()>;
    fn discard<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        n: i64,
        qid: i64,
        callbacks: ResponseCallbacks,
    ) -> Result<()>;
    fn pull<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        n: i64,
        qid: i64,
        callbacks: ResponseCallbacks,
    ) -> Result<()>;
    #[allow(clippy::too_many_arguments)]
    fn begin<RW: Read + Write, K: Borrow<str> + Debug>(
        &mut self,
        data: &mut BoltData<RW>,
        bookmarks: Option<&[String]>,
        tx_timeout: Option<i64>,
        tx_metadata: Option<&HashMap<K, ValueSend>>,
        mode: Option<&str>,
        db: Option<&str>,
        imp_user: Option<&str>,
    ) -> Result<()>;
    fn commit<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        callbacks: ResponseCallbacks,
    ) -> Result<()>;
    fn rollback<RW: Read + Write>(&mut self, data: &mut BoltData<RW>) -> Result<()>;
    fn route<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        routing_context: &HashMap<String, ValueSend>,
        bookmarks: Option<&[String]>,
        db: Option<&str>,
        imp_user: Option<&str>,
        callbacks: ResponseCallbacks,
    ) -> Result<()>;

    fn load_value<R: Read>(&mut self, reader: &mut R) -> Result<ValueReceive>;
    fn handle_response<RW: Read + Write>(
        &mut self,
        data: &mut BoltData<RW>,
        message: BoltMessage<ValueReceive>,
        on_server_error: OnServerErrorCb,
    ) -> Result<()>;
}

#[enum_dispatch(BoltProtocol)]
#[derive(Debug)]
enum BoltProtocolVersion {
    V4x4(Bolt4x4<Bolt4x4StructTranslator>),
    V5x0(Bolt5x0<Bolt5x0StructTranslator>),
}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum ConnectionState {
    Healthy,
    Broken,
    Closed,
}

struct BoltData<RW: Read + Write> {
    message_buff: VecDeque<Vec<Vec<u8>>>,
    responses: VecDeque<BoltResponse>,
    stream: RW,
    socket: Option<TcpStream>,
    local_port: Option<u16>,
    version: (u8, u8),
    connection_state: ConnectionState,
    bolt_state: BoltStateTracker,
    meta: Arc<AtomicRefCell<HashMap<String, ValueReceive>>>,
    server_agent: Arc<AtomicRefCell<Arc<String>>>,
    address: Arc<Address>,
    address_str: String,
    last_qid: Arc<AtomicRefCell<Option<i64>>>,
}

impl<RW: Read + Write> BoltData<RW> {
    fn new(
        version: (u8, u8),
        stream: RW,
        socket: Option<TcpStream>,
        local_port: Option<u16>,
        address: Arc<Address>,
    ) -> Self {
        let address_str = address.to_string();
        Self {
            message_buff: VecDeque::with_capacity(2048),
            responses: VecDeque::with_capacity(10),
            stream,
            socket,
            local_port,
            version,
            connection_state: ConnectionState::Healthy,
            bolt_state: BoltStateTracker::new(version),
            meta: Default::default(),
            server_agent: Default::default(),
            address,
            address_str,
            last_qid: Default::default(),
        }
    }

    fn dbg_extra(&self) -> String {
        let meta = self.meta.try_borrow();
        let Ok(meta) = meta else {
            return dbg_extra(self.local_port, Some("!!!!"));
        };
        let Some(ValueReceive::String(id)) = meta.get("connection_id") else {
            return dbg_extra(self.local_port, None);
        };
        dbg_extra(self.local_port, Some(id))
    }

    fn closed(&self) -> bool {
        !matches!(self.connection_state, ConnectionState::Healthy)
    }

    fn unexpectedly_closed(&self) -> bool {
        matches!(self.connection_state, ConnectionState::Broken)
            && !matches!(self.bolt_state.state(), BoltState::Failed)
    }

    fn can_omit_qid(&self, qid: i64) -> bool {
        qid == -1 || Some(qid) == *(self.last_qid.deref().borrow())
    }

    fn serialize_dict<S: PackStreamSerializer, T: BoltStructTranslator, K: Borrow<str>>(
        &self,
        serializer: &mut S,
        translator: &T,
        map: &HashMap<K, ValueSend>,
    ) -> result::Result<(), S::Error> {
        serializer.write_dict_header(u64::from_usize(map.len()))?;
        for (k, v) in map {
            serializer.write_string(k.borrow())?;
            self.serialize_value(serializer, translator, v)?;
        }
        Ok(())
    }

    fn serialize_str_slice<S: PackStreamSerializer, V: Borrow<str>>(
        &self,
        serializer: &mut S,
        slice: &[V],
    ) -> result::Result<(), S::Error> {
        serializer.write_list_header(u64::from_usize(slice.len()))?;
        for v in slice {
            serializer.write_string(v.borrow())?;
        }
        Ok(())
    }

    fn serialize_value<S: PackStreamSerializer, T: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        translator: &T,
        v: &ValueSend,
    ) -> result::Result<(), S::Error> {
        translator.serialize(serializer, v).map_err(Into::into)
    }

    fn serialize_routing_context<S: PackStreamSerializer, T: BoltStructTranslator>(
        &self,
        serializer: &mut S,
        translator: &T,
        routing_context: &HashMap<String, ValueSend>,
    ) -> result::Result<(), S::Error> {
        serializer.write_dict_header(u64::from_usize(routing_context.len()))?;
        for (k, v) in routing_context {
            serializer.write_string(k.borrow())?;
            self.serialize_value(serializer, translator, v)?;
        }
        Ok(())
    }

    fn write_all(&mut self, deadline: Option<Instant>) -> Result<()> {
        while self.has_buffered_message() {
            self.write_one(deadline)?
        }
        Ok(())
    }

    fn write_one(&mut self, deadline: Option<Instant>) -> Result<()> {
        if let Some(message_buff) = self.message_buff.pop_front() {
            let chunker = Chunker::new(&message_buff);
            let mut writer = DeadlineIO::new(&mut self.stream, deadline, self.socket.as_ref());
            for chunk in chunker {
                let res = Neo4jError::wrap_write(writer.write_all(&chunk));
                let res = writer.rewrite_error(res);
                if let Err(err) = &res {
                    self.handle_write_error(err);
                    return res;
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self, deadline: Option<Instant>) -> Result<()> {
        let mut writer = DeadlineIO::new(&mut self.stream, deadline, self.socket.as_ref());
        let res = Neo4jError::wrap_write(writer.flush());
        let res = writer.rewrite_error(res);
        if let Err(err) = &res {
            self.handle_write_error(err);
            return res;
        }
        Ok(())
    }

    fn handle_write_error(&mut self, err: &Neo4jError) {
        bolt_debug!(self, "write failed: {}", err);
        self.connection_state = ConnectionState::Broken;
        self.socket.as_ref().map(|s| s.shutdown(Shutdown::Both));
    }

    fn has_buffered_message(&self) -> bool {
        !self.message_buff.is_empty()
    }
    fn buffered_messages_len(&self) -> usize {
        self.message_buff.len()
    }

    fn expects_reply(&self) -> bool {
        !self.responses.is_empty()
    }
    fn expected_reply_len(&self) -> usize {
        self.responses.len()
    }

    fn needs_reset(&self) -> bool {
        if let Some(response) = self.responses.iter().last() {
            if response.message == ResponseMessage::Reset {
                return false;
            }
        }
        if self.connection_state != ConnectionState::Healthy {
            return false;
        }
        !(self.bolt_state.state() == BoltState::Ready && self.responses.is_empty())
    }
}

impl<RW: Read + Write> Debug for BoltData<RW> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoltData")
            .field("message_buff", &self.message_buff)
            .field("responses", &self.responses)
            .finish()
    }
}

impl<RW: Read + Write> Drop for Bolt<RW> {
    fn drop(&mut self) {
        if self.data.closed() {
            return;
        }
        self.data.message_buff.clear();
        self.data.responses.clear();
        if self.goodbye().is_err() {
            return;
        }
        let _ = self
            .data
            .write_all(Some(Instant::now() + Duration::from_millis(100)));
    }
}

pub(crate) trait BoltStructTranslator: Debug + Default {
    fn serialize<S: PackStreamSerializer>(
        &self,
        serializer: &mut S,
        value: &ValueSend,
    ) -> result::Result<(), S::Error>;

    fn deserialize_struct(&self, tag: u8, fields: Vec<ValueReceive>) -> ValueReceive;
}

pub(crate) trait BoltStructTranslatorWithUtcPatch: BoltStructTranslator {
    fn enable_utc_patch(&mut self);
}

fn assert_response_field_count<T>(name: &str, fields: &[T], expected_count: usize) -> Result<()> {
    if fields.len() == expected_count {
        Ok(())
    } else {
        Err(Neo4jError::protocol_error(format!(
            "{} response should have {} field(s) but found {:?}",
            name,
            expected_count,
            fields.len()
        )))
    }
}

const BOLT_MAGIC_PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];
const BOLT_VERSION_OFFER: [u8; 16] = [
    0, 0, 0, 5, // BOLT 5.0
    0, 0, 4, 4, // BOLT 4.4
    0, 0, 0, 0, // -
    0, 0, 0, 0, // -
];

pub(crate) fn open(
    address: Arc<Address>,
    deadline: Option<Instant>,
    mut connect_timeout: Option<Duration>,
    tls_config: Option<Arc<ClientConfig>>,
) -> Result<TcpBolt> {
    if log_enabled!(Trace) {
        trace!(
            "{}{}",
            dbg_extra(None, None),
            format!("C: <OPEN> {address:?}")
        );
    } else {
        debug!(
            "{}{}",
            dbg_extra(None, None),
            format!("C: <OPEN> {address}")
        );
    }
    if let Some(deadline) = deadline {
        let mut time_left = deadline.saturating_duration_since(Instant::now());
        if time_left == Duration::from_secs(0) {
            time_left = Duration::from_nanos(1);
        }
        match connect_timeout {
            None => connect_timeout = Some(time_left),
            Some(timeout) => connect_timeout = Some(timeout.min(time_left)),
        }
    }
    let raw_socket = Neo4jError::wrap_connect(match connect_timeout {
        None => TcpStream::connect(&*address),
        Some(timeout) => each_addr(&*address, |addr| TcpStream::connect_timeout(addr?, timeout)),
    })?;
    let local_port = raw_socket
        .local_addr()
        .map(|addr| addr.port())
        .unwrap_or_default();

    // don't use a buffered socket if TLS is enabled
    // https://github.com/rustls/rustls/issues/1537
    let buffered = tls_config.is_none();
    let buffered_socket = BufTcpStream::new(&raw_socket, buffered)?;
    let mut socket = Socket::new(buffered_socket, address.unresolved_host(), tls_config)?;

    let mut deadline_io = DeadlineIO::new(&mut socket, deadline, Some(&raw_socket));

    socket_debug!(local_port, "C: <HANDSHAKE> {:02X?}", BOLT_MAGIC_PREAMBLE);
    wrap_write_socket(
        &raw_socket,
        local_port,
        deadline_io.write_all(&BOLT_MAGIC_PREAMBLE),
    )?;
    socket_debug!(local_port, "C: <BOLT> {:02X?}", BOLT_VERSION_OFFER);
    wrap_write_socket(
        &raw_socket,
        local_port,
        deadline_io.write_all(&BOLT_VERSION_OFFER),
    )?;
    wrap_write_socket(&raw_socket, local_port, deadline_io.flush())?;

    let mut negotiated_version = [0u8; 4];
    wrap_read_socket(
        &raw_socket,
        local_port,
        deadline_io.read_exact(&mut negotiated_version),
    )?;
    socket_debug!(local_port, "S: <BOLT> {:02X?}", negotiated_version);

    let version = match negotiated_version {
        [0, 0, 0, 0] => Err(Neo4jError::InvalidConfig {
            message: String::from("Server version not supported."),
        }),
        [0, 0, 0, 5] => Ok((5, 0)),
        [0, 0, 4, 4] => Ok((4, 4)),
        [72, 84, 84, 80] => {
            // "HTTP"
            Err(Neo4jError::InvalidConfig {
                message: format!(
                    "Unexpected server handshake response {:?} (looks like HTTP)",
                    &negotiated_version
                ),
            })
        }
        _ => Err(Neo4jError::InvalidConfig {
            message: format!(
                "Unexpected server handshake response {:?}",
                &negotiated_version
            ),
        }),
    }?;

    Ok(Bolt::new(
        version,
        socket,
        Some(raw_socket),
        Some(local_port),
        address,
    ))
}

// copied from std::net
fn each_addr<A: ToSocketAddrs, F, T>(addr: A, mut f: F) -> io::Result<T>
where
    F: FnMut(io::Result<&SocketAddr>) -> io::Result<T>,
{
    let addrs = match addr.to_socket_addrs() {
        Ok(addrs) => addrs,
        Err(e) => return f(Err(e)),
    };
    let mut last_err = None;
    for addr in addrs {
        match f(Ok(&addr)) {
            Ok(l) => return Ok(l),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not resolve to any addresses",
        )
    }))
}

fn wrap_write_socket<T>(stream: &TcpStream, local_port: u16, res: io::Result<T>) -> Result<T> {
    match res {
        Ok(res) => Ok(res),
        Err(err) => {
            socket_debug!(local_port, "   write error: {}", err);
            let _ = stream.shutdown(Shutdown::Both);
            Neo4jError::wrap_write(Err(err))
        }
    }
}

fn wrap_read_socket<T>(stream: &TcpStream, local_port: u16, res: io::Result<T>) -> Result<T> {
    match res {
        Ok(res) => Ok(res),
        Err(err) => {
            socket_debug!(local_port, "   read error: {}", err);
            let _ = stream.shutdown(Shutdown::Both);
            Neo4jError::wrap_read(Err(err))
        }
    }
}
