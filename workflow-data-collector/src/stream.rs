//! Stream source (§3 follow): the Redis `XREAD` client behind a small trait
//! seam so the follow loop is testable with a scripted fake.

use std::collections::BTreeMap;

use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::Commands;

/// One stream entry as XREAD returns it: an id + the flat string field map.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    pub id: String,
    pub fields: BTreeMap<String, String>,
}

/// Read failures. `Unavailable` (Redis down, timeouts) is non-fatal — the
/// loop backs off and retries; `Protocol` means the reply was garbage.
#[derive(Debug)]
pub enum StreamError {
    Unavailable(String),
    Protocol(String),
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Unavailable(m) => write!(f, "stream unavailable: {m}"),
            StreamError::Protocol(m) => write!(f, "stream protocol error: {m}"),
        }
    }
}

impl std::error::Error for StreamError {}

/// Read one XREAD batch: entries with id `> from`, blocking up to `block_ms`.
pub trait StreamSource {
    fn xread(
        &mut self,
        stream: &str,
        from: &str,
        block_ms: u64,
        count: usize,
    ) -> Result<Vec<StreamEntry>, StreamError>;

    /// Read one XRANGE page: entries with id in `[from, to]` (inclusive,
    /// Redis XRANGE semantics), up to `count`. Used by `backfill` for a
    /// chosen-range replay; `follow` never calls it.
    fn xrange(
        &mut self,
        stream: &str,
        from: &str,
        to: &str,
        count: usize,
    ) -> Result<Vec<StreamEntry>, StreamError>;

    /// Whether the stream key exists (§3: a missing key is logged and
    /// retried with backoff, like Redis being down — never a silent idle).
    fn stream_exists(&mut self, stream: &str) -> Result<bool, StreamError>;
}

const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Redis-backed [`StreamSource`]. Self-healing: a failed read drops the
/// connection and the next call reconnects, so the loop recovers once Redis
/// is back without any reconnect logic of its own.
pub struct RedisStream {
    client: redis::Client,
    conn: Option<redis::Connection>,
}

impl RedisStream {
    /// Parse the URL only (no network). A malformed URL is a config error —
    /// it must never be mistaken for "Redis down" (which backs off forever).
    pub fn new(url: &str) -> Result<RedisStream, crate::Error> {
        let client = redis::Client::open(url)
            .map_err(|e| crate::Error::Config(format!("invalid redis_url {url:?}: {e}")))?;
        Ok(RedisStream { client, conn: None })
    }

    fn ensure_conn(&mut self) -> Result<&mut redis::Connection, StreamError> {
        if self.conn.is_none() {
            self.conn = Some(
                self.client
                    .get_connection_with_timeout(CONNECT_TIMEOUT)
                    .map_err(|e| StreamError::Unavailable(format!("connect: {e}")))?,
            );
        }
        Ok(self.conn.as_mut().expect("conn set above"))
    }
}

impl StreamSource for RedisStream {
    fn xread(
        &mut self,
        stream: &str,
        from: &str,
        block_ms: u64,
        count: usize,
    ) -> Result<Vec<StreamEntry>, StreamError> {
        let opts = StreamReadOptions::default()
            .block(block_ms as usize)
            .count(count);
        let result = self.ensure_conn().and_then(|conn| {
            conn.xread_options(&[stream], &[from], &opts)
                .map_err(|e| StreamError::Unavailable(format!("xread: {e}")))
        });
        let reply: Option<StreamReadReply> = match result {
            Ok(r) => r,
            Err(e) => {
                // drop the broken connection; the next call reconnects
                self.conn = None;
                return Err(e);
            }
        };
        let mut out = Vec::new();
        for key in reply.into_iter().flat_map(|r| r.keys) {
            for sid in key.ids {
                let fields = sid
                    .map
                    .iter()
                    .map(|(k, v)| (k.clone(), redis_string(v)))
                    .collect();
                out.push(StreamEntry { id: sid.id, fields });
            }
        }
        Ok(out)
    }

    fn xrange(
        &mut self,
        stream: &str,
        from: &str,
        to: &str,
        count: usize,
    ) -> Result<Vec<StreamEntry>, StreamError> {
        let result = self.ensure_conn().and_then(|conn| {
            conn.xrange_count(stream, from, to, count)
                .map_err(|e| StreamError::Unavailable(format!("xrange: {e}")))
        });
        let reply: redis::streams::StreamRangeReply = match result {
            Ok(r) => r,
            Err(e) => {
                // drop the broken connection; the next call reconnects
                self.conn = None;
                return Err(e);
            }
        };
        // The redis crate hands back each entry's field map as a HashMap;
        // collect into a BTreeMap so serialized output is deterministic.
        Ok(reply
            .ids
            .iter()
            .map(|sid| StreamEntry {
                id: sid.id.clone(),
                fields: sid
                    .map
                    .iter()
                    .map(|(k, v)| (k.clone(), redis_string(v)))
                    .collect(),
            })
            .collect())
    }

    fn stream_exists(&mut self, stream: &str) -> Result<bool, StreamError> {
        let result = self.ensure_conn().and_then(|conn| {
            redis::cmd("EXISTS")
                .arg(stream)
                .query::<i64>(conn)
                .map_err(|e| StreamError::Unavailable(format!("exists: {e}")))
        });
        match result {
            Ok(n) => Ok(n > 0),
            Err(e) => {
                self.conn = None;
                Err(e)
            }
        }
    }
}

/// Convert a redis `Value` (always a bulk string for stream fields) to String.
fn redis_string(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        redis::Value::SimpleString(s) => s.clone(),
        other => format!("{other:?}"),
    }
}

/// Scripted [`StreamSource`] for loop tests.
#[cfg(test)]
pub struct FakeSource {
    queue: std::collections::VecDeque<Result<Vec<StreamEntry>, StreamError>>,
    /// Per-read `stream_exists` answers; falls back to the last answer.
    exists_queue: std::collections::VecDeque<bool>,
    exists_default: bool,
}

#[cfg(test)]
impl Default for FakeSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl FakeSource {
    pub fn new() -> Self {
        FakeSource {
            queue: std::collections::VecDeque::new(),
            exists_queue: std::collections::VecDeque::new(),
            exists_default: true,
        }
    }
    pub fn push(&mut self, r: Result<Vec<StreamEntry>, StreamError>) {
        self.queue.push_back(r);
    }
    /// Script the next `stream_exists` answer (§3 missing-key scenario).
    pub fn push_exists(&mut self, exists: bool) {
        self.exists_queue.push_back(exists);
    }
    pub fn drained(&self) -> bool {
        self.queue.is_empty()
    }
    pub fn entry(id: &str, pairs: &[(&str, &str)]) -> StreamEntry {
        StreamEntry {
            id: id.to_string(),
            fields: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }
}

#[cfg(test)]
impl StreamSource for FakeSource {
    fn xread(
        &mut self,
        _stream: &str,
        _from: &str,
        _block_ms: u64,
        _count: usize,
    ) -> Result<Vec<StreamEntry>, StreamError> {
        self.queue.pop_front().unwrap_or_else(|| Ok(Vec::new()))
    }

    fn xrange(
        &mut self,
        _stream: &str,
        _from: &str,
        _to: &str,
        _count: usize,
    ) -> Result<Vec<StreamEntry>, StreamError> {
        self.queue.pop_front().unwrap_or_else(|| Ok(Vec::new()))
    }

    fn stream_exists(&mut self, _stream: &str) -> Result<bool, StreamError> {
        match self.exists_queue.pop_front() {
            Some(v) => {
                self.exists_default = v;
                Ok(v)
            }
            None => Ok(self.exists_default),
        }
    }
}
