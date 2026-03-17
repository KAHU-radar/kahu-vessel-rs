/// Avro/TCP uploader — ports the Python AvroUploader from kahu-vessel/daemon/submit.py.
///
/// Protocol (matches TrackServer :9900):
///   1. Open TCP connection
///   2. Send Login call (API key)
///   3. Read Login response
///   4. For each track: send Submit call, read Submit response
///   5. On any error: reconnect and retry

use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use anyhow::{Context, Result};
use apache_avro::{from_avro_datum, to_avro_datum, Schema, types::Value};

const SCHEMA_JSON: &str = include_str!("../proto_avro.json");
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY: Duration = Duration::from_secs(5);

// ── Public types ──────────────────────────────────────────────────────────────

/// One point in a track.
#[derive(Debug, Clone)]
pub struct TrackPoint {
    pub lat: f32,
    pub lon: f32,
    /// Seconds since track start.
    pub offset_s: f32,
}

/// A completed track ready to upload.
#[derive(Debug)]
pub struct UploadTrack {
    pub uuid: String,
    pub start_ms: i64,
    pub points: Vec<TrackPoint>,
}

// ── Uploader ──────────────────────────────────────────────────────────────────

pub struct Uploader {
    host: String,
    port: u16,
    api_key: String,
    schema: Schema,
    conn: Option<TcpStream>,
    call_id: i32,
}

impl Uploader {
    pub fn new(host: &str, port: u16, api_key: &str) -> Result<Self> {
        let schema = Schema::parse_str(SCHEMA_JSON).context("failed to parse Avro schema")?;
        Ok(Self {
            host: host.to_string(),
            port,
            api_key: api_key.to_string(),
            schema,
            conn: None,
            call_id: 0,
        })
    }

    /// Upload a track, reconnecting and retrying on failure.
    pub fn submit(&mut self, track: &UploadTrack) -> Result<()> {
        loop {
            if self.conn.is_none() {
                match self.connect() {
                    Ok(()) => log::info!("connected to TrackServer {}:{}", self.host, self.port),
                    Err(e) => {
                        log::warn!("connect failed: {} — retrying in {}s", e, RETRY_DELAY.as_secs());
                        std::thread::sleep(RETRY_DELAY);
                        continue;
                    }
                }
            }
            match self.send_submit(track) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    log::warn!("submit failed: {} — reconnecting", e);
                    self.conn = None;
                }
            }
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    fn connect(&mut self) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let socket_addr = addr
            .to_socket_addrs()
            .context("DNS lookup failed")?
            .next()
            .context("no address for host")?;
        let stream = TcpStream::connect_timeout(&socket_addr, CONNECT_TIMEOUT)
            .context("TCP connect failed")?;
        stream.set_nodelay(true)?;
        self.conn = Some(stream);
        self.call_id = 0;
        self.send_login()
    }

    fn send_login(&mut self) -> Result<()> {
        self.call_id += 1;
        let msg = self.make_login(self.call_id, &self.api_key.clone());
        self.write_datum(&msg)?;
        let resp = self.read_datum()?;
        log::debug!("login response: {:?}", resp);
        Ok(())
    }

    fn send_submit(&mut self, track: &UploadTrack) -> Result<()> {
        self.call_id += 1;
        let msg = self.make_submit(self.call_id, track);
        self.write_datum(&msg)?;
        let resp = self.read_datum()?;
        log::info!(
            "submitted {}-pt track {} — response: {:?}",
            track.points.len(),
            &track.uuid[..8],
            resp
        );
        Ok(())
    }

    fn write_datum(&mut self, value: &Value) -> Result<()> {
        let bytes = to_avro_datum(&self.schema, value.clone())
            .context("Avro encode failed")?;
        let conn = self.conn.as_mut().context("not connected")?;
        conn.write_all(&bytes).context("TCP write failed")?;
        conn.flush().context("TCP flush failed")?;
        Ok(())
    }

    fn read_datum(&mut self) -> Result<Value> {
        let conn = self.conn.as_mut().context("not connected")?;
        // apache-avro reads from a `Read` impl; TcpStream implements Read.
        let value = from_avro_datum(&self.schema, conn, None)
            .context("Avro decode failed")?;
        Ok(value)
    }

    // ── Avro value builders ───────────────────────────────────────────────────

    fn make_login(&self, id: i32, api_key: &str) -> Value {
        Value::Record(vec![(
            "Message".into(),
            Value::Union(
                0,
                Box::new(Value::Record(vec![(
                    "Call".into(),
                    Value::Record(vec![
                        ("id".into(), Value::Int(id)),
                        (
                            "Call".into(),
                            Value::Union(
                                0,
                                Box::new(Value::Record(vec![(
                                    "Login".into(),
                                    Value::Record(vec![(
                                        "apikey".into(),
                                        Value::String(api_key.to_string()),
                                    )]),
                                )])),
                            ),
                        ),
                    ]),
                )])),
            ),
        )])
    }

    fn make_submit(&self, id: i32, track: &UploadTrack) -> Value {
        let route: Vec<Value> = track
            .points
            .iter()
            .map(|p| {
                Value::Record(vec![
                    ("lat".into(), Value::Float(p.lat)),
                    ("lon".into(), Value::Float(p.lon)),
                    ("timestamp".into(), Value::Float(p.offset_s)),
                ])
            })
            .collect();

        Value::Record(vec![(
            "Message".into(),
            Value::Union(
                0,
                Box::new(Value::Record(vec![(
                    "Call".into(),
                    Value::Record(vec![
                        ("id".into(), Value::Int(id)),
                        (
                            "Call".into(),
                            Value::Union(
                                1,
                                Box::new(Value::Record(vec![(
                                    "Submit".into(),
                                    Value::Record(vec![
                                        (
                                            "uuid".into(),
                                            Value::Union(
                                                1,
                                                Box::new(Value::String(track.uuid.clone())),
                                            ),
                                        ),
                                        ("route".into(), Value::Array(route)),
                                        ("nmea".into(), Value::Union(0, Box::new(Value::Null))),
                                        ("start".into(), Value::TimestampMillis(track.start_ms)),
                                    ]),
                                )])),
                            ),
                        ),
                    ]),
                )])),
            ),
        )])
    }
}
