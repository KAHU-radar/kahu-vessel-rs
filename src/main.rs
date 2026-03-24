mod clutter;
mod detect;
mod geo;
mod tracker;
mod upload;
mod protos;

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use futures_util::StreamExt;
use protobuf::Message as _;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

use clutter::ClutterMap;
use protos::RadarMessage::RadarMessage;
use tracker::{Tracker, MIN_FIXES};
use upload::{TrackPoint, UploadTrack, Uploader};

/// Spokes per full revolution — HALO radar default.  Override with --spokes.
const DEFAULT_SPOKES_PER_REV: u32 = 2048;

#[derive(Parser, Debug)]
#[command(about = "KAHU vessel daemon — radar spokes → target tracks → TrackServer")]
#[command(long_about = "Reads RadarMessages from mayara via WebSocket (default) or length-prefixed\n\
    stdin stream.\n\
    WebSocket:  kahu-daemon --ws-url ws://localhost:6502/signalk/v2/api/vessels/self/radars/<id>/spokes\n\
    Stdin pipe: mayara --output ... | kahu-daemon --stdin")]
struct Args {
    /// TrackServer hostname or IP.
    #[arg(long, default_value = "crowdsource.kahu.earth")]
    upload_host: String,

    /// TrackServer Avro/TCP port.
    #[arg(long, default_value_t = 9900)]
    upload_port: u16,

    /// KAHU API key (or set KAHU_API_KEY env var).
    #[arg(long, env = "KAHU_API_KEY", default_value = "")]
    api_key: String,

    /// Spokes per full revolution (default: 2048 for HALO).
    #[arg(long, default_value_t = DEFAULT_SPOKES_PER_REV)]
    spokes: u32,

    /// Flush all active tracks and reconnect if no spoke data arrives for this
    /// many seconds.  Useful when a radar goes unexpectedly silent — partial
    /// tracks are uploaded rather than lost.  0 = disabled (default).
    #[arg(long, default_value_t = 0)]
    spoke_timeout: u64,

    /// Dry-run: detect and track but do not upload.
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Enable the sliding-window land / clutter filter.
    /// Suppresses stationary returns (coastlines, docks) after a ~30-sweep
    /// warm-up period.  Recommended for coastal deployments.
    #[arg(long, default_value_t = false)]
    land_filter: bool,

    /// WebSocket URL for mayara's spoke stream.
    /// e.g. ws://localhost:6502/signalk/v2/api/vessels/self/radars/<id>/spokes
    /// Takes precedence over --stdin if both are given.
    #[arg(long)]
    ws_url: Option<String>,

    /// Read length-prefixed protobuf frames from stdin (legacy / pipe mode).
    /// Use when piping: mayara --output ... | kahu-daemon --stdin
    #[arg(long, default_value_t = false)]
    stdin: bool,

    /// Seconds to wait on startup before connecting to mayara's WebSocket.
    /// Gives mayara time to discover the radar before the first connection
    /// attempt.  Only applied once at startup; reconnect retries are not
    /// affected.
    #[arg(long, default_value_t = 10)]
    startup_delay: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    let mut uploader = if args.dry_run {
        None
    } else {
        Some(Uploader::new(&args.upload_host, args.upload_port, &args.api_key)?)
    };

    let mut state = ProcessState::new(args.spokes, args.land_filter);

    if let Some(ref url) = args.ws_url {
        if args.startup_delay > 0 {
            println!("Waiting {}s for mayara to initialise...", args.startup_delay);
            tokio::time::sleep(tokio::time::Duration::from_secs(args.startup_delay)).await;
        }
        run_websocket(&mut state, url, &mut uploader, args.spoke_timeout).await?;
    } else {
        // Default: read from stdin (length-prefixed frames written by mayara --output
        // after the framing fix, or piped via --stdin flag).
        run_stdin(&mut state, &mut uploader).await?;
    }

    // Flush all remaining active tracks on clean exit.
    let all = state.tracker.drain();
    log::info!("flushing {} remaining tracks", all.len());
    for track in all {
        flush_track(track, &mut uploader);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Processing state — shared between stdin and WebSocket input paths
// ---------------------------------------------------------------------------

struct ProcessState {
    tracker: Tracker,
    clutter: Option<ClutterMap>,
    sweep_dets: Vec<(f64, f64)>,
    prev_angle: Option<u32>,
    own_pos: Option<(f64, f64)>,
    spokes_per_rev: u32,
}

impl ProcessState {
    fn new(spokes_per_rev: u32, land_filter: bool) -> Self {
        Self {
            tracker: Tracker::new(),
            clutter: land_filter.then(ClutterMap::new),
            sweep_dets: Vec::new(),
            prev_angle: None,
            own_pos: None,
            spokes_per_rev,
        }
    }

    /// Parse one serialised RadarMessage and process all its spokes.
    /// Returns tracks that completed during this message.
    fn process_bytes(&mut self, bytes: &[u8]) -> Vec<tracker::Track> {
        let radar_msg = match RadarMessage::parse_from_bytes(bytes) {
            Ok(m) => m,
            Err(e) => {
                log::warn!("protobuf parse error: {}", e);
                return vec![];
            }
        };

        let mut flushed = Vec::new();

        for spoke in &radar_msg.spokes {
            log::debug!(
                "spoke angle={} bearing={:?} range={}m lat={:?} lon={:?} pixels={}",
                spoke.angle,
                spoke.bearing,
                spoke.range,
                spoke.lat,
                spoke.lon,
                spoke.data.len()
            );

            // Skip spokes without a true bearing — can't compute lat/lon.
            let bearing = match spoke.bearing {
                Some(b) => b,
                None => continue,
            };

            // Own-ship position embedded in the spoke.
            if let (Some(la), Some(lo)) = (spoke.lat, spoke.lon) {
                self.own_pos = Some((la, lo));
            }
            let (own_lat, own_lon) = match self.own_pos {
                Some(p) => p,
                None => continue, // no position yet — skip until NMEA arrives
            };

            let dets = detect::detect(&spoke.data, spoke.range, bearing, self.spokes_per_rev);

            for d in dets {
                let (lat, lon) = geo::polar_to_latlon(
                    own_lat,
                    own_lon,
                    d.range_m as f64,
                    d.bearing_rad as f64,
                );
                self.sweep_dets.push((lat, lon));
            }

            // Detect a new revolution: angle wraps around or drops significantly.
            let new_rev = self
                .prev_angle
                .map(|prev| spoke.angle < prev && prev > self.spokes_per_rev / 2)
                .unwrap_or(false);
            self.prev_angle = Some(spoke.angle);

            if new_rev {
                let ts = Utc::now();
                let filtered: Vec<(f64, f64)> = if let Some(ref mut cm) = self.clutter {
                    let f = cm.filter(&self.sweep_dets);
                    log::debug!(
                        "clutter: {}/{} dets passed ({} map cells)",
                        f.len(),
                        self.sweep_dets.len(),
                        cm.active_cells()
                    );
                    f
                } else {
                    self.sweep_dets.clone()
                };
                let lost = self.tracker.update(&filtered, ts);
                log::debug!(
                    "sweep: {} detections, {} active tracks, {} flushed",
                    filtered.len(),
                    self.tracker.active_count(),
                    lost.len()
                );
                self.sweep_dets.clear();
                flushed.extend(lost);
            }
        }

        flushed
    }
}

// ---------------------------------------------------------------------------
// WebSocket input — reconnecting loop with graceful SIGINT shutdown
// ---------------------------------------------------------------------------

/// Seconds to wait between reconnect attempts.
const RECONNECT_DELAY_S: u64 = 5;

async fn run_websocket(
    state: &mut ProcessState,
    url: &str,
    uploader: &mut Option<Uploader>,
    spoke_timeout: u64,
) -> Result<()> {
    // Spawn a task that watches for Ctrl-C and signals the watch channel.
    // All select! arms below subscribe to this channel so shutdown interrupts
    // both the active connection and any reconnect sleep.
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        log::info!("SIGINT received — will flush tracks and exit");
        let _ = stop_tx.send(true);
    });

    loop {
        if *stop_rx.borrow() {
            break;
        }

        log::info!("connecting to {}", url);
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                log::info!("WebSocket connected");
                let (_write, mut read) = ws_stream.split();

                // Inner loop: process frames until the connection drops,
                // a spoke timeout fires, or shutdown is requested.
                //
                // The sleep is pinned and persists across iterations so that
                // non-binary frames (pings, text metadata) do NOT reset the
                // countdown — only arriving binary spoke data does.
                // Pinned sleep used only when spoke_timeout > 0.  The select!
                // guard `if spoke_timeout > 0` keeps this arm disabled when
                // the feature is off, avoiding any need for a "far future" sentinel.
                let timeout_dur =
                    tokio::time::Duration::from_secs(spoke_timeout.max(1));
                let sleep = tokio::time::sleep(timeout_dur);
                tokio::pin!(sleep);

                loop {
                    tokio::select! {
                        msg = read.next() => {
                            match msg {
                                Some(Ok(WsMessage::Binary(data))) => {
                                    log::debug!("ws frame: {} bytes", data.len());
                                    // Binary spoke data arrived — reset the countdown.
                                    if spoke_timeout > 0 {
                                        sleep.as_mut().reset(
                                            tokio::time::Instant::now() + timeout_dur,
                                        );
                                    }
                                    let tracks = state.process_bytes(&data);
                                    for track in tracks {
                                        flush_track(track, uploader);
                                    }
                                }
                                Some(Ok(WsMessage::Close(_))) | None => {
                                    log::info!("WebSocket closed by server — will reconnect");
                                    break;
                                }
                                Some(Err(e)) => {
                                    log::warn!("WebSocket error: {} — will reconnect", e);
                                    break;
                                }
                                Some(Ok(_)) => {} // Ping, Pong, Text, Frame — ignore
                            }
                        }
                        _ = &mut sleep, if spoke_timeout > 0 => {
                            log::info!("no spoke data for {}s — flushing tracks", spoke_timeout);
                            let stale = state.tracker.drain();
                            log::info!("flushing {} stale tracks", stale.len());
                            for track in stale {
                                flush_track(track, uploader);
                            }
                            break; // triggers reconnect; fresh tracker state
                        }
                        Ok(_) = stop_rx.changed() => {
                            // SIGINT fired — break inner loop; outer loop will
                            // see stop=true and exit, then main() drains tracks.
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                log::warn!("connection failed: {} — retrying in {}s", e, RECONNECT_DELAY_S);
            }
        }

        if *stop_rx.borrow() {
            break;
        }

        // Wait before reconnecting, but exit immediately on SIGINT.
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(RECONNECT_DELAY_S)) => {}
            Ok(_) = stop_rx.changed() => {}
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Stdin input — length-prefixed frames: [u32 LE length][protobuf bytes]
// ---------------------------------------------------------------------------

async fn run_stdin(state: &mut ProcessState, uploader: &mut Option<Uploader>) -> Result<()> {
    log::info!("reading mayara protobuf stream from stdin (length-prefixed)");

    let mut reader = BufReader::new(tokio::io::stdin());

    loop {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                log::info!("stdin closed — mayara exited");
                break;
            }
            Err(e) => {
                log::error!("stdin read error: {}", e);
                break;
            }
        }
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut msg_buf = vec![0u8; len];
        if let Err(e) = reader.read_exact(&mut msg_buf).await {
            log::error!("stdin read error (body, expected {} bytes): {}", len, e);
            break;
        }

        log::debug!("frame: {} bytes", len);

        let tracks = state.process_bytes(&msg_buf);
        for track in tracks {
            flush_track(track, uploader);
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Track flushing — shared by both input paths
// ---------------------------------------------------------------------------

fn flush_track(track: tracker::Track, uploader: &mut Option<Uploader>) {
    if track.fixes.len() < MIN_FIXES {
        log::debug!(
            "dropping track {} ({} fixes)",
            &track.id.to_string()[..8],
            track.fixes.len()
        );
        return;
    }

    let start = track.fixes[0].ts;
    let start_ms = start.timestamp_millis();
    let points: Vec<TrackPoint> = track
        .fixes
        .iter()
        .map(|f| TrackPoint {
            lat: f.lat as f32,
            lon: f.lon as f32,
            offset_s: (f.ts - start).num_milliseconds() as f32 / 1000.0,
        })
        .collect();

    println!(
        "[track {}] {} pts | ({:.5},{:.5}) → ({:.5},{:.5})",
        &track.id.to_string()[..8],
        points.len(),
        points.first().map(|p| p.lat).unwrap_or(0.0),
        points.first().map(|p| p.lon).unwrap_or(0.0),
        points.last().map(|p| p.lat).unwrap_or(0.0),
        points.last().map(|p| p.lon).unwrap_or(0.0),
    );

    let upload = UploadTrack {
        uuid: track.id.to_string(),
        start_ms,
        points,
    };

    if let Some(up) = uploader.as_mut() {
        if let Err(e) = up.submit(&upload) {
            log::error!("upload failed: {}", e);
        }
    }
}
