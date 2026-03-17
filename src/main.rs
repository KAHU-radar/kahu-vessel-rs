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
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

use protos::RadarMessage::RadarMessage;
use tracker::{Tracker, MIN_FIXES};
use upload::{TrackPoint, UploadTrack, Uploader};

const RETRY_DELAY: Duration = Duration::from_secs(3);
/// Spokes per full revolution — HALO radar default.  Override with --spokes.
const DEFAULT_SPOKES_PER_REV: u32 = 2048;

#[derive(Parser, Debug)]
#[command(about = "KAHU vessel daemon — radar spokes → target tracks → TrackServer")]
struct Args {
    /// Mayara WebSocket URL for the spoke stream.
    #[arg(long, default_value = "ws://127.0.0.1:6502/signalk/v2/api/vessels/self/radars/nav1034A/spokes")]
    mayara_url: String,

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

    /// Dry-run: detect and track but do not upload.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
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

    let mut tracker = Tracker::new();
    // Accumulate detections across spokes; flush once per revolution.
    let mut sweep_dets: Vec<(f64, f64)> = Vec::new();
    let mut prev_angle: Option<u32> = None;
    // Cache own-ship position — spokes only carry lat/lon when NMEA is fresh.
    let mut own_pos: Option<(f64, f64)> = None;

    loop {
        log::info!("connecting to mayara at {}", args.mayara_url);

        let ws = match connect_async(&args.mayara_url).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                log::warn!("connection failed: {} — retrying in {}s", e, RETRY_DELAY.as_secs());
                sleep(RETRY_DELAY).await;
                continue;
            }
        };

        log::info!("connected — reading spokes");
        let mut ws = ws;

        'stream: loop {
            let msg = match ws.next().await {
                Some(Ok(m)) => m,
                Some(Err(e)) => { log::warn!("ws error: {}", e); break 'stream; }
                None => { log::info!("stream closed — reconnecting"); break 'stream; }
            };

            let bytes = match msg {
                WsMessage::Binary(b) => b,
                WsMessage::Close(_) => break 'stream,
                WsMessage::Text(t) => {
                    log::debug!("text message: {}", &t[..t.len().min(120)]);
                    continue;
                }
                _ => continue,
            };

            log::debug!("binary frame: {} bytes", bytes.len());

            let radar_msg = match RadarMessage::parse_from_bytes(&bytes) {
                Ok(m) => m,
                Err(e) => { log::warn!("protobuf parse error: {}", e); continue; }
            };

            for spoke in &radar_msg.spokes {
                log::debug!(
                    "spoke angle={} bearing={:?} range={}m lat={:?} lon={:?} pixels={}",
                    spoke.angle, spoke.bearing, spoke.range, spoke.lat, spoke.lon, spoke.data.len()
                );

                // Skip spokes without a true bearing — can't compute lat/lon.
                let bearing = match spoke.bearing {
                    Some(b) => b,
                    None => continue,
                };

                // Own-ship position embedded in the spoke (1e-16 degrees).
                // Update cache when fresh; fall back to cached value otherwise.
                if let (Some(la), Some(lo)) = (spoke.lat, spoke.lon) {
                    own_pos = Some((la, lo));
                }
                let (own_lat, own_lon) = match own_pos {
                    Some(p) => p,
                    None => continue, // no position yet — skip until NMEA arrives
                };

                // Detect blobs on this spoke.
                let dets = detect::detect(&spoke.data, spoke.range, bearing, args.spokes);

                // Convert each detection to lat/lon.
                for d in dets {
                    let (lat, lon) = geo::polar_to_latlon(
                        own_lat,
                        own_lon,
                        d.range_m as f64,
                        d.bearing_rad as f64,
                    );
                    sweep_dets.push((lat, lon));
                }

                // Detect a new revolution: angle wraps around or drops significantly.
                let new_rev = prev_angle
                    .map(|prev| spoke.angle < prev && prev > args.spokes / 2)
                    .unwrap_or(false);
                prev_angle = Some(spoke.angle);

                if new_rev {
                    let ts = Utc::now();
                    let lost = tracker.update(&sweep_dets, ts);
                    log::debug!(
                        "sweep: {} detections, {} active tracks, {} lost",
                        sweep_dets.len(),
                        tracker.active_count(),
                        lost.len()
                    );
                    sweep_dets.clear();

                    for track in lost {
                        flush_track(track, &mut uploader);
                    }
                }
            }
        }

        // On disconnect: flush everything so we don't lose data.
        let all = tracker.drain();
        log::info!("disconnected — flushing {} tracks", all.len());
        for track in all {
            flush_track(track, &mut uploader);
        }
        sweep_dets.clear();
        prev_angle = None;

        sleep(RETRY_DELAY).await;
    }
}

fn flush_track(track: tracker::Track, uploader: &mut Option<Uploader>) {
    if track.fixes.len() < MIN_FIXES {
        log::debug!("dropping track {} ({} fixes)", &track.id.to_string()[..8], track.fixes.len());
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
