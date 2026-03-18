mod clutter;
mod detect;
mod geo;
mod tracker;
mod upload;
mod protos;

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use protobuf::Message as _;
use tokio::io::{AsyncReadExt, BufReader};

use clutter::ClutterMap;
use protos::RadarMessage::RadarMessage;
use tracker::{Tracker, MIN_FIXES};
use upload::{TrackPoint, UploadTrack, Uploader};

/// Spokes per full revolution — HALO radar default.  Override with --spokes.
const DEFAULT_SPOKES_PER_REV: u32 = 2048;

#[derive(Parser, Debug)]
#[command(about = "KAHU vessel daemon — radar spokes → target tracks → TrackServer")]
#[command(long_about = "Reads length-prefixed protobuf RadarMessages from stdin.\n\
    Launch via: mayara-server --output ... | kahu-daemon [options]")]
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

    /// Dry-run: detect and track but do not upload.
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Enable the sliding-window land / clutter filter.
    /// Suppresses stationary returns (coastlines, docks) after a ~30-sweep
    /// warm-up period.  Recommended for coastal deployments.
    #[arg(long, default_value_t = false)]
    land_filter: bool,
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
    let mut clutter = args.land_filter.then(ClutterMap::new);
    // Accumulate detections across spokes; flush once per revolution.
    let mut sweep_dets: Vec<(f64, f64)> = Vec::new();
    let mut prev_angle: Option<u32> = None;
    // Cache own-ship position — spokes only carry lat/lon when NMEA is fresh.
    let mut own_pos: Option<(f64, f64)> = None;

    log::info!("reading mayara protobuf stream from stdin");

    let mut reader = BufReader::new(tokio::io::stdin());

    loop {
        // Each message is framed as: [u32 LE length][protobuf bytes].
        // This matches the length prefix written by mayara's forward_output().
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

        let radar_msg = match RadarMessage::parse_from_bytes(&msg_buf) {
            Ok(m) => m,
            Err(e) => {
                log::warn!("protobuf parse error: {}", e);
                continue;
            }
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

            // Own-ship position embedded in the spoke.
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
                let filtered: Vec<(f64, f64)> = if let Some(ref mut cm) = clutter {
                    let f = cm.filter(&sweep_dets);
                    log::debug!(
                        "clutter: {}/{} dets passed ({} map cells)",
                        f.len(), sweep_dets.len(), cm.active_cells()
                    );
                    f
                } else {
                    sweep_dets.clone()
                };
                let lost = tracker.update(&filtered, ts);
                log::debug!(
                    "sweep: {} detections, {} active tracks, {} flushed",
                    filtered.len(),
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

    // Flush all remaining active tracks on clean exit.
    let all = tracker.drain();
    log::info!("flushing {} remaining tracks", all.len());
    for track in all {
        flush_track(track, &mut uploader);
    }

    Ok(())
}

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
