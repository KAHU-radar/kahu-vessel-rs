mod protos;

use anyhow::Result;
use clap::Parser;
use futures_util::StreamExt;
use protobuf::Message;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

use protos::RadarMessage::RadarMessage;

// How long to wait before retrying a failed connection.
const RETRY_DELAY: Duration = Duration::from_secs(3);

#[derive(Parser, Debug)]
#[command(about = "KAHU vessel daemon — ingests mayara radar spokes")]
struct Args {
    /// Mayara WebSocket URL for the radar spoke stream.
    /// Radar key comes from: curl http://<host>:6502/v1/api/radars
    #[arg(long, default_value = "ws://127.0.0.1:6502/v1/api/spokes/nav1034A")]
    mayara_url: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    let mayara_url = &args.mayara_url;

    let mut total_spokes: u64 = 0;

    // Outer retry loop — keeps running across reconnects and replays.
    loop {
        log::info!("connecting to mayara at {}", mayara_url);

        let ws = match connect_async(mayara_url).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                log::warn!("connection failed ({}), retrying in {}s", e, RETRY_DELAY.as_secs());
                sleep(RETRY_DELAY).await;
                continue;
            }
        };

        log::info!("connected — reading spokes (total so far: {})", total_spokes);
        let mut ws = ws;

        loop {
            let msg = match ws.next().await {
                Some(Ok(m)) => m,
                Some(Err(e)) => {
                    log::warn!("WebSocket error: {}", e);
                    break; // reconnect
                }
                None => {
                    log::info!("mayara closed stream — will reconnect");
                    break; // reconnect
                }
            };

            let bytes = match msg {
                WsMessage::Binary(b) => b,
                WsMessage::Close(_) => {
                    log::info!("mayara sent close frame — will reconnect");
                    break;
                }
                _ => continue,
            };

            let radar_msg = match RadarMessage::parse_from_bytes(&bytes) {
                Ok(m) => m,
                Err(e) => {
                    log::warn!("protobuf parse error: {}", e);
                    continue;
                }
            };

            for spoke in &radar_msg.spokes {
                total_spokes += 1;

                log::debug!(
                    "spoke #{}: angle={} bearing={:?} range={}m pixels={} lat={:?} lon={:?}",
                    total_spokes,
                    spoke.angle,
                    spoke.bearing,
                    spoke.range,
                    spoke.data.len(),
                    spoke.lat,
                    spoke.lon,
                );

                if total_spokes % 512 == 0 {
                    println!(
                        "[{} spokes] angle={} range={}m pixels={}",
                        total_spokes,
                        spoke.angle,
                        spoke.range,
                        spoke.data.len()
                    );
                }
            }
        }

        sleep(RETRY_DELAY).await;
    }
}
