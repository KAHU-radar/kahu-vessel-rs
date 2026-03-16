mod protos;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use protobuf::Message;
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};

// Generated struct lives at protos::RadarMessage::RadarMessage
// (protobuf-codegen creates a module named after the .proto file,
//  containing the struct of the same name)
use protos::RadarMessage::RadarMessage;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    // TODO: read from config file; hard-code for initial bring-up
    // Radar ID comes from mayara's detected radar key (e.g. "nav1034A").
    // Check active IDs with: curl http://localhost:6502/v1/api/radars
    let mayara_url = "ws://127.0.0.1:6502/v2/api/radars/nav1034A/spokes";

    log::info!("kahu-daemon connecting to mayara at {}", mayara_url);

    let (mut ws, _response) = connect_async(mayara_url)
        .await
        .with_context(|| format!("connecting to mayara at {mayara_url}"))?;

    log::info!("connected — reading spokes");

    let mut spoke_count: u64 = 0;

    // Each WebSocket binary message is exactly one RadarMessage protobuf.
    // WS provides message framing, so we don't need a length prefix.
    while let Some(msg) = ws.next().await {
        let msg = msg.context("WebSocket receive error")?;

        let bytes = match msg {
            WsMessage::Binary(b) => b,
            WsMessage::Close(_) => {
                log::info!("mayara closed connection — processed {} spokes", spoke_count);
                break;
            }
            // ping/pong/text are not expected; skip them
            _ => continue,
        };

        let radar_msg =
            RadarMessage::parse_from_bytes(&bytes).context("parsing RadarMessage protobuf")?;

        for spoke in &radar_msg.spokes {
            spoke_count += 1;

            log::debug!(
                "spoke #{}: angle={} bearing={:?} range={}m data_len={} lat={:?} lon={:?}",
                spoke_count,
                spoke.angle,
                spoke.bearing,
                spoke.range,
                spoke.data.len(),
                spoke.lat,
                spoke.lon,
            );

            // Print a brief summary every 512 spokes so progress is visible
            // even without RUST_LOG=debug.
            if spoke_count % 512 == 0 {
                println!(
                    "[{} spokes] last: angle={} range={}m pixels={}",
                    spoke_count,
                    spoke.angle,
                    spoke.range,
                    spoke.data.len()
                );
            }
        }
    }

    println!("Done. Total spokes received: {}", spoke_count);
    Ok(())
}
