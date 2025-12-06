use std::error::Error;
use tracing::{error, info};

use crate::{logger::init_logger, server::connection::connection_listener};

pub mod logger;
pub mod server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // initialize logger
    info!("initializing the logger");
    init_logger();

    // read CLI args
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        error!("usage: cargo run <port>");
        std::process::exit(1);
    }

    let port: u16 = args[1].parse().map_err(|e| {
        error!("Failed to parse port, port must be a valid number");
        e
    })?;

    let address = format!("0.0.0.0:{}", port);

    // start connection listener
    connection_listener(address).await
}
