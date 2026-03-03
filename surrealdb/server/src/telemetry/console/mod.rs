use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result};
use console_subscriber::ConsoleLayer;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

use crate::cnf::TelemetryConfig;

const DEFAULT_TOKIO_CONSOLE_ADDR: SocketAddr =
	SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6669);

pub fn new<S>(telemetry: &TelemetryConfig) -> Result<Box<dyn Layer<S> + Send + Sync>>
where
	S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
	let socket_addr = match &telemetry.tokio_console_socket_addr {
		Some(addr) => addr.parse().context("failed to parse Tokio Console socket address")?,
		None => DEFAULT_TOKIO_CONSOLE_ADDR,
	};
	info!("Tokio Console server configured to run on {socket_addr}");
	Ok(Box::new(
		ConsoleLayer::builder()
			.server_addr(socket_addr)
			.retention(Duration::from_secs(telemetry.tokio_console_retention))
			.spawn(),
	))
}
