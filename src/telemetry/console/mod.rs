use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result};
use console_subscriber::ConsoleLayer;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

use crate::cnf::{TOKIO_CONSOLE_RETENTION, TOKIO_CONSOLE_SOCKET_ADDR};

const DEFAULT_TOKIO_CONSOLE_ADDR: SocketAddr =
	SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6669);

pub fn new<S>() -> Result<Box<dyn Layer<S> + Send + Sync>>
where
	S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
	let socket_addr = match &*TOKIO_CONSOLE_SOCKET_ADDR {
		Some(addr) => addr.parse().context("failed to parse Tokio Console socket address")?,
		None => DEFAULT_TOKIO_CONSOLE_ADDR,
	};
	info!("Tokio Console server configured to run on {socket_addr}");
	Ok(Box::new(
		ConsoleLayer::builder()
			.server_addr(socket_addr)
			.retention(Duration::from_secs(*TOKIO_CONSOLE_RETENTION))
			.spawn(),
	))
}
