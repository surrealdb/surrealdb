use crate::cnf::TOKIO_CONSOLE_RETENTION;
use crate::cnf::TOKIO_CONSOLE_SOCKET_ADDR;
use anyhow::Context;
use anyhow::Result;
use console_subscriber::ConsoleLayer;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::time::Duration;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

const DEFAULT_TOKIO_CONSOLE_ADDR: SocketAddr =
	SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6669);

pub fn new<S>() -> Result<impl Layer<S> + Send + Sync>
where
	S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
	let socket_addr = match &*TOKIO_CONSOLE_SOCKET_ADDR {
		Some(addr) => addr.parse().context("failed to parse Tokio Console socket address")?,
		None => DEFAULT_TOKIO_CONSOLE_ADDR,
	};
	info!("Tokio Console server configured to run on {socket_addr}");
	Ok(ConsoleLayer::builder()
		.server_addr(socket_addr)
		.retention(Duration::from_secs(*TOKIO_CONSOLE_RETENTION))
		.spawn())
}
