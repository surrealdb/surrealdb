use std::net::SocketAddr;
use std::time::Duration;
use anyhow::Result;
use console_subscriber::ConsoleLayer;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

pub fn new<S>() -> Result<impl Layer<S> + Send + Sync>
where
	S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync,
{
	let socket_addr = "".parse::<SocketAddr>()?;
	Ok(ConsoleLayer::builder()
		.retention(Duration::from_secs(60))
		.server_addr(socket_addr)
		.spawn())
}
