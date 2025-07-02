use std::time::Duration;
use tracing::Subscriber;
use tracing_subscriber::Layer;

pub fn new<S>() -> impl Layer<S> + Send + Sync
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	console_subscriber::ConsoleLayer::builder()
		.retention(Duration::from_secs(60))
		.server_addr(([127, 0, 0, 1], 6669))
		.spawn()
}
