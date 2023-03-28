use tracing::Subscriber;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{EnvFilter, Layer};

pub fn new<S>(level: String) -> Box<dyn Layer<S> + Send + Sync>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	tracing_subscriber::fmt::layer()
		.compact()
		.with_ansi(true)
		.with_span_events(FmtSpan::NONE)
		.with_writer(std::io::stderr)
		.with_filter(EnvFilter::builder().parse(level).unwrap())
		.boxed()
}
