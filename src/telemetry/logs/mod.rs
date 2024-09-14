use crate::cli::validator::parser::env_filter::CustomEnvFilter;
use tracing::Subscriber;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::Layer;

pub fn new<S>(filter: CustomEnvFilter) -> Box<dyn Layer<S> + Send + Sync>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	#[cfg(not(debug_assertions))]
	{
		tracing_subscriber::fmt::layer()
			.compact()
			.with_ansi(true)
			.with_target(true)
			.with_span_events(FmtSpan::NONE)
			.with_writer(std::io::stderr)
			.with_filter(filter.0)
			.boxed()
	}
	#[cfg(debug_assertions)]
	{
		tracing_subscriber::fmt::layer()
			.compact()
			.with_ansi(true)
			.with_file(true)
			.with_target(true)
			.with_line_number(true)
			.with_thread_ids(false)
			.with_thread_names(false)
			.with_span_events(FmtSpan::NONE)
			.with_writer(std::io::stderr)
			.with_filter(filter.0)
			.boxed()
	}
}
