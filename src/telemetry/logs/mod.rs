use crate::cli::validator::parser::env_filter::CustomEnvFilter;
use crate::err::Error;
use tracing::Level;
use tracing::Subscriber;
use tracing_appender::non_blocking::NonBlocking;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::Layer;

pub fn new<S>(
	filter: CustomEnvFilter,
	stdout: NonBlocking,
	stderr: NonBlocking,
) -> Result<Box<dyn Layer<S> + Send + Sync>, Error>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	// Log INFO, DEBUG, TRACE to stdout, WARN, ERROR to stderr
	let writer = stderr.with_max_level(Level::WARN).or_else(stdout);
	// Configure the log tracer for production
	#[cfg(not(debug_assertions))]
	{
		Ok(tracing_subscriber::fmt::layer()
			.compact()
			.with_ansi(true)
			.with_file(false)
			.with_target(true)
			.with_line_number(false)
			.with_thread_ids(false)
			.with_thread_names(false)
			.with_span_events(FmtSpan::NONE)
			.with_writer(writer)
			.with_filter(filter.0)
			.boxed())
	}
	// Configure the log tracer for development
	#[cfg(debug_assertions)]
	{
		Ok(tracing_subscriber::fmt::layer()
			.compact()
			.with_ansi(true)
			.with_file(true)
			.with_target(true)
			.with_line_number(true)
			.with_thread_ids(false)
			.with_thread_names(false)
			.with_span_events(FmtSpan::NONE)
			.with_writer(writer)
			.with_filter(filter.0)
			.boxed())
	}
}
