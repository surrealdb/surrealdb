pub mod socket;

use anyhow::Result;
use tracing::{Level, Subscriber};
use tracing_appender::non_blocking::NonBlocking;
use tracing_subscriber::Layer;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::writer::MakeWriterExt;

use crate::cli::LogFormat;
use crate::cli::validator::parser::tracing::CustomFilter;

pub fn file<S>(
	filter: CustomFilter,
	file: NonBlocking,
	format: LogFormat,
) -> Result<Box<dyn Layer<S> + Send + Sync>>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	// Log ERROR, WARN, INFO, DEBUG, TRACE to rotating file
	let writer = file.with_max_level(Level::TRACE);
	// Configure the log file tracer
	let layer = tracing_subscriber::fmt::layer();
	// Configure the log file format
	match format {
		LogFormat::Json => Ok(layer
			.json()
			.with_ansi(false)
			.with_file(false)
			.with_target(true)
			.with_line_number(false)
			.with_thread_ids(false)
			.with_thread_names(false)
			.with_span_events(FmtSpan::NONE)
			.with_writer(writer)
			.with_filter(filter.env())
			.with_filter(filter.span_filter::<S>())
			.boxed()),
		LogFormat::Text => Ok(layer
			.compact()
			.with_ansi(false)
			.with_file(false)
			.with_target(true)
			.with_line_number(false)
			.with_thread_ids(false)
			.with_thread_names(false)
			.with_span_events(FmtSpan::NONE)
			.with_writer(writer)
			.with_filter(filter.env())
			.with_filter(filter.span_filter::<S>())
			.boxed()),
	}
}

pub fn output<S>(
	filter: CustomFilter,
	stdout: NonBlocking,
	stderr: NonBlocking,
	format: LogFormat,
) -> Result<Box<dyn Layer<S> + Send + Sync>>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	// Log INFO, DEBUG, TRACE to stdout, WARN, ERROR to stderr
	let writer = stderr.with_max_level(Level::WARN).or_else(stdout);
	// Configure the log tracer for production
	#[cfg(not(debug_assertions))]
	{
		let layer = tracing_subscriber::fmt::layer();
		// Configure the log console writer
		match format {
			LogFormat::Json => Ok(layer
				.json()
				.with_ansi(true)
				.with_file(false)
				.with_target(true)
				.with_line_number(false)
				.with_thread_ids(false)
				.with_thread_names(false)
				.with_span_events(FmtSpan::NONE)
				.with_writer(writer)
				.with_filter(filter.env())
				.with_filter(filter.span_filter::<S>())
				.boxed()),
			LogFormat::Text => Ok(layer
				.compact()
				.with_ansi(true)
				.with_file(false)
				.with_target(true)
				.with_line_number(false)
				.with_thread_ids(false)
				.with_thread_names(false)
				.with_span_events(FmtSpan::NONE)
				.with_writer(writer)
				.with_filter(filter.env())
				.with_filter(filter.span_filter::<S>())
				.boxed()),
		}
	}
	// Configure the log tracer for development
	#[cfg(debug_assertions)]
	{
		let layer = tracing_subscriber::fmt::layer();
		// Configure the log console writer
		match format {
			LogFormat::Json => Ok(layer
				.json()
				.with_ansi(true)
				.with_file(true)
				.with_target(true)
				.with_line_number(true)
				.with_thread_ids(false)
				.with_thread_names(false)
				.with_span_events(FmtSpan::NONE)
				.with_writer(writer)
				.with_filter(filter.env())
				.with_filter(filter.span_filter::<S>())
				.boxed()),
			LogFormat::Text => Ok(layer
				.compact()
				.with_ansi(true)
				.with_file(true)
				.with_target(true)
				.with_line_number(true)
				.with_thread_ids(false)
				.with_thread_names(false)
				.with_span_events(FmtSpan::NONE)
				.with_writer(writer)
				.with_filter(filter.env())
				.with_filter(filter.span_filter::<S>())
				.boxed()),
		}
	}
}
