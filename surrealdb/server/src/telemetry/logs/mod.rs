pub mod socket;

use std::io::IsTerminal;

use anyhow::Result;
use tracing::{Level, Subscriber};
use tracing_appender::non_blocking::NonBlocking;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::LevelFilter;
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

/// Build separate stdout and stderr logging layers with per-stream ANSI
/// detection. Each stream independently checks whether it is a TTY and
/// respects the `NO_COLOR` environment variable (https://no-color.org).
/// JSON output never emits ANSI escape codes regardless of TTY state.
pub fn output<S>(
	filter: CustomFilter,
	stdout: NonBlocking,
	stderr: NonBlocking,
	format: LogFormat,
) -> Result<Vec<Box<dyn Layer<S> + Send + Sync>>>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	let no_color = std::env::var_os("NO_COLOR").is_some();
	let ansi_stdout =
		!no_color && std::io::stdout().is_terminal() && matches!(format, LogFormat::Text);
	let ansi_stderr =
		!no_color && std::io::stderr().is_terminal() && matches!(format, LogFormat::Text);
	// Route WARN/ERROR to stderr, INFO/DEBUG/TRACE to stdout
	let stderr_writer = stderr.with_max_level(Level::WARN);
	let stdout_writer = stdout.with_min_level(Level::INFO);
	// Include source location in debug builds only
	#[cfg(not(debug_assertions))]
	let (show_file, show_line) = (false, false);
	#[cfg(debug_assertions)]
	let (show_file, show_line) = (true, true);
	// Build the stderr layer (WARN and ERROR)
	let stderr_filter = filter.clone();
	let stderr_layer = {
		let layer = tracing_subscriber::fmt::layer();
		match format {
			LogFormat::Json => layer
				.json()
				.with_ansi(ansi_stderr)
				.with_file(show_file)
				.with_target(true)
				.with_line_number(show_line)
				.with_thread_ids(false)
				.with_thread_names(false)
				.with_span_events(FmtSpan::NONE)
				.with_writer(stderr_writer)
				.with_filter(stderr_filter.env())
				.with_filter(stderr_filter.span_filter::<S>())
				.with_filter(LevelFilter::WARN)
				.boxed(),
			LogFormat::Text => layer
				.compact()
				.with_ansi(ansi_stderr)
				.with_file(show_file)
				.with_target(true)
				.with_line_number(show_line)
				.with_thread_ids(false)
				.with_thread_names(false)
				.with_span_events(FmtSpan::NONE)
				.with_writer(stderr_writer)
				.with_filter(stderr_filter.env())
				.with_filter(stderr_filter.span_filter::<S>())
				.with_filter(LevelFilter::WARN)
				.boxed(),
		}
	};
	// Build the stdout layer (INFO, DEBUG, and TRACE)
	let stdout_layer = {
		let layer = tracing_subscriber::fmt::layer();
		let level_filter =
			tracing_subscriber::filter::filter_fn(|meta| *meta.level() > Level::WARN);
		match format {
			LogFormat::Json => layer
				.json()
				.with_ansi(ansi_stdout)
				.with_file(show_file)
				.with_target(true)
				.with_line_number(show_line)
				.with_thread_ids(false)
				.with_thread_names(false)
				.with_span_events(FmtSpan::NONE)
				.with_writer(stdout_writer)
				.with_filter(filter.env())
				.with_filter(filter.span_filter::<S>())
				.with_filter(level_filter)
				.boxed(),
			LogFormat::Text => layer
				.compact()
				.with_ansi(ansi_stdout)
				.with_file(show_file)
				.with_target(true)
				.with_line_number(show_line)
				.with_thread_ids(false)
				.with_thread_names(false)
				.with_span_events(FmtSpan::NONE)
				.with_writer(stdout_writer)
				.with_filter(filter.env())
				.with_filter(filter.span_filter::<S>())
				.with_filter(level_filter)
				.boxed(),
		}
	};
	Ok(vec![stderr_layer, stdout_layer])
}
