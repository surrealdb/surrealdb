//! SurrealDB profiling tool.
//!
//! This is used to profile specific queries in isolation.

use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use surrealdb::opt::Config;
use surrealdb::types::SurrealValue;
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "surrealdb-profiler")]
#[command(about = "SurrealDB profiling tool with tracing support")]
struct Args {
	/// Path to write Perfetto trace files.
	#[arg(long, default_value = "./traces")]
	trace_root: PathBuf,

	/// Name to prefix the trace files.
	#[arg(long, default_value = "trace")]
	name: String,
}

/// A person record.
#[derive(Debug, SurrealValue)]
struct Person {
	/// The name of the person.
	name: String,
	/// The age of the person.
	age: u64,
}

/// Main function.
#[tokio::main]
async fn main() -> Result<()> {
	let args = Args::parse();

	let config = Config::new();

	let db = Surreal::new::<Mem>(config).await.context("Failed to connect to database")?;

	db.use_ns("test").use_db("test").await?;

	db.query(
		r#"
			FOR $i IN 0..100 {
				CREATE person CONTENT { id: $i, name: 'Stu', age: $i }
			}
    	"#,
	)
	.await?;

	// Initialize tracing subscriber based on arguments
	let trace_path = args.trace_root.canonicalize().context("Failed to canonicalize trace path")?;
	let perfetto_path =
		trace_path.join(format!("{}_{}.pftrace", args.name, Utc::now().format("%Y%m%d%H%M%S")));

	let file =
		std::fs::File::create(&perfetto_path).context("Failed to create perfetto trace file")?;
	let perfetto_layer =
		PerfettoLayer::new(std::sync::Mutex::new(file)).with_debug_annotations(true);

	tracing_subscriber::registry()
		.with(perfetto_layer)
		.with(tracing_subscriber::filter::LevelFilter::TRACE)
		.init();

	println!("Perfetto tracing enabled, writing to: {}", perfetto_path.display());

	let mut results = db.query("SELECT * FROM person").await.context("Failed to query database")?;

	let _: Vec<Person> = results.take(0).context("Failed to take result")?;

	Ok(())
}
