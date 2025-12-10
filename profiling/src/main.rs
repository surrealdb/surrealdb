use std::path::PathBuf;

use chrono::Utc;
use clap::Parser;
use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use surrealdb::opt::Config;
use surrealdb::types::SurrealValue;
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "surrealdb-benchmark")]
#[command(about = "SurrealDB benchmark tool with tracing support")]
struct Args {
	/// Path to write Perfetto trace file (only used with --perfetto)
	#[arg(long, default_value = "./traces")]
	trace_root: PathBuf,

	#[arg(long, default_value = "trace")]
	name: String,
}

#[derive(Debug, SurrealValue)]
struct Person {
	name: String,
	age: u64,
}

#[tokio::main]
async fn main() {
	let args = Args::parse();

	let config = Config::new();

	let db = Surreal::new::<Mem>(config).await.unwrap();

	db.use_ns("test").use_db("test").await.unwrap();

	db.query(
		r#"
			FOR $i IN 0..100 {
				CREATE person CONTENT { id: $i, name: 'Stu', age: $i }
			}
    	"#,
	)
	.await
	.unwrap();

	// Initialize tracing subscriber based on arguments
	let trace_path = args.trace_root.canonicalize().unwrap();
	let perfetto_path = trace_path.join(format!("{}_{}.pftrace", args.name, Utc::now().format("%Y%m%d%H%M%S")));

	let file = std::fs::File::create(&perfetto_path).expect("Failed to create perfetto trace file");
	let perfetto_layer = PerfettoLayer::new(std::sync::Mutex::new(file));

	tracing_subscriber::registry()
		.with(perfetto_layer)
		.with(tracing_subscriber::filter::LevelFilter::TRACE)
		.init();

	println!("Perfetto tracing enabled, writing to: {}", perfetto_path.display());

	let mut results = db.query("SELECT * FROM person WHERE age > 50").await.unwrap();

	let results: Vec<Person> = results.take(0).unwrap();
	assert_eq!(results.len(), 100);
}
