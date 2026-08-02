#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

//! Measures how INSERT (`CREATE`) latency scales with the number of active
//! live-query subscribers on the written table.
//!
//! This is the headline benchmark for the live-query work: today every committed
//! write does its per-subscriber matching/permission/projection on the mutator's
//! transaction path (`Document::process_table_lives`), so a single `CREATE` costs
//! `O(subscribers)`. This benchmark quantifies that by registering N live queries
//! and then timing `CREATE`s for N in {0, 1, 10, 100, 1000}; the per-subscriber
//! cost shows up as the gap above the N=0 baseline.
//!
//! Both engine modes are benchmarked:
//! - `inline` — the default engine (per-subscriber work on the write path).
//! - `router` — the inverted engine. NOTE: until the inline notification path is removed from the
//!   write path (a later phase), `router` still runs the inline matching *plus* the O(1) live-event
//!   capture, so today it tracks `inline` (slightly above it), not flat. Both series are kept so
//!   this same harness demonstrates the inversion — `router` flattening — once that lands.
//!
//! The datastore is built with a notification channel (drained in the
//! background); without one the broker is absent and `process_table_lives`
//! short-circuits, so the per-subscriber cost would not be exercised.
//!
//! Run (matching CI's feature set):
//! ```bash
//! cargo bench --package surrealdb --no-default-features \
//!   --features kv-mem,scripting,http,jwks --bench live_query_scaling
//! ```
//! Env overrides: `BENCH_LQ_SAMPLE_SIZE` (default 10),
//! `BENCH_LQ_MEASUREMENT_SECS` (default 10).

use std::hint::black_box;
use std::sync::LazyLock;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use surrealdb_core::cnf::ConfigMap;
use surrealdb_core::dbs::{Capabilities, Session};
use surrealdb_core::kvs::Datastore;
use surrealdb_types::Notification;
use tokio::runtime::{Builder, Runtime};

/// Subscriber counts to benchmark.
const SUBSCRIBER_COUNTS: &[usize] = &[0, 1, 10, 100, 1000];
/// Engine modes to benchmark.
const ENGINES: &[&str] = &["inline", "router"];

const NS: &str = "bench";
const DB: &str = "bench";
const TB: &str = "person";

static SAMPLE_SIZE: LazyLock<usize> = LazyLock::new(|| env("BENCH_LQ_SAMPLE_SIZE", 10));
static MEASUREMENT_SECS: LazyLock<u64> = LazyLock::new(|| env("BENCH_LQ_MEASUREMENT_SECS", 10));

fn env<T: std::str::FromStr>(key: &str, default: T) -> T {
	std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// A prepared datastore with `subscribers` live queries registered on `TB`.
struct Prepared {
	ds: Datastore,
	session: Session,
}

/// Build a fresh in-memory datastore for the given engine, register `subscribers`
/// `LIVE SELECT`s on the table, and start a background task draining the
/// notification channel so it never applies backpressure.
async fn prepare(engine: &str, subscribers: usize, runtime: &Runtime) -> Prepared {
	let (send, recv) = surrealdb_core::channel::unbounded::<Notification>();
	let config = ConfigMap::empty().with_key_value("live_query_engine", engine);
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_auth(false)
		.with_notify(send)
		.with_config(config)
		.build_with_path("memory")
		.await
		.unwrap();
	// Discard delivered notifications; the post-commit flush is not what we time.
	runtime.spawn(async move { while recv.recv().await.is_ok() {} });
	// Set up namespace, database, and table.
	ds.execute(&format!("DEFINE NAMESPACE {NS}"), &Session::owner(), None).await.unwrap();
	ds.execute(&format!("DEFINE DATABASE {DB}"), &Session::owner().with_ns(NS), None)
		.await
		.unwrap();
	let session = Session::owner().with_ns(NS).with_db(DB);
	ds.execute(&format!("DEFINE TABLE {TB}"), &session, None).await.unwrap();
	// Register the live-query subscribers (realtime session required for LIVE).
	let live = Session::owner().with_ns(NS).with_db(DB).with_rt(true);
	for _ in 0..subscribers {
		ds.execute(&format!("LIVE SELECT * FROM {TB}"), &live, None).await.unwrap();
	}
	Prepared {
		ds,
		session,
	}
}

/// Time a single `CREATE` (auto-generated id), which fans out to every registered
/// subscriber on the write path.
async fn create_one(p: &Prepared) {
	let res =
		p.ds.execute(&format!("CREATE {TB} SET n = 1"), &p.session, None)
			.await
			.expect("create should execute");
	black_box(res);
}

fn bench_live_query_scaling(c: &mut Criterion) {
	let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
	for &engine in ENGINES {
		let mut group = c.benchmark_group(format!("live_query_insert_scaling/{engine}"));
		group.throughput(Throughput::Elements(1));
		group.sample_size(*SAMPLE_SIZE);
		group.measurement_time(Duration::from_secs(*MEASUREMENT_SECS));
		for &subscribers in SUBSCRIBER_COUNTS {
			let prepared = runtime.block_on(prepare(engine, subscribers, &runtime));
			group.bench_with_input(
				BenchmarkId::from_parameter(subscribers),
				&subscribers,
				|b, _| {
					b.to_async(&runtime).iter(|| create_one(&prepared));
				},
			);
			runtime.block_on(async { drop(prepared) });
		}
		group.finish();
	}
}

criterion_group!(benches, bench_live_query_scaling);
criterion_main!(benches);
