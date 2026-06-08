#![allow(clippy::unwrap_used)]
#![recursion_limit = "256"]

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::os::raw::c_int;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::measurement::WallTime;
use criterion::profiler::Profiler;
use criterion::{BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main};
use flate2::read::GzDecoder;
use pprof::ProfilerGuard;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{RecordId, RecordIdKey, ToSql, Value};
use temp_dir::TempDir;
use tokio::runtime::{Builder, Runtime};
use tokio_util::sync::CancellationToken;

const EF_CONSTRUCTION: u16 = 150;
const EF_SEARCH: usize = 80;

const NN: usize = 10;
const M: u8 = 24;

const DIMENSION: u16 = 20;

const NS: &str = "ns";
const DB: &str = "db";

const INGESTING_SOURCE: &str =
	concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/hnsw-random-9000-20-euclidean.gz");
const QUERYING_SOURCE: &str =
	concat!(env!("CARGO_MANIFEST_DIR"), "/../tests/data/hnsw-random-5000-20-euclidean.gz");
const SAMPLE_SIZE_ENV: &str = "SURREALDB_ANN_BENCH_SAMPLE_SIZE";
const MEASUREMENT_SECS_ENV: &str = "SURREALDB_ANN_BENCH_MEASUREMENT_SECS";

/// Criterion profiler that writes a `flamegraph.svg` beside profiled benchmark output.
struct PprofFlamegraphProfiler {
	frequency: c_int,
	active_profiler: Option<ProfilerGuard<'static>>,
}

impl PprofFlamegraphProfiler {
	fn new(frequency: c_int) -> Self {
		Self {
			frequency,
			active_profiler: None,
		}
	}
}

impl Profiler for PprofFlamegraphProfiler {
	fn start_profiling(&mut self, _: &str, _: &Path) {
		self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
	}

	fn stop_profiling(&mut self, _: &str, benchmark_dir: &Path) {
		std::fs::create_dir_all(benchmark_dir).unwrap();

		let output_path = benchmark_dir.join("flamegraph.svg");
		let output_file = File::create(&output_path)
			.unwrap_or_else(|_| panic!("failed to create {}", output_path.display()));

		if let Some(profiler) = self.active_profiler.take() {
			profiler.report().build().unwrap().flamegraph(output_file).unwrap();
		}
	}
}

/// ANN index implementation under benchmark.
#[derive(Clone, Copy)]
enum AnnIndex {
	Hnsw,
	DiskAnn,
}

/// Datastore backend under benchmark.
#[derive(Clone, Copy)]
enum BenchStore {
	Memory,
	#[cfg(feature = "kv-rocksdb")]
	RocksDb,
}

/// Datastore handle plus any temporary storage that must live for the benchmark duration.
struct BenchDb {
	ds: Arc<Datastore>,
	_temp_dir: Option<TempDir>,
}

impl AnnIndex {
	fn group_name(self, store: BenchStore) -> String {
		let group = match (self, store) {
			(Self::Hnsw, BenchStore::Memory) => "hnsw_with_db",
			(Self::DiskAnn, BenchStore::Memory) => "diskann_with_db",
			#[cfg(feature = "kv-rocksdb")]
			(Self::Hnsw, BenchStore::RocksDb) => "hnsw_with_rocksdb",
			#[cfg(feature = "kv-rocksdb")]
			(Self::DiskAnn, BenchStore::RocksDb) => "diskann_with_rocksdb",
		};
		group.to_owned()
	}

	fn define_index_sql(self) -> String {
		match self {
			Self::Hnsw => {
				format!(
					"DEFINE INDEX ix ON e FIELDS r HNSW DIMENSION {DIMENSION} DIST EUCLIDEAN TYPE F32 EFC {EF_CONSTRUCTION} M {M};"
				)
			}
			Self::DiskAnn => {
				format!(
					"DEFINE INDEX ix ON e FIELDS r DISKANN DIMENSION {DIMENSION} DIST EUCLIDEAN TYPE F32 DEGREE {M} L_BUILD {EF_CONSTRUCTION};"
				)
			}
		}
	}
}

impl BenchStore {
	fn endpoint(self, temp_dir: Option<&TempDir>) -> String {
		match self {
			Self::Memory => "memory".to_owned(),
			#[cfg(feature = "kv-rocksdb")]
			Self::RocksDb => {
				let path = temp_dir.expect("RocksDB benchmark requires a temp dir").child("db");
				format!("rocksdb:{}", path.display())
			}
		}
	}
}

impl BenchDb {
	fn datastore(&self) -> &Datastore {
		self.ds.as_ref()
	}
}

fn bench_hnsw_with_db(c: &mut Criterion) {
	bench_ann_with_db(c, AnnIndex::Hnsw, BenchStore::Memory);
	#[cfg(feature = "kv-rocksdb")]
	bench_ann_with_db(c, AnnIndex::Hnsw, BenchStore::RocksDb);
}

fn bench_diskann_with_db(c: &mut Criterion) {
	bench_ann_with_db(c, AnnIndex::DiskAnn, BenchStore::Memory);
	#[cfg(feature = "kv-rocksdb")]
	bench_ann_with_db(c, AnnIndex::DiskAnn, BenchStore::RocksDb);
}

fn bench_ann_with_db(c: &mut Criterion, index: AnnIndex, store: BenchStore) {
	let group_name = index.group_name(store);
	let samples = new_vectors_from_file(INGESTING_SOURCE);
	let samples: Vec<String> = samples
		.into_iter()
		.map(|(r, a)| format!("CREATE {} SET r={a} RETURN NONE;", r.to_sql()))
		.collect();

	let session = &Session::owner().with_ns(NS).with_db(DB);

	// Indexing benchmark group
	{
		let mut group = get_group(c, &group_name, samples.len(), 10);
		let id = format!("insert len: {}", samples.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap())
				.iter(|| insert_objects_db(session, Some(index), store, &samples));
		});
		group.finish();
	}

	// Pending compaction benchmark group
	{
		let mut group = get_group(c, &group_name, samples.len(), 10);
		let id = format!("compact pending len: {}", samples.len());
		group.bench_function(id, |b| {
			b.iter_custom(|iters| {
				let rt =
					Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
				let mut total = Duration::ZERO;
				for _ in 0..iters {
					let ds = rt.block_on(insert_objects_db(session, Some(index), store, &samples));
					let start = Instant::now();
					rt.block_on(compact_index_db(&ds));
					total += start.elapsed();
				}
				total
			});
		});
		group.finish();
	}

	let b = Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
	let ds = Arc::new(b.block_on(insert_objects_db(session, Some(index), store, &samples)));
	b.block_on(compact_index_db(ds.as_ref()));

	// Knn lookup benchmark group
	let samples = new_vectors_from_file(QUERYING_SOURCE);
	let selects: Vec<String> = samples
		.into_iter()
		.map(|(_, a)| format!("SELECT id FROM e WHERE r <|{NN},{EF_SEARCH}|> {a};"))
		.collect();
	{
		let mut group = get_group(c, &group_name, selects.len(), 10);
		let id = format!("lookup len: {}", selects.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap())
				.iter(|| knn_lookup_objects_db(ds.datastore(), session, &selects));
		});
		group.finish();
	}
}

fn bench_db_without_index(c: &mut Criterion) {
	const GROUP_NAME: &str = "ann_without_index";

	let samples = new_vectors_from_file(INGESTING_SOURCE);
	let samples: Vec<String> = samples
		.into_iter()
		.map(|(r, a)| format!("CREATE {} SET r={a} RETURN NONE;", r.to_sql()))
		.collect();

	let session = &Session::owner().with_ns(NS).with_db(DB);

	// Ingesting benchmark group
	{
		let mut group = get_group(c, GROUP_NAME, samples.len(), 10);
		let id = format!("insert len: {}", samples.len());

		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap())
				.iter(|| insert_objects_db(session, None, BenchStore::Memory, &samples));
		});
		group.finish();
	}

	let b = Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
	let ds = b.block_on(insert_objects_db(session, None, BenchStore::Memory, &samples));

	// Knn lookup benchmark group
	let samples = new_vectors_from_file(QUERYING_SOURCE);
	let selects: Vec<String> = samples
		.into_iter()
		.map(|(id, _)| {
			format!(
				"SELECT id FROM {id},{id},{id},{id},{id},{id},{id},{id},{id},{id};",
				id = id.to_sql()
			)
		})
		.collect();
	{
		let mut group = get_group(c, GROUP_NAME, selects.len(), 10);
		let id = format!("lookup len: {}", selects.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap())
				.iter(|| knn_lookup_objects_db(ds.datastore(), session, &selects));
		});
		group.finish();
	}
}

fn get_group<'a>(
	c: &'a mut Criterion,
	group_name: &str,
	samples_len: usize,
	measurement_secs: u64,
) -> BenchmarkGroup<'a, WallTime> {
	let mut group = c.benchmark_group(group_name);
	group.throughput(Throughput::Elements(samples_len as u64));
	let sample_size =
		std::env::var(SAMPLE_SIZE_ENV).ok().and_then(|v| v.parse().ok()).unwrap_or(10);
	let measurement_secs = std::env::var(MEASUREMENT_SECS_ENV)
		.ok()
		.and_then(|v| v.parse().ok())
		.unwrap_or(measurement_secs);
	group.sample_size(sample_size.max(10));
	group.measurement_time(Duration::from_secs(measurement_secs));
	group
}

fn new_vectors_from_file(path: &str) -> Vec<(RecordId, String)> {
	// Open the gzip file
	let file = File::open(path).unwrap();

	// Create a GzDecoder to read the file
	let gz = GzDecoder::new(file);

	// Wrap the decoder in a BufReader
	let reader = BufReader::new(gz);

	let mut res = Vec::new();
	// Iterate over each line in the file
	for (i, line_result) in reader.lines().enumerate() {
		let line = line_result.unwrap();
		res.push((RecordId::new("e".to_owned(), RecordIdKey::from(i as i64)), line));
	}
	res
}

/// Creates an isolated datastore and optionally defines the ANN index under test.
async fn init_datastore(session: &Session, index: Option<AnnIndex>, store: BenchStore) -> BenchDb {
	let temp_dir = match store {
		BenchStore::Memory => None,
		#[cfg(feature = "kv-rocksdb")]
		BenchStore::RocksDb => Some(TempDir::new().unwrap()),
	};
	let endpoint = store.endpoint(temp_dir.as_ref());
	let ds = Arc::new(Datastore::new(&endpoint).await.unwrap());
	execute_ok(ds.as_ref(), &Session::owner(), &format!("DEFINE NAMESPACE {NS};")).await;
	execute_ok(ds.as_ref(), &Session::owner().with_ns(NS), &format!("DEFINE DATABASE {DB};")).await;
	if let Some(index) = index {
		let sql = index.define_index_sql();
		execute_ok(ds.as_ref(), session, &sql).await;
	}
	BenchDb {
		ds,
		_temp_dir: temp_dir,
	}
}

/// Creates a datastore and inserts all benchmark records.
async fn insert_objects_db(
	session: &Session,
	index: Option<AnnIndex>,
	store: BenchStore,
	inserts: &[String],
) -> BenchDb {
	let ds = init_datastore(session, index, store).await;
	for sql in inserts {
		execute_ok(ds.datastore(), session, sql).await;
	}
	ds
}

/// Runs one background index-compaction pass and asserts that it performed work.
async fn compact_index_db(ds: &BenchDb) {
	let (iterations, errors) = Datastore::index_compaction(
		Arc::clone(&ds.ds),
		Duration::from_secs(1),
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(errors, 0);
	assert!(iterations > 0);
}

/// Executes setup or mutation SQL and panics if any statement fails.
async fn execute_ok(ds: &Datastore, session: &Session, sql: &str) {
	for res in ds.execute(sql, session, None).await.expect(sql) {
		res.result.expect(sql);
	}
}

async fn knn_lookup_objects_db(ds: &Datastore, session: &Session, selects: &[String]) {
	for sql in selects {
		let mut res = ds.execute(sql, session, None).await.expect(sql);
		let res = res.remove(0).result.expect(sql);
		if let Value::Array(a) = &res {
			assert_eq!(a.len(), NN);
		} else {
			panic!("{res:#?}");
		}
	}
}

criterion_group! {
	name = benches;
	config = Criterion::default().with_profiler(PprofFlamegraphProfiler::new(100));
	targets = bench_hnsw_with_db, bench_diskann_with_db, bench_db_without_index
}
criterion_main!(benches);
