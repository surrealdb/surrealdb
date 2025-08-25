use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main};
use flate2::read::GzDecoder;
use reblessive::TreeStack;
use surrealdb_core::catalog::{Distance, HnswParams, VectorType};
use surrealdb_core::dbs::Session;
use surrealdb_core::idx::IndexKeyBase;
use surrealdb_core::idx::planner::checker::{HnswChecker, HnswConditionChecker};
use surrealdb_core::idx::trees::hnsw::index::HnswIndex;
use surrealdb_core::kvs::LockType::Optimistic;
use surrealdb_core::kvs::TransactionType::{Read, Write};
use surrealdb_core::kvs::{Datastore, Transaction};
use surrealdb_core::syn;
use surrealdb_core::val::{Array, Number, RecordId, RecordIdKey, Value};
use tokio::runtime::{Builder, Runtime};

const EF_CONSTRUCTION: u16 = 150;
const EF_SEARCH: usize = 80;

const NN: usize = 10;
const M: u8 = 24;
const M0: u8 = 48;

const DIMENSION: u16 = 20;

const INGESTING_SOURCE: &str = "../../tests/data/hnsw-random-9000-20-euclidean.gz";
const QUERYING_SOURCE: &str = "../../tests/data/hnsw-random-5000-20-euclidean.gz";

fn bench_hnsw_no_db(c: &mut Criterion) {
	const GROUP_NAME: &str = "hnsw_no_db";

	let samples = new_vectors_from_file(INGESTING_SOURCE);
	let samples: Vec<(RecordId, Vec<Value>)> =
		samples.into_iter().map(|(r, a)| (r, vec![Value::Array(a)])).collect();

	// Indexing benchmark group
	{
		let mut group = get_group(c, GROUP_NAME, samples.len(), 10);
		let id = format!("insert len: {}", samples.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap()).iter(|| insert_objects(&samples));
		});
		group.finish();
	}

	// Create an HNSW instance with data
	let b = Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
	let (ds, hnsw) = b.block_on(insert_objects(&samples));

	let samples = new_vectors_from_file(QUERYING_SOURCE);
	let samples: Vec<Vec<Number>> =
		samples.into_iter().map(|(_, a)| convert_array_to_vec_number(a)).collect();

	// Knn lookup benchmark group
	{
		let mut group = get_group(c, GROUP_NAME, samples.len(), 10);
		let id = format!("lookup len: {}", samples.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap()).iter(|| knn_lookup_objects(&ds, &hnsw, &samples));
		});
		group.finish();
	}
}

fn bench_hnsw_with_db(c: &mut Criterion) {
	const GROUP_NAME: &str = "hnsw_with_db";

	let samples = new_vectors_from_file(INGESTING_SOURCE);
	let samples: Vec<String> =
		samples.into_iter().map(|(r, a)| format!("CREATE {r} SET r={a} RETURN NONE;")).collect();

	let session = &Session::owner().with_ns("ns").with_db("db");

	// Indexing benchmark group
	{
		let mut group = get_group(c, GROUP_NAME, samples.len(), 10);
		let id = format!("insert len: {}", samples.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap()).iter(|| insert_objects_db(session, true, &samples));
		});
		group.finish();
	}

	let b = Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
	let ds = b.block_on(insert_objects_db(session, true, &samples));

	// Knn lookup benchmark group
	let samples = new_vectors_from_file(QUERYING_SOURCE);
	let selects: Vec<String> = samples
		.into_iter()
		.map(|(_, a)| format!("SELECT id FROM e WHERE r <|{NN},{EF_SEARCH}|> {a};"))
		.collect();
	{
		let mut group = get_group(c, GROUP_NAME, selects.len(), 10);
		let id = format!("lookup len: {}", selects.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap())
				.iter(|| knn_lookup_objects_db(&ds, session, &selects));
		});
		group.finish();
	}
}

fn bench_db_without_index(c: &mut Criterion) {
	const GROUP_NAME: &str = "hnsw_without_index";

	let samples = new_vectors_from_file(INGESTING_SOURCE);
	let samples: Vec<String> =
		samples.into_iter().map(|(r, a)| format!("CREATE {r} SET r={a} RETURN NONE;")).collect();

	let session = &Session::owner().with_ns("ns").with_db("db");

	// Ingesting benchmark group
	{
		let mut group = get_group(c, GROUP_NAME, samples.len(), 10);
		let id = format!("insert len: {}", samples.len());

		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap())
				.iter(|| insert_objects_db(session, false, &samples));
		});
		group.finish();
	}

	let b = Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
	let ds = b.block_on(insert_objects_db(session, false, &samples));

	// Knn lookup benchmark group
	let samples = new_vectors_from_file(QUERYING_SOURCE);
	let selects: Vec<String> = samples
		.into_iter()
		.map(|(id, _)| format!("SELECT id FROM {id},{id},{id},{id},{id},{id},{id},{id},{id},{id};"))
		.collect();
	{
		let mut group = get_group(c, GROUP_NAME, selects.len(), 10);
		let id = format!("lookup len: {}", selects.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap())
				.iter(|| knn_lookup_objects_db(&ds, session, &selects));
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
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(measurement_secs));
	group
}

fn new_vectors_from_file(path: &str) -> Vec<(RecordId, Array)> {
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
		let value = syn::value(&line).unwrap();
		if let Value::Array(a) = value {
			res.push((RecordId::new("e".to_owned(), RecordIdKey::from(i as i64)), a));
		} else {
			panic!("Wrong value");
		}
	}
	res
}

fn convert_array_to_vec_number(a: Array) -> Vec<Number> {
	a.into_iter()
		.map(|v| {
			if let Value::Number(n) = v {
				n
			} else {
				panic!("Wrong value {}", v);
			}
		})
		.collect()
}

async fn init_datastore(session: &Session, with_index: bool) -> Datastore {
	let ds = Datastore::new("memory").await.unwrap();
	if with_index {
		let sql = format!(
			"DEFINE INDEX ix ON e FIELDS r HNSW DIMENSION {DIMENSION} DIST EUCLIDEAN TYPE F32 EFC {EF_CONSTRUCTION} M {M};"
		);
		ds.execute(&sql, session, None).await.expect(&sql);
	}
	ds
}

async fn hnsw(tx: &Transaction) -> HnswIndex {
	let p = HnswParams {
		dimension: DIMENSION,
		distance: Distance::Euclidean,
		vector_type: VectorType::F32,
		m: M,
		m0: M0,
		ml: (1.0 / (M as f64).ln()).into(),
		ef_construction: EF_CONSTRUCTION,
		extend_candidates: false,
		keep_pruned_connections: false,
	};
	HnswIndex::new(tx, IndexKeyBase::new(0, 0, "test", "test"), "test".to_string(), &p)
		.await
		.unwrap()
}

async fn insert_objects(samples: &[(RecordId, Vec<Value>)]) -> (Datastore, HnswIndex) {
	let ds = Datastore::new("memory").await.unwrap();
	let tx = ds.transaction(Write, Optimistic).await.unwrap();
	let mut h = hnsw(&tx).await;
	for (thg, content) in samples {
		h.index_document(&tx, &thg.key, content).await.unwrap();
	}
	tx.commit().await.unwrap();
	(ds, h)
}

async fn insert_objects_db(session: &Session, create_index: bool, inserts: &[String]) -> Datastore {
	let ds = init_datastore(session, create_index).await;
	for sql in inserts {
		ds.execute(sql, session, None).await.expect(sql);
	}
	ds
}

async fn knn_lookup_objects(ds: &Datastore, h: &HnswIndex, samples: &[Vec<Number>]) {
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let db = tx.ensure_ns_db("ns", "db", false).await.unwrap();
	drop(tx);
	let mut stack = TreeStack::new();
	stack
		.enter(|stk| async {
			let tx = ds.transaction(Read, Optimistic).await.unwrap();
			for v in samples {
				let r = h
					.knn_search(
						&db,
						&tx,
						stk,
						v,
						NN,
						EF_SEARCH,
						HnswConditionChecker::Hnsw(HnswChecker {}),
					)
					.await
					.unwrap();
				assert_eq!(r.len(), NN);
			}
		})
		.finish()
		.await;
}

async fn knn_lookup_objects_db(ds: &Datastore, session: &Session, selects: &[String]) {
	for sql in selects {
		let mut res = ds.execute(sql, session, None).await.expect(sql);
		let res = res.remove(0).result.expect(sql);
		if let Value::Array(a) = &res {
			assert_eq!(a.len(), NN);
		} else {
			panic!("{res:#}");
		}
	}
}

criterion_group!(benches, bench_hnsw_no_db, bench_hnsw_with_db, bench_db_without_index);
criterion_main!(benches);
