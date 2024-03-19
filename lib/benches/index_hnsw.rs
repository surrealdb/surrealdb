use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use flate2::read::GzDecoder;
use futures::executor::block_on;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Duration;
use surrealdb::idx::trees::hnsw::HnswIndex;
use surrealdb::sql::index::Distance;
use surrealdb_core2::dbs::Session;
use surrealdb_core2::err::Error;
use surrealdb_core2::kvs::Datastore;
use surrealdb_core2::sql::index::{HnswParams, VectorType};
use surrealdb_core2::sql::{Array, Id, Thing, Value};
use surrealdb_core2::syn::value;
use tokio::runtime::{Builder, Runtime};
use tracing::info;

const EF_CONSTRUCTION: u16 = 500;
const EF_SEARCH: usize = 80;

const NN: usize = 10;
const M: u16 = 24;
const M0: u16 = 48;

const DIMENSION: u16 = 20;

fn bench_hnsw_no_db(c: &mut Criterion) {
	let samples = new_vectors_from_file("../tests/data/hnsw-random-9000-20-euclidean.gz").unwrap();
	let samples: Vec<(Thing, Vec<Value>)> =
		samples.into_iter().map(|(r, a)| (r, vec![Value::Array(a)])).collect();

	// Indexing benchmark group
	{
		let mut group = get_group(c, "hnsw_no_db", samples.len(), 10);
		let id = format!("insert len: {}", samples.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap()).iter(|| insert_objects(&samples));
		});
		group.finish();
	}

	// Create an HNSW instance with data
	let hnsw = block_on(insert_objects(&samples));

	let samples = new_vectors_from_file("../tests/data/hnsw-random-5000-20-euclidean.gz").unwrap();
	let samples: Vec<Array> = samples.into_iter().map(|(_, a)| a).collect();

	// Knn lookup benchmark group
	{
		let mut group = get_group(c, "hnsw_no_db", samples.len(), 10);
		let id = format!("lookup len: {}", samples.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap()).iter(|| knn_lookup_objects(&hnsw, &samples));
		});
		group.finish();
	}
}

fn bench_hnsw_with_db(c: &mut Criterion) {
	let samples = new_vectors_from_file("../tests/data/hnsw-random-9000-20-euclidean.gz").unwrap();
	let samples: Vec<String> =
		samples.into_iter().map(|(r, a)| format!("CREATE {r} SET r={a} RETURN NONE;")).collect();

	let session = &Session::owner().with_ns("ns").with_db("db");

	// Indexing benchmark group
	// {
	// 	let mut group = get_group(c, "hnsw_with_db", samples.len(), 10);
	// 	let id = format!("insert len: {}", samples.len());
	//
	// 	group.bench_function(id, |b| {
	// 		b.to_async(Runtime::new().unwrap()).iter(|| insert_objects_db(session, true, &samples));
	// 	});
	// 	group.finish();
	// }

	let b = Builder::new_multi_thread().worker_threads(1).enable_all().build().unwrap();
	let ds = b.block_on(insert_objects_db(session, true, &samples));

	// Knn lookup benchmark group
	let samples = new_vectors_from_file("../tests/data/hnsw-random-5000-20-euclidean.gz").unwrap();
	let selects: Vec<String> = samples
		.into_iter()
		.map(|(_, a)| format!("SELECT id FROM e WHERE r <{NN},{EF_SEARCH}> {a};"))
		.collect();
	{
		let mut group = get_group(c, "hnsw_with_db", selects.len(), 10);
		let id = format!("lookup len: {}", selects.len());
		group.bench_function(id, |b| {
			b.to_async(Runtime::new().unwrap())
				.iter(|| knn_lookup_objects_db(&ds, session, &selects));
		});
		group.finish();
	}
}

fn bench_db_without_index(c: &mut Criterion) {
	info!("Build data collection");
	let samples = new_vectors_from_file("../tests/data/hnsw-random-9000-20-euclidean.gz").unwrap();
	let samples: Vec<String> =
		samples.into_iter().map(|(r, a)| format!("CREATE {r} SET r={a} RETURN NONE;")).collect();

	let session = &Session::owner().with_ns("ns").with_db("db");

	// Ingesting benchmark group
	{
		let mut group = get_group(c, "hnsw_without_index", samples.len(), 10);
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
	let samples = new_vectors_from_file("../tests/data/hnsw-random-5000-20-euclidean.gz").unwrap();
	let selects: Vec<String> = samples
		.into_iter()
		.map(|(id, _)| format!("SELECT id FROM {id},{id},{id},{id},{id},{id},{id},{id},{id},{id};"))
		.collect();
	{
		let mut group = get_group(c, "hnsw_without_index", selects.len(), 10);
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

fn new_vectors_from_file(path: &str) -> Result<Vec<(Thing, Array)>, Error> {
	// Open the gzip file
	let file = File::open(path)?;

	// Create a GzDecoder to read the file
	let gz = GzDecoder::new(file);

	// Wrap the decoder in a BufReader
	let reader = BufReader::new(gz);

	let mut res = Vec::new();
	// Iterate over each line in the file
	for (i, line_result) in reader.lines().enumerate() {
		let line = line_result?;
		let value = value(&line).unwrap();
		if let Value::Array(a) = value {
			res.push((Thing::from(("e", Id::Number(i as i64))), a));
		} else {
			panic!("Wrong value");
		}
	}
	Ok(res)
}
async fn init_datastore(session: &Session, with_index: bool) -> Datastore {
	let ds = Datastore::new("memory").await.unwrap();
	if with_index {
		let sql = format!("DEFINE INDEX ix ON e FIELDS r HNSW DIMENSION {DIMENSION} DIST EUCLIDEAN TYPE F32 EFC {EF_CONSTRUCTION} M {M};");
		ds.execute(&sql, session, None).await.expect(&sql);
	}
	ds
}

fn hnsw() -> HnswIndex {
	HnswIndex::new(&HnswParams {
		dimension: DIMENSION,
		distance: Distance::Euclidean,
		vector_type: VectorType::F32,
		m: M,
		m0: M0,
		ef_construction: EF_CONSTRUCTION,
		heuristic: false,
		extend_candidates: false,
		keep_pruned_connections: false,
		ml: (1.0 / (M as f64).ln()).into(),
	})
}

async fn insert_objects(samples: &[(Thing, Vec<Value>)]) -> HnswIndex {
	let mut h = hnsw();
	for (id, content) in samples {
		h.index_document(&id, content).await.unwrap();
	}
	h
}

async fn insert_objects_db(session: &Session, create_index: bool, inserts: &[String]) -> Datastore {
	let ds = init_datastore(session, create_index).await;
	for sql in inserts {
		ds.execute(sql, session, None).await.expect(&sql);
	}
	ds
}

async fn knn_lookup_objects(h: &HnswIndex, samples: &[Array]) {
	for a in samples {
		let r = h.knn_search(a, NN, EF_SEARCH).await.unwrap();
		assert_eq!(r.len(), NN);
	}
}

async fn knn_lookup_objects_db(ds: &Datastore, session: &Session, selects: &[String]) {
	for sql in selects {
		let mut res = ds.execute(sql, session, None).await.expect(&sql);
		let res = res.remove(0).result.expect(&sql);
		if let Value::Array(a) = &res {
			assert_eq!(a.len(), NN);
		} else {
			panic!("{res:#}");
		}
	}
}

// criterion_group!(benches, bench_hnsw_no_db, bench_hnsw_with_db, bench_db_without_index);
criterion_group!(benches, bench_hnsw_with_db);
criterion_main!(benches);
