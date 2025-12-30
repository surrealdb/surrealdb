#![allow(clippy::unwrap_used)]

//! Benchmarks for full-text search performance with BM25 scoring.
//!
//! This benchmark measures:
//! - Full-text index creation performance
//! - BM25 scoring computation performance
//! - Query performance with different corpus sizes
//! - Hybrid search fusion (RRF and Linear) performance
//!
//! Run with RocksDB (set BENCH_DB_PATH to a real disk path):
//!   BENCH_DB_PATH=/path/to/disk cargo bench -p surrealdb --features kv-rocksdb -- bm25

use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use tokio::runtime::Runtime;

/// Get datastore path based on enabled features.
/// Uses RocksDB if kv-rocksdb feature is enabled and BENCH_DB_PATH env var is set.
fn get_datastore_path() -> String {
	#[cfg(feature = "kv-rocksdb")]
	{
		if let Ok(base_path) = std::env::var("BENCH_DB_PATH") {
			let ts =
				std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
			format!("rocksdb://{}/bench-ft-{}.db", base_path, ts)
		} else {
			"memory".to_string()
		}
	}
	#[cfg(not(feature = "kv-rocksdb"))]
	{
		"memory".to_string()
	}
}

// Sample documents for benchmarking - varied lengths and vocabulary
const DOCUMENTS: &[&str] = &[
	"Graph databases excel at relationship traversal and connected data queries.",
	"Relational databases store data in structured tables with SQL query support.",
	"Document stores provide flexible schema-less data storage for JSON documents.",
	"Key-value stores offer simple fast access patterns for caching and sessions.",
	"Time series databases optimize for temporal data and metrics collection.",
	"Search engines provide full-text indexing with relevance scoring algorithms.",
	"Column-family databases handle wide columns and sparse data efficiently.",
	"Vector databases enable similarity search for embeddings and ML applications.",
	"Multi-model databases combine multiple data models in a single platform.",
	"NewSQL databases provide SQL semantics with horizontal scalability features.",
	"Graph traversal algorithms find shortest paths and connected components quickly.",
	"Full-text search uses inverted indexes for fast term lookup and scoring.",
	"BM25 scoring algorithm balances term frequency and document length normalization.",
	"Distributed databases replicate data across nodes for fault tolerance.",
	"ACID transactions ensure data consistency in concurrent environments.",
	"Eventually consistent systems trade consistency for availability and performance.",
	"Sharding distributes data horizontally across multiple database nodes.",
	"Indexing strategies optimize query performance for specific access patterns.",
	"Query optimization analyzes execution plans to minimize resource usage.",
	"Database caching reduces latency by storing frequently accessed data in memory.",
];

// Longer documents for testing document length normalization
const LONG_DOCUMENTS: &[&str] = &[
	"Graph databases are specifically designed to store and navigate relationships between data points. Unlike relational databases that use tables and foreign keys, graph databases use nodes and edges to represent and store data. This makes them particularly efficient for use cases like social networks, recommendation engines, fraud detection, and knowledge graphs where understanding connections is crucial. The query language used by graph databases, such as Cypher or SPARQL, allows for intuitive expression of graph patterns.",
	"Full-text search engines implement sophisticated algorithms for relevance ranking. The BM25 algorithm, also known as Okapi BM25, is a probabilistic retrieval function that ranks documents based on query terms appearing in each document. It considers term frequency, inverse document frequency, and document length normalization. The algorithm has proven effective across various information retrieval tasks and remains the default in many search systems including Elasticsearch and Lucene.",
	"Modern database systems must balance multiple competing concerns including consistency, availability, partition tolerance, latency, and throughput. The CAP theorem states that distributed systems cannot simultaneously provide all three of consistency, availability, and partition tolerance. Different database designs make different tradeoffs based on their intended use cases. Understanding these tradeoffs is essential for choosing the right database for a specific application.",
];

struct BenchInput {
	dbs: Datastore,
	ses: Session,
	doc_count: usize,
}

async fn prepare_small_corpus() -> BenchInput {
	let dbs = Datastore::new(&get_datastore_path()).await.unwrap();
	let ses = Session::owner().with_ns("bench").with_db("bench");

	// Define analyzer and full-text index
	let sql = r"
        DEFINE ANALYZER bench_analyzer TOKENIZERS blank, class, punct FILTERS lowercase, ascii;
        DEFINE INDEX ft_idx ON TABLE doc FIELDS content FULLTEXT ANALYZER bench_analyzer BM25;
    ";
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	for _ in 0..2 {
		res.remove(0).result.unwrap();
	}

	// Insert documents
	let doc_count = if cfg!(debug_assertions) {
		100
	} else {
		1_000
	};

	for i in 0..doc_count {
		let content = DOCUMENTS[i % DOCUMENTS.len()];
		let sql = format!(r#"CREATE doc SET content = "{content}""#);
		dbs.execute(&sql, &ses, None).await.unwrap().remove(0).result.unwrap();
	}

	BenchInput {
		dbs,
		ses,
		doc_count,
	}
}

async fn prepare_medium_corpus() -> BenchInput {
	let dbs = Datastore::new(&get_datastore_path()).await.unwrap();
	let ses = Session::owner().with_ns("bench").with_db("bench");

	let sql = r"
        DEFINE ANALYZER bench_analyzer TOKENIZERS blank, class, punct FILTERS lowercase, ascii;
        DEFINE INDEX ft_idx ON TABLE doc FIELDS content FULLTEXT ANALYZER bench_analyzer BM25;
    ";
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	for _ in 0..2 {
		res.remove(0).result.unwrap();
	}

	let doc_count = if cfg!(debug_assertions) {
		500
	} else {
		10_000
	};

	for i in 0..doc_count {
		let content = if i % 10 == 0 {
			LONG_DOCUMENTS[i % LONG_DOCUMENTS.len()]
		} else {
			DOCUMENTS[i % DOCUMENTS.len()]
		};
		let sql = format!(r#"CREATE doc SET content = "{content}""#);
		dbs.execute(&sql, &ses, None).await.unwrap().remove(0).result.unwrap();
	}

	BenchInput {
		dbs,
		ses,
		doc_count,
	}
}

async fn prepare_hybrid_corpus() -> BenchInput {
	let dbs = Datastore::new(&get_datastore_path()).await.unwrap();
	let ses = Session::owner().with_ns("bench").with_db("bench");

	// Define analyzer, full-text index, and vector index
	let sql = r"
        DEFINE ANALYZER bench_analyzer TOKENIZERS blank, class, punct FILTERS lowercase, ascii;
        DEFINE INDEX ft_idx ON TABLE doc FIELDS content FULLTEXT ANALYZER bench_analyzer BM25;
        DEFINE INDEX vec_idx ON TABLE doc FIELDS embedding HNSW DIMENSION 8 DIST COSINE;
    ";
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	for _ in 0..3 {
		res.remove(0).result.unwrap();
	}

	let doc_count = if cfg!(debug_assertions) {
		100
	} else {
		1_000
	};

	for i in 0..doc_count {
		let content = DOCUMENTS[i % DOCUMENTS.len()];
		// Generate pseudo-random embedding based on document index
		let e: Vec<f32> = (0..8).map(|j| ((i + j) as f32 * 0.1).sin()).collect();
		let embedding = format!(
			"[{:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}, {:.4}]",
			e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7]
		);
		let sql = format!(r#"CREATE doc SET content = "{content}", embedding = {embedding}"#);
		dbs.execute(&sql, &ses, None).await.unwrap().remove(0).result.unwrap();
	}

	BenchInput {
		dbs,
		ses,
		doc_count,
	}
}

async fn run_query(input: &BenchInput, query: &str) {
	let r = input.dbs.execute(black_box(query), &input.ses, None).await.unwrap();
	black_box(r);
}

fn bench_fulltext_search(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();

	// Benchmark with small corpus
	let small = rt.block_on(prepare_small_corpus());

	let mut group = c.benchmark_group("fulltext_search");
	group.sample_size(50);
	group.measurement_time(Duration::from_secs(10));

	// Single term search
	group.bench_function("small_corpus/single_term", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run_query(&small, "SELECT * FROM doc WHERE content @@ 'database'"))
	});

	// Multi-term search (AND)
	group.bench_function("small_corpus/multi_term_and", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run_query(&small, "SELECT * FROM doc WHERE content @@ 'graph database'"))
	});

	// Search with scoring
	group.bench_function("small_corpus/with_bm25_score", |b| {
		b.to_async(Runtime::new().unwrap()).iter(|| {
			run_query(
				&small,
				"SELECT *, search::score(1) AS score FROM doc WHERE content @1@ 'database' ORDER BY score DESC",
			)
		})
	});

	// Search with limit
	group.bench_function("small_corpus/with_limit", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| {
            run_query(
                &small,
                "SELECT *, search::score(1) AS score FROM doc WHERE content @1@ 'database' ORDER BY score DESC LIMIT 10",
            )
        })
    });

	group.finish();
	rt.block_on(async { drop(small) });
}

fn bench_fulltext_scaling(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();

	let medium = rt.block_on(prepare_medium_corpus());

	let mut group = c.benchmark_group("fulltext_scaling");
	group.sample_size(30);
	group.measurement_time(Duration::from_secs(15));
	group.throughput(Throughput::Elements(medium.doc_count as u64));

	// Common term (appears in many documents)
	group.bench_function(
        BenchmarkId::new("common_term", medium.doc_count),
        |b| {
            b.to_async(Runtime::new().unwrap()).iter(|| {
                run_query(
                    &medium,
                    "SELECT *, search::score(1) AS score FROM doc WHERE content @1@ 'data' ORDER BY score DESC LIMIT 20",
                )
            })
        },
    );

	// Rare term (appears in few documents)
	group.bench_function(BenchmarkId::new("rare_term", medium.doc_count), |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| {
            run_query(
                &medium,
                "SELECT *, search::score(1) AS score FROM doc WHERE content @1@ 'probabilistic' ORDER BY score DESC LIMIT 20",
            )
        })
    });

	// Phrase-like query
	group.bench_function(
        BenchmarkId::new("phrase_query", medium.doc_count),
        |b| {
            b.to_async(Runtime::new().unwrap()).iter(|| {
                run_query(
                    &medium,
                    "SELECT *, search::score(1) AS score FROM doc WHERE content @1@ 'full text search' ORDER BY score DESC LIMIT 20",
                )
            })
        },
    );

	group.finish();
	rt.block_on(async { drop(medium) });
}

fn bench_hybrid_search(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();

	let hybrid = rt.block_on(prepare_hybrid_corpus());

	let mut group = c.benchmark_group("hybrid_search");
	group.sample_size(30);
	group.measurement_time(Duration::from_secs(15));

	// Full-text only
	group.bench_function("fulltext_only", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| {
            run_query(
                &hybrid,
                "SELECT id, search::score(1) AS ft_score FROM doc WHERE content @1@ 'database' ORDER BY ft_score DESC LIMIT 10",
            )
        })
    });

	// Vector only
	group.bench_function("vector_only", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| {
            run_query(
                &hybrid,
                "SELECT id, vector::distance::knn() AS distance FROM doc WHERE embedding <|10,100|> [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]",
            )
        })
    });

	// RRF fusion
	group.bench_function("rrf_fusion", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| {
            run_query(
                &hybrid,
                r#"
                LET $ft = SELECT id, search::score(1) AS ft_score FROM doc WHERE content @1@ 'database' ORDER BY ft_score DESC LIMIT 20;
                LET $vec = SELECT id, vector::distance::knn() AS distance FROM doc WHERE embedding <|20,100|> [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
                RETURN search::rrf([$ft, $vec], 10, 60);
                "#,
            )
        })
    });

	// Linear fusion with minmax normalization
	group.bench_function("linear_fusion_minmax", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| {
            run_query(
                &hybrid,
                r#"
                LET $ft = SELECT id, search::score(1) AS ft_score FROM doc WHERE content @1@ 'database' ORDER BY ft_score DESC LIMIT 20;
                LET $vec = SELECT id, vector::distance::knn() AS distance FROM doc WHERE embedding <|20,100|> [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
                RETURN search::linear([$ft, $vec], [1, 1], 10, 'minmax');
                "#,
            )
        })
    });

	// Linear fusion with zscore normalization
	group.bench_function("linear_fusion_zscore", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| {
            run_query(
                &hybrid,
                r#"
                LET $ft = SELECT id, search::score(1) AS ft_score FROM doc WHERE content @1@ 'database' ORDER BY ft_score DESC LIMIT 20;
                LET $vec = SELECT id, vector::distance::knn() AS distance FROM doc WHERE embedding <|20,100|> [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
                RETURN search::linear([$ft, $vec], [1, 1], 10, 'zscore');
                "#,
            )
        })
    });

	group.finish();
	rt.block_on(async { drop(hybrid) });
}

fn bench_bm25_scoring_only(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();

	let small = rt.block_on(prepare_small_corpus());

	let mut group = c.benchmark_group("bm25_scoring");
	group.sample_size(100);
	group.measurement_time(Duration::from_secs(10));

	// Measure scoring overhead by comparing with/without score computation
	group.bench_function("without_score", |b| {
		b.to_async(Runtime::new().unwrap())
			.iter(|| run_query(&small, "SELECT id FROM doc WHERE content @@ 'database' LIMIT 50"))
	});

	group.bench_function("with_score", |b| {
		b.to_async(Runtime::new().unwrap()).iter(|| {
			run_query(
				&small,
				"SELECT id, search::score(1) AS score FROM doc WHERE content @1@ 'database' LIMIT 50",
			)
		})
	});

	group.bench_function("with_score_and_order", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| {
            run_query(
                &small,
                "SELECT id, search::score(1) AS score FROM doc WHERE content @1@ 'database' ORDER BY score DESC LIMIT 50",
            )
        })
    });

	group.finish();
	rt.block_on(async { drop(small) });
}

/// Prepare a large corpus using batch insertion.
/// Inserts documents FIRST, then creates index (bulk loading pattern).
async fn prepare_million_scale_corpus(doc_count: usize) -> BenchInput {
	let dbs = Datastore::new(&get_datastore_path()).await.unwrap();
	let ses = Session::owner().with_ns("bench").with_db("bench");

	// Simple analyzer for speed
	let sql = r"DEFINE ANALYZER bench_analyzer TOKENIZERS blank FILTERS lowercase;";
	dbs.execute(sql, &ses, None).await.unwrap().remove(0).result.unwrap();

	// Sequential batch insert (avoids transaction conflicts)
	const BATCH_SIZE: usize = 1000;
	let all_docs: Vec<&str> = DOCUMENTS.iter().chain(LONG_DOCUMENTS.iter()).copied().collect();

	eprintln!("  Inserting {} documents...", doc_count);
	let start = std::time::Instant::now();

	for batch_start in (0..doc_count).step_by(BATCH_SIZE) {
		let batch_end = (batch_start + BATCH_SIZE).min(doc_count);

		let mut values = Vec::with_capacity(batch_end - batch_start);
		for i in batch_start..batch_end {
			let content = all_docs[i % all_docs.len()];
			values.push(format!(r#"{{ content: "{content}" }}"#));
		}

		let sql = format!("INSERT INTO doc [{}]", values.join(", "));
		dbs.execute(&sql, &ses, None).await.unwrap().remove(0).result.unwrap();

		if batch_start % 100_000 == 0 && batch_start > 0 {
			eprintln!("    ... {} docs inserted", batch_start);
		}
	}

	eprintln!("  Inserted {} docs in {:.1}s", doc_count, start.elapsed().as_secs_f64());

	// NOW create index (uses parallel batch processing internally)
	eprintln!("  Creating fulltext index with BM25...");
	let idx_start = std::time::Instant::now();
	let sql = "DEFINE INDEX ft_idx ON TABLE doc FIELDS content FULLTEXT ANALYZER bench_analyzer BM25;";
	dbs.execute(sql, &ses, None).await.unwrap().remove(0).result.unwrap();
	eprintln!("  Index created in {:.1}s", idx_start.elapsed().as_secs_f64());

	BenchInput {
		dbs,
		ses,
		doc_count,
	}
}

/// Benchmark with 1M documents.
/// Run with: BENCH_DB_PATH=/path/to/disk cargo bench -p surrealdb --features kv-rocksdb -- 1m
fn bench_bm25_1m_corpus(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();

	// Only run with RocksDB to avoid memory issues
	#[cfg(not(feature = "kv-rocksdb"))]
	{
		eprintln!("Skipping 1M benchmark: requires kv-rocksdb feature");
		return;
	}

	if std::env::var("BENCH_DB_PATH").is_err() {
		eprintln!("Skipping 1M benchmark: set BENCH_DB_PATH for RocksDB");
		return;
	}

	let doc_count = 1_000_000;
	eprintln!("Preparing 1M documents corpus...");
	let corpus = rt.block_on(prepare_million_scale_corpus(doc_count));

	let mut group = c.benchmark_group("bm25_1m_corpus");
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(30));
	group.throughput(Throughput::Elements(doc_count as u64));

	// Common term - many matches
	group.bench_function("common_term", |b| {
		b.to_async(Runtime::new().unwrap()).iter(|| {
			run_query(
				&corpus,
				"SELECT *, search::score(1) AS score FROM doc WHERE content @1@ 'database' ORDER BY score DESC LIMIT 100",
			)
		})
	});

	// Rare term - few matches
	group.bench_function("rare_term", |b| {
		b.to_async(Runtime::new().unwrap()).iter(|| {
			run_query(
				&corpus,
				"SELECT *, search::score(1) AS score FROM doc WHERE content @1@ 'probabilistic' ORDER BY score DESC LIMIT 100",
			)
		})
	});

	group.finish();
	rt.block_on(async { drop(corpus) });
}

/// Benchmark with 10M documents.
/// Run with: BENCH_DB_PATH=/path/to/disk cargo bench -p surrealdb --features kv-rocksdb -- 10m
fn bench_bm25_10m_corpus(c: &mut Criterion) {
	let rt = Runtime::new().unwrap();

	// Only run with RocksDB to avoid memory issues
	#[cfg(not(feature = "kv-rocksdb"))]
	{
		eprintln!("Skipping 10M benchmark: requires kv-rocksdb feature");
		return;
	}

	if std::env::var("BENCH_DB_PATH").is_err() {
		eprintln!("Skipping 10M benchmark: set BENCH_DB_PATH for RocksDB");
		return;
	}

	let doc_count = 10_000_000;
	eprintln!("Preparing 10M documents corpus...");
	let corpus = rt.block_on(prepare_million_scale_corpus(doc_count));

	let mut group = c.benchmark_group("bm25_10m_corpus");
	group.sample_size(10);
	group.measurement_time(Duration::from_secs(60));
	group.throughput(Throughput::Elements(doc_count as u64));

	// Common term - many matches
	group.bench_function("common_term", |b| {
		b.to_async(Runtime::new().unwrap()).iter(|| {
			run_query(
				&corpus,
				"SELECT *, search::score(1) AS score FROM doc WHERE content @1@ 'database' ORDER BY score DESC LIMIT 100",
			)
		})
	});

	// Rare term - few matches
	group.bench_function("rare_term", |b| {
		b.to_async(Runtime::new().unwrap()).iter(|| {
			run_query(
				&corpus,
				"SELECT *, search::score(1) AS score FROM doc WHERE content @1@ 'probabilistic' ORDER BY score DESC LIMIT 100",
			)
		})
	});

	group.finish();
	rt.block_on(async { drop(corpus) });
}

criterion_group!(
	benches,
	bench_fulltext_search,
	bench_fulltext_scaling,
	bench_hybrid_search,
	bench_bm25_scoring_only,
	bench_bm25_1m_corpus,
	bench_bm25_10m_corpus
);
criterion_main!(benches);
