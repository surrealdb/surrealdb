//! Inline sidecar cache semantics, driven end to end through a `Datastore`
//! with real SurrealQL.
//!
//! The invariant under test: query results must be indistinguishable with
//! caches on and off, while `EXPLAIN ANALYZE`'s `cache_hits` /
//! `cache_misses` counters make the tier's engagement observable — a live
//! hit serves the traversal without a cursor, everything else falls back.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_types::Value;

use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::expr::Dir;
use crate::idx::adjacency::fold_scope;
use crate::kvs::Datastore;
use crate::val::RecordId;

async fn ds() -> Arc<Datastore> {
	Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap()
}

async fn run(ds: &Datastore, ses: &Session, sql: &str) -> Vec<Value> {
	ds.execute(sql, ses, None)
		.await
		.unwrap()
		.into_iter()
		.map(|response| response.result.unwrap())
		.collect()
}

/// Sums every occurrence of the named metric across an `EXPLAIN ANALYZE`
/// rendering (a text plan tree with `name: value` metric pairs).
fn sum_metric(value: &Value, name: &str) -> i64 {
	let Value::String(text) = value else {
		panic!("expected a rendered plan, found {value:?}");
	};
	let marker = format!("{name}: ");
	let mut sum = 0;
	let mut rest = text.as_str();
	while let Some(at) = rest.find(&marker) {
		rest = &rest[at + marker.len()..];
		let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
		sum += digits.parse::<i64>().unwrap_or(0);
	}
	sum
}

/// Runs one query under `EXPLAIN ANALYZE` and returns the summed
/// `(cache_hits, cache_misses)` its plan reports.
async fn cache_counters(ds: &Datastore, ses: &Session, query: &str) -> (i64, i64) {
	let explained = run(ds, ses, &format!("EXPLAIN ANALYZE {query}")).await.remove(0);
	(sum_metric(&explained, "cache_hits"), sum_metric(&explained, "cache_misses"))
}

/// Folds every scope of the given vertex to completion, through the same
/// context-as-environment shape the background task uses.
async fn fold_vertex(ds: &Datastore, rid: &RecordId) {
	for dir in [Dir::In, Dir::Out] {
		loop {
			let txn = Arc::new(ds.transaction(Write).await.unwrap());
			let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
			let mut env = ds.setup_ctx().unwrap();
			env.set_transaction(Arc::clone(&txn));
			let env = env.freeze();
			let outcome =
				fold_scope(&env, db.namespace_id, db.database_id, "test", "test", rid, dir, 1024)
					.await
					.unwrap();
			txn.commit().await.unwrap();
			if !outcome.has_more {
				break;
			}
		}
	}
}

/// The query battery whose results must be cache-invariant: id-only
/// traversal, the target-vertex fast path, reverse and bidirectional
/// traversal, full edge projection, a pushed-down edge predicate, and a
/// multi-hop chain.
const BATTERY: &[&str] = &[
	"SELECT VALUE ->likes FROM person:p0;",
	"SELECT VALUE ->likes->person FROM person:p0;",
	"SELECT VALUE <-likes FROM person:p2;",
	"SELECT VALUE <->likes FROM person:p1;",
	"SELECT VALUE ->likes.* FROM person:p0;",
	"SELECT VALUE ->(likes WHERE out = person:p2) FROM person:p0;",
	"SELECT VALUE ->likes->person->likes->person FROM person:p0;",
	"SELECT VALUE ->likes->person FROM person:p0 LIMIT 2;",
	"SELECT VALUE ->likes FROM person ORDER BY id;",
];

async fn battery(ds: &Datastore, ses: &Session) -> Vec<Vec<Value>> {
	let mut out = Vec::new();
	for query in BATTERY {
		out.push(run(ds, ses, query).await);
	}
	out
}

/// Seeds the graph; `caps` is the person table's INLINE clause, or empty.
async fn seed(ds: &Datastore, ses: &Session, caps: &str) {
	run(
		ds,
		ses,
		&format!(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE person {caps};
			 DEFINE TABLE likes TYPE RELATION;
			 CREATE person:p0, person:p1, person:p2, person:p3, person:p4;
			 RELATE person:p0->likes:l01->person:p1;
			 RELATE person:p0->likes:l02->person:p2;
			 RELATE person:p0->likes:l03->person:p3;
			 RELATE person:p1->likes:l12->person:p2;
			 RELATE person:p2->likes:l23->person:p3;
			 RELATE person:p3->likes:l34->person:p4;
			 RELATE person:p4->likes:l40->person:p0;"
		),
	)
	.await;
}

fn person(n: usize) -> RecordId {
	RecordId::new("person".into(), format!("p{n}"))
}

/// The headline equality: an instance with caps on answers the whole
/// battery identically to one with caps off, across creates, deletes and
/// re-relates — and its fan-out queries actually engage the cache.
#[tokio::test]
async fn cached_results_equal_uncached_results() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let cached = ds().await;
	let uncached = ds().await;
	seed(&cached, &ses, "INLINE EDGES 16").await;
	seed(&uncached, &ses, "").await;
	assert_eq!(battery(&cached, &ses).await, battery(&uncached, &ses).await);

	// Mutate both identically: delete one edge, re-relate it to a new
	// target, delete a vertex (cascading over its edges).
	const MUTATIONS: &str = "DELETE likes:l02;
		 RELATE person:p0->likes:l02->person:p4;
		 DELETE person:p3;";
	run(&cached, &ses, MUTATIONS).await;
	run(&uncached, &ses, MUTATIONS).await;
	assert_eq!(battery(&cached, &ses).await, battery(&uncached, &ses).await);

	// The RELATEs above materialised the caches, so a fan-out serves from
	// them: every per-(source, direction) lookup hits.
	let (hits, misses) = cache_counters(&cached, &ses, BATTERY[0]).await;
	assert_eq!((hits, misses), (1, 0));
	let (hits, misses) = cache_counters(&uncached, &ses, BATTERY[0]).await;
	assert_eq!((hits, misses), (0, 0));
}

/// A key-resident predicate reads only the `(edge, target)` pairs the
/// cache stores, so a cached vertex serves the filtered lookup without
/// opening a cursor: answers equal the uncached ones, the cache-hit
/// counter engages, and the merged cursor reports no delta hits.
#[tokio::test]
async fn key_resident_predicates_serve_from_the_cache() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let cached = ds().await;
	let uncached = ds().await;
	seed(&cached, &ses, "INLINE EDGES 16").await;
	seed(&uncached, &ses, "").await;

	// The RELATEs in `seed` materialised person:p0's cache.
	const QUERIES: &[&str] = &[
		"SELECT VALUE ->(likes WHERE out = person:p2) FROM person:p0;",
		"SELECT VALUE ->(likes WHERE out != person:p2) FROM person:p0;",
		"SELECT VALUE ->(likes WHERE id = likes:l01) FROM person:p0;",
	];
	for query in QUERIES {
		assert_eq!(
			run(&cached, &ses, query).await,
			run(&uncached, &ses, query).await,
			"cached and uncached answers diverged for {query}"
		);
		let explained = run(&cached, &ses, &format!("EXPLAIN ANALYZE {query}")).await.remove(0);
		assert_eq!(sum_metric(&explained, "cache_hits"), 1, "{query} must hit the cache");
		assert_eq!(sum_metric(&explained, "cache_misses"), 0);
		assert_eq!(
			sum_metric(&explained, "delta_hits"),
			0,
			"{query} must be served without opening the merged cursor"
		);
		assert_eq!(
			sum_metric(&explained, "props_evals"),
			3,
			"{query} must evaluate every cached edge"
		);
	}
}

/// Setting a cap on a table with pre-existing edges: the first eligible
/// write backfills the complete set — the cache serves the old edges too.
#[tokio::test]
async fn first_write_backfills_preexisting_edges() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, "").await;

	// No cap yet: not even a miss is counted (the lookup is ineligible).
	assert_eq!(cache_counters(&ds, &ses, BATTERY[0]).await, (0, 0));

	run(&ds, &ses, "ALTER TABLE person INLINE EDGES 16;").await;
	// Cap set, cache not yet materialised: eligible lookup, miss.
	assert_eq!(cache_counters(&ds, &ses, BATTERY[0]).await, (0, 1));

	let before = run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p0;").await;
	run(&ds, &ses, "RELATE person:p0->likes:l04->person:p4;").await;
	let (hits, _) = cache_counters(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p0;").await;
	assert_eq!(hits, 1);
	let after = run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p0;").await;
	// The cached answer is the pre-existing edges plus the new one.
	let Value::Array(before) = before.into_iter().next().unwrap() else {
		panic!("expected an array")
	};
	let Value::Array(after) = after.into_iter().next().unwrap() else {
		panic!("expected an array")
	};
	let before = before.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	let after = after.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(after.len(), before.len() + 1);
	for v in &before {
		assert!(after.contains(v));
	}
}

/// Deleting a vertex's last edge leaves an authoritative empty cache: the
/// next fan-out is a hit that yields nothing, with no cursor opened.
#[tokio::test]
async fn delete_to_empty_is_an_authoritative_hit() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, "INLINE EDGES 16").await;

	run(&ds, &ses, "DELETE likes:l01; DELETE likes:l02; DELETE likes:l03;").await;
	let (hits, misses) = cache_counters(&ds, &ses, BATTERY[0]).await;
	assert_eq!((hits, misses), (1, 0));
	assert_eq!(run(&ds, &ses, BATTERY[0]).await, vec![crate::syn::value("[[]]").unwrap()]);
}

/// Folding moves adjacency beneath the cache without changing it: deletes
/// on a folded table tombstone the pointer key and still maintain the
/// cache, and the battery stays invariant.
#[tokio::test]
async fn folded_tables_keep_caches_correct() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let cached = ds().await;
	let uncached = ds().await;
	seed(&cached, &ses, "INLINE EDGES 16").await;
	seed(&uncached, &ses, "").await;

	for n in 0..5 {
		fold_vertex(&cached, &person(n)).await;
		fold_vertex(&uncached, &person(n)).await;
	}
	assert_eq!(battery(&cached, &ses).await, battery(&uncached, &ses).await);

	// A delete on the folded table tombstones below the cache and removes
	// the entry from it, in one transaction.
	run(&cached, &ses, "DELETE likes:l02;").await;
	run(&uncached, &ses, "DELETE likes:l02;").await;
	assert_eq!(battery(&cached, &ses).await, battery(&uncached, &ses).await);
	let (hits, misses) = cache_counters(&cached, &ses, BATTERY[0]).await;
	assert_eq!((hits, misses), (1, 0));
}

/// Any cap change is durable DDL that drops the table's cache subspace:
/// stale caches cannot survive into the new regime, and the next write
/// re-materialises under it.
#[tokio::test]
async fn cap_changes_invalidate_and_rematerialise() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, "INLINE EDGES 16").await;
	assert_eq!(cache_counters(&ds, &ses, BATTERY[0]).await, (1, 0));

	// Raise the cap: caches dropped, eligible lookups miss.
	run(&ds, &ses, "ALTER TABLE person INLINE EDGES 32;").await;
	assert_eq!(cache_counters(&ds, &ses, BATTERY[0]).await, (0, 1));

	// A write re-materialises; results unchanged throughout.
	let before = run(&ds, &ses, BATTERY[0]).await;
	run(&ds, &ses, "RELATE person:p0->likes:l04->person:p4; DELETE likes:l04;").await;
	assert_eq!(cache_counters(&ds, &ses, BATTERY[0]).await, (1, 0));
	assert_eq!(run(&ds, &ses, BATTERY[0]).await, before);

	// Dropping the cap disables the tier entirely.
	run(&ds, &ses, "ALTER TABLE person DROP INLINE EDGES;").await;
	assert_eq!(cache_counters(&ds, &ses, BATTERY[0]).await, (0, 0));
	assert_eq!(run(&ds, &ses, BATTERY[0]).await, before);
}

/// A vertex past its cap spills: results are served by the scan, observably
/// as an eligible miss, and stay correct.
#[tokio::test]
async fn spilled_vertices_fall_back_to_the_scan() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, "INLINE EDGES 2").await;
	// p0 has three outgoing likes: over the cap, spilled at write time.
	assert_eq!(cache_counters(&ds, &ses, BATTERY[0]).await, (0, 1));
	let out = run(&ds, &ses, "SELECT VALUE ->likes FROM person:p0;").await;
	let Value::Array(rows) = out.into_iter().next().unwrap() else {
		panic!("expected an array")
	};
	let edges = rows.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(edges.len(), 3);
}

/// Concurrent RELATEs against one vertex all survive: the cache key is a
/// single point of write contention, so writers conflict and retry, but no
/// update is lost and the final cache equals the full edge set.
#[tokio::test]
async fn concurrent_relates_lose_no_cache_updates() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person INLINE EDGES 32;
		 DEFINE TABLE likes TYPE RELATION;
		 CREATE person:hub;
		 CREATE person:s0, person:s1, person:s2, person:s3,
		        person:s4, person:s5, person:s6, person:s7;",
	)
	.await;
	let mut handles = Vec::new();
	for n in 0..8 {
		let ds = Arc::clone(&ds);
		let ses = ses.clone();
		handles.push(tokio::spawn(async move {
			let sql = format!("RELATE person:hub->likes:l{n}->person:s{n};");
			// A conflict on the shared cache key aborts the transaction;
			// retry until the write lands, as a client would.
			for _ in 0..64 {
				let mut res = ds.execute(&sql, &ses, None).await.unwrap();
				if res.remove(0).result.is_ok() {
					return;
				}
			}
			panic!("relate never committed");
		}));
	}
	for handle in handles {
		handle.await.unwrap();
	}
	let (hits, misses) = cache_counters(&ds, &ses, "SELECT VALUE ->likes FROM person:hub;").await;
	assert_eq!((hits, misses), (1, 0));
	let out = run(&ds, &ses, "SELECT VALUE ->likes FROM person:hub;").await;
	let Value::Array(rows) = out.into_iter().next().unwrap() else {
		panic!("expected an array")
	};
	let edges = rows.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(edges.len(), 8);
}

/// The reference cache serves `<~` reverse lookups and stays maintained
/// through reference writes, unsets and the ON DELETE strategies.
#[tokio::test]
async fn reference_caches_serve_reverse_lookups() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person INLINE REFERENCES 8;
		 DEFINE TABLE post;
		 DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE ON DELETE UNSET;
		 CREATE person:p1;
		 CREATE post:a SET author = person:p1;
		 CREATE post:b SET author = person:p1;",
	)
	.await;
	let query = "SELECT VALUE <~post FROM person:p1;";
	let (hits, misses) = cache_counters(&ds, &ses, query).await;
	assert_eq!((hits, misses), (1, 0));
	let out = run(&ds, &ses, query).await;
	let Value::Array(rows) = out.into_iter().next().unwrap() else {
		panic!("expected an array")
	};
	let refs = rows.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(refs.len(), 2);

	// Unsetting a reference maintains the cache in the same transaction.
	run(&ds, &ses, "UPDATE post:a UNSET author;").await;
	let out = run(&ds, &ses, query).await;
	let Value::Array(rows) = out.into_iter().next().unwrap() else {
		panic!("expected an array")
	};
	let refs = rows.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(refs.len(), 1);
	assert_eq!(cache_counters(&ds, &ses, query).await, (1, 0));

	// Deleting the referencing record cleans its back-link too.
	run(&ds, &ses, "DELETE post:b;").await;
	let out = run(&ds, &ses, query).await;
	let Value::Array(rows) = out.into_iter().next().unwrap() else {
		panic!("expected an array")
	};
	let refs = rows.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(refs.len(), 0);
	assert_eq!(cache_counters(&ds, &ses, query).await, (1, 0));
}

/// The cache fast path spends only the LIMIT budget: entries past the
/// limit are neither decoded nor counted, so the scanned-edges metric
/// equals the rows the limit consumed — exactly how the cursor path
/// meters itself.
#[tokio::test]
async fn cache_fast_path_stops_at_the_limit() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let cached = ds().await;
	let uncached = ds().await;
	seed(&cached, &ses, "INLINE EDGES 16").await;
	seed(&uncached, &ses, "").await;
	// p0 has three outgoing edges; the limit consumes two of them.
	let query = "SELECT VALUE ->(likes LIMIT 2) FROM person:p0;";
	assert_eq!(run(&cached, &ses, query).await, run(&uncached, &ses, query).await);
	let explained = run(&cached, &ses, &format!("EXPLAIN ANALYZE {query}")).await.remove(0);
	assert_eq!(sum_metric(&explained, "cache_hits"), 1);
	assert_eq!(
		sum_metric(&explained, "scanned"),
		2,
		"the fast path must not materialise entries past the limit"
	);
}

/// Narrowed reverse-reference lookups (table- and field-restricted) over a
/// cached target equal the uncached scan, while still engaging the cache:
/// the raw-byte pre-screen must reject exactly the entries the decoded
/// comparison would.
#[tokio::test]
async fn narrowed_reference_lookups_stay_cache_invariant() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let cached = ds().await;
	let uncached = ds().await;
	for (ds, caps) in [(&cached, "INLINE REFERENCES 8"), (&uncached, "")] {
		run(
			ds,
			&ses,
			&format!(
				"DEFINE NAMESPACE test;
				 DEFINE DATABASE test;
				 DEFINE TABLE person {caps};
				 DEFINE TABLE post;
				 DEFINE TABLE comment;
				 DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE ON DELETE UNSET;
				 DEFINE FIELD editor ON post TYPE option<record<person>> REFERENCE ON DELETE UNSET;
				 DEFINE FIELD author ON comment TYPE option<record<person>> REFERENCE ON DELETE UNSET;
				 CREATE person:p1;
				 CREATE post:a SET author = person:p1, editor = person:p1;
				 CREATE post:b SET author = person:p1;
				 CREATE comment:c SET author = person:p1;"
			),
		)
		.await;
	}
	let queries = [
		"SELECT VALUE <~? FROM person:p1;",
		"SELECT VALUE <~post FROM person:p1;",
		"SELECT VALUE <~comment FROM person:p1;",
		"SELECT VALUE <~(post FIELD author) FROM person:p1;",
		"SELECT VALUE <~(post FIELD editor) FROM person:p1;",
	];
	for query in queries {
		assert_eq!(
			run(&cached, &ses, query).await,
			run(&uncached, &ses, query).await,
			"diverged on {query}"
		);
		let (hits, misses) = cache_counters(&cached, &ses, query).await;
		assert_eq!((hits, misses), (1, 0), "no cache engagement on {query}");
	}
}

/// Whether the record's `!cr` reference-cache key is present in the store.
async fn ref_cache_key_present(ds: &Datastore, rid: &RecordId) -> bool {
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let key = crate::key::schema::RefCacheKey {
		ns: db.namespace_id,
		db: db.database_id,
		tb: std::borrow::Cow::Borrowed(&rid.table),
		id: std::borrow::Cow::Borrowed(&rid.key),
	};
	let present = txn.get_key(&key, None).await.unwrap().is_some();
	txn.cancel().await.unwrap();
	present
}

/// Deleting a record removes its reference-cache key whenever some field in
/// the database can reference the table (the same gate the reference purge
/// applies) — the key must not outlive the record it caches.
#[tokio::test]
async fn record_delete_purges_its_ref_cache_key() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person INLINE REFERENCES 8;
		 DEFINE TABLE post;
		 DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE ON DELETE UNSET;
		 CREATE person:p1;
		 CREATE post:a SET author = person:p1;",
	)
	.await;
	let rid = RecordId::new("person".into(), "p1".to_owned());
	assert!(ref_cache_key_present(&ds, &rid).await, "the reference write materialises the cache");
	run(&ds, &ses, "DELETE person:p1;").await;
	assert!(!ref_cache_key_present(&ds, &rid).await, "the purge must drop the cache key");
}

/// Seeded randomized oracle: a cap-on instance and a cap-off twin replay
/// the same random relate/delete/re-relate sequence, checking the whole
/// battery for equality along the way. Caps small enough that spill and
/// un-spill-by-purge paths are exercised, not just steady-state hits.
#[tokio::test]
async fn randomized_mutations_stay_cache_invariant() {
	use rand::rngs::StdRng;
	use rand::{Rng, SeedableRng};

	let ses = Session::owner().with_ns("test").with_db("test");
	for seed in [3u64, 17, 4242] {
		let mut rng = StdRng::seed_from_u64(seed);
		let cached = ds().await;
		let uncached = ds().await;
		seed_ds(&cached, &ses, "INLINE EDGES 4").await;
		seed_ds(&uncached, &ses, "").await;

		// (edge id, source, target) triples currently believed live.
		let mut live: Vec<(usize, usize, usize)> = Vec::new();
		let mut next_edge = 0usize;
		for step in 0..60 {
			let sql = if live.is_empty() || rng.random_range(0..10) < 6 {
				let source = rng.random_range(0..5);
				let target = rng.random_range(0..5);
				let id = next_edge;
				next_edge += 1;
				live.push((id, source, target));
				format!("RELATE person:p{source}->likes:l{id}->person:p{target};")
			} else if rng.random_range(0..4) == 0 {
				// Delete a vertex: cascades over each edge touching it.
				let victim = rng.random_range(0..5);
				live.retain(|(_, s, t)| *s != victim && *t != victim);
				format!("DELETE person:p{victim}; CREATE person:p{victim};")
			} else {
				let at = rng.random_range(0..live.len());
				let (id, _, _) = live.swap_remove(at);
				format!("DELETE likes:l{id};")
			};
			run(&cached, &ses, &sql).await;
			run(&uncached, &ses, &sql).await;
			if step % 10 == 9 {
				assert_eq!(
					battery(&cached, &ses).await,
					battery(&uncached, &ses).await,
					"diverged at seed {seed} step {step}"
				);
			}
		}
		assert_eq!(battery(&cached, &ses).await, battery(&uncached, &ses).await);
	}
}

/// Seeds only the schema and vertices — the randomized test drives all
/// edge mutations itself.
async fn seed_ds(ds: &Datastore, ses: &Session, caps: &str) {
	run(
		ds,
		ses,
		&format!(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE person {caps};
			 DEFINE TABLE likes TYPE RELATION;
			 CREATE person:p0, person:p1, person:p2, person:p3, person:p4;"
		),
	)
	.await;
}
