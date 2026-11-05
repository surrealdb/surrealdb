//! The format a defined or rebuilt index is stamped with follows the store's
//! feature level: a store at the Legacy level stamps no format above
//! `LEGACY_LEVEL_INDEX_FORMAT_VERSION`, and a store at a recorded level stamps
//! `INDEX_FORMAT_VERSION`.

use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{INDEX_FORMAT_VERSION, LEGACY_LEVEL_INDEX_FORMAT_VERSION};
use crate::dbs::Session;
use crate::key::schema::{StorageVersionKey, VersionKey};
use crate::kvs::Datastore;
use crate::kvs::version::{MajorVersion, StorageVersion};
use crate::val::TableName;

/// A store an earlier release wrote, opened by this build with the opt-in to upgrade it, and held
/// at the Legacy feature level.
async fn upgraded() -> Arc<Datastore> {
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.without_startup_transactions()
		.with_config(
			surrealdb_cnf::ConfigMap::empty()
				.with_key_value("experimental_upgrade_from_3x", "true"),
		)
		.with_legacy_feature_level()
		.build_with_path("memory")
		.await
		.unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	txn.set_key(&VersionKey {}, &MajorVersion::latest()).await.unwrap();
	txn.set_key(&StorageVersionKey {}, &StorageVersion::parse("3.3.0").unwrap()).await.unwrap();
	txn.commit().await.unwrap();
	ds.check_version().await.unwrap();
	assert!(ds.feature_level().await.unwrap().level.is_legacy(), "the store left the Legacy level");
	ds
}

/// A store this build created, at its own release's level.
async fn levelled() -> Arc<Datastore> {
	let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await;
	let ds = ds.unwrap();
	assert!(!ds.feature_level().await.unwrap().level.is_legacy(), "a new store is at Legacy");
	ds
}

/// The format `ix` on `t` is stamped with.
async fn stamped(ds: &Datastore, ix: &str) -> u16 {
	let tx = ds.transaction(Write).await.unwrap();
	let ns = tx.get_ns_by_name("test", None).await.unwrap().expect("namespace exists");
	let db = tx.get_db_by_name("test", "test", None).await.unwrap().expect("database exists");
	let tb: TableName = "t".into();
	let def = tx
		.get_tb_index(ns.namespace_id, db.database_id, &tb, ix, None)
		.await
		.unwrap()
		.expect("index exists");
	tx.cancel().await.unwrap();
	def.format_version
}

/// The id and format `ix` on `t` has.
async fn identity(ds: &Datastore, ix: &str) -> (crate::catalog::IndexId, u16) {
	let tx = ds.transaction(Write).await.unwrap();
	let ns = tx.get_ns_by_name("test", None).await.unwrap().expect("namespace exists");
	let db = tx.get_db_by_name("test", "test", None).await.unwrap().expect("database exists");
	let tb: TableName = "t".into();
	let def = tx
		.get_tb_index(ns.namespace_id, db.database_id, &tb, ix, None)
		.await
		.unwrap()
		.expect("index exists");
	tx.cancel().await.unwrap();
	(def.index_id, def.format_version)
}

/// A full-text, a b-tree and an HNSW index, each defined as `OVERWRITE` would
/// define it again.
const OVERWRITTEN: [(&str, &str); 3] = [
	("ft", "DEFINE INDEX OVERWRITE ft ON t FIELDS body FULLTEXT ANALYZER simple BM25"),
	("ix", "DEFINE INDEX OVERWRITE ix ON t FIELDS n"),
	("hv", "DEFINE INDEX OVERWRITE hv ON t FIELDS v HNSW DIMENSION 2"),
];

/// Defines [`OVERWRITTEN`]'s indexes over one record on `ds`.
async fn define_overwritten(ds: &Datastore) {
	run(
		ds,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
		 CREATE t:1 SET body = 'hello world', n = 1, v = [1.0, 2.0];",
	)
	.await;
	for (_, define) in OVERWRITTEN {
		run(ds, define).await;
	}
}

/// An unchanged `DEFINE INDEX OVERWRITE` on a store at the Legacy level keeps
/// every index: each was stamped with the capped format a rebuild there would
/// write again.
#[tokio::test]
async fn an_unchanged_overwrite_at_legacy_keeps_every_index() {
	let ds = upgraded().await;
	define_overwritten(&ds).await;
	for (ix, define) in OVERWRITTEN {
		let before = identity(&ds, ix).await;
		assert_eq!(before.1, LEGACY_LEVEL_INDEX_FORMAT_VERSION, "{ix}");
		run(&ds, define).await;
		assert_eq!(identity(&ds, ix).await, before, "{ix} was rebuilt");
	}
}

/// Defining a spatial index is refused while a store is held at the Legacy
/// feature level (`3.x`), because `3.x` nodes cannot decode or maintain
/// `Index::Spatial`. Once the store's feature level is raised (or on a store
/// created at `4.0+`), defining the spatial index succeeds, stamps
/// `INDEX_FORMAT_VERSION`, and plans spatial index scans.
#[tokio::test]
async fn a_spatial_index_is_refused_at_legacy_and_admitted_once_levelled() {
	let legacy_ds = upgraded().await;
	let levelled_ds = levelled().await;
	let ses = Session::owner().with_ns("test").with_db("test");

	run(
		&legacy_ds,
		"DEFINE NAMESPACE IF NOT EXISTS test; \
		 DEFINE DATABASE IF NOT EXISTS test; \
		 CREATE t:1 SET loc = (0.5, 0.5); \
		 CREATE t:2 SET loc = (50.0, 50.0);",
	)
	.await;
	let mut legacy_res =
		legacy_ds.execute("DEFINE INDEX sp ON t FIELDS loc SPATIAL;", &ses, None).await.unwrap();
	let legacy_err = legacy_res
		.pop()
		.unwrap()
		.result
		.expect_err("DEFINE INDEX ... SPATIAL at Legacy level must be refused");
	assert!(
		legacy_err.to_string().contains("feature level 4.0"),
		"unexpected error at Legacy level: {legacy_err}"
	);

	legacy_ds.raise_feature_level(crate::kvs::Release::FIRST_STORED).await.unwrap();

	for (label, ds) in [("raised", &legacy_ds), ("levelled", &levelled_ds)] {
		run(
			ds,
			"DEFINE NAMESPACE IF NOT EXISTS test; \
			 DEFINE DATABASE IF NOT EXISTS test; \
			 UPSERT t:1 SET loc = (0.5, 0.5); \
			 UPSERT t:2 SET loc = (50.0, 50.0); \
			 DEFINE INDEX sp ON t FIELDS loc SPATIAL;",
		)
		.await;
		assert_eq!(stamped(ds, "sp").await, INDEX_FORMAT_VERSION, "{label} stamp");
		let query = "LET $box = { type: 'Polygon', coordinates: [[[0,0],[1,0],[1,1],[0,1],[0,0]]] }; \
			SELECT VALUE id FROM t WHERE loc INSIDE $box; \
			SELECT VALUE id FROM t WHERE loc INSIDE $box EXPLAIN;";
		let mut res = ds.execute(query, &ses, None).await.unwrap();
		let explain = format!("{:?}", res.pop().unwrap().result.unwrap());
		let rows = format!("{:?}", res.pop().unwrap().result.unwrap());
		assert!(
			explain.contains("UnionIndexScan") || explain.contains("IndexScan"),
			"{label} store fell back to TableScan instead of spatial index scan: {explain}"
		);
		assert!(!explain.contains("TableScan"), "{label} store planned TableScan: {explain}");
		assert!(rows.contains("Number(1)") && !rows.contains("Number(2)"), "{label} rows: {rows}");
	}
	legacy_ds.shutdown().await.unwrap();
	levelled_ds.shutdown().await.unwrap();
}

/// Once the store is raised, an unchanged `DEFINE INDEX OVERWRITE` keeps an
/// index whose layout the current format leaves as it is (the b-tree index), and
/// rebuilds the full-text and HNSW indexes, whose layouts it changes.
#[tokio::test]
async fn an_unchanged_overwrite_after_a_raise_rebuilds_only_the_changed_layouts() {
	let ds = upgraded().await;
	define_overwritten(&ds).await;
	ds.raise_feature_level(crate::kvs::Release::FIRST_STORED).await.unwrap();
	for (ix, define) in OVERWRITTEN {
		let before = identity(&ds, ix).await;
		run(&ds, define).await;
		let after = identity(&ds, ix).await;
		if ix == "ft" || ix == "hv" {
			assert_ne!(after.0, before.0, "{ix} was kept");
			assert_eq!(
				after.1, INDEX_FORMAT_VERSION,
				"the rebuild of {ix} stamped the current format"
			);
		} else {
			assert_eq!(after, before, "{ix} was rebuilt");
		}
	}
}

async fn run(ds: &Datastore, sql: &str) {
	let ses = Session::owner().with_ns("test").with_db("test");
	for response in ds.execute(sql, &ses, None).await.unwrap() {
		response.result.unwrap_or_else(|e| panic!("{sql}: {e}"));
	}
}

/// Defines and rebuilds a full-text and a b-tree index on `ds`, and answers each stamp.
async fn stamps(ds: &Datastore) -> [u16; 4] {
	run(
		ds,
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
		 DEFINE INDEX ft ON t FIELDS body FULLTEXT ANALYZER simple BM25; \
		 DEFINE INDEX ix ON t FIELDS n; \
		 CREATE t:1 SET body = 'hello world', n = 1;",
	)
	.await;
	let defined = [stamped(ds, "ft").await, stamped(ds, "ix").await];
	run(ds, "REBUILD INDEX ft ON t; REBUILD INDEX ix ON t;").await;
	[defined[0], defined[1], stamped(ds, "ft").await, stamped(ds, "ix").await]
}

#[tokio::test]
async fn a_legacy_store_stamps_no_format_above_its_cap() {
	let ds = upgraded().await;
	let cap = INDEX_FORMAT_VERSION.min(LEGACY_LEVEL_INDEX_FORMAT_VERSION);
	assert_eq!(stamps(&ds).await, [cap; 4], "defined ft, ix; rebuilt ft, ix");
	ds.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_levelled_store_stamps_the_current_format() {
	let ds = levelled().await;
	assert_eq!(stamps(&ds).await, [INDEX_FORMAT_VERSION; 4], "defined ft, ix; rebuilt ft, ix");
	ds.shutdown().await.unwrap();
}

/// What every check of the rebuild test asks: each document's score, a
/// ranked top-k with and without the score threshold the scan rejects hits by,
/// a sum of two clauses' scores over their `OR`, and highlights.
const ANSWERS: &str = "SELECT id, search::score(1) AS s FROM t WHERE body @1@ 'hello' ORDER BY id; \
	SELECT id, search::score(1) AS s FROM t WHERE body @1@ 'hello' ORDER BY s DESC LIMIT 4; \
	SELECT id, search::score(1) AS s FROM t WHERE body @1@ 'hello world' ORDER BY s DESC LIMIT 3; \
	SELECT id, search::score(1) + search::score(2) AS s FROM t \
		WHERE body @1@ 'hello' OR body @2@ 'world' ORDER BY s DESC LIMIT 4; \
	SELECT id, search::highlight('<b>', '</b>', 1) AS h FROM t WHERE body @1@ 'hello world' ORDER BY id;";

/// The answers of [`ANSWERS`] on `ds`, one rendering per statement.
async fn answers(ds: &Datastore) -> Vec<String> {
	let ses = Session::owner().with_ns("test").with_db("test");
	ds.execute(ANSWERS, &ses, None)
		.await
		.unwrap()
		.into_iter()
		.map(|response| format!("{:?}", response.result.unwrap_or_else(|e| panic!("{e}"))))
		.collect()
}

/// Folds every pending index change on `ds`.
async fn compact(ds: &Arc<Datastore>) {
	Datastore::index_compaction(
		Arc::clone(ds),
		std::time::Duration::from_secs(1),
		tokio_util::sync::CancellationToken::new(),
	)
	.await
	.unwrap();
}

/// A full-text index an upgraded store keeps in the bitmap format, rebuilt once
/// the store's level is raised, keeps its posting blocks and answers every
/// score, ranking and highlight as a store that stayed on the bitmap format
/// does: right after the rebuild, with writes since left uncompacted, and once
/// they are folded.
#[tokio::test]
async fn a_rebuild_onto_posting_blocks_answers_as_the_bitmap_index() {
	use crate::catalog::{
		FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION, LEGACY_LEVEL_INDEX_FORMAT_VERSION,
	};

	let mut setup = String::from(
		"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
		 DEFINE TABLE t SCHEMALESS; \
		 DEFINE INDEX ft ON t FIELDS body FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;",
	);
	for i in 0..300 {
		let body = match i % 5 {
			0 => format!("hello{}", " hello".repeat(i % 4)),
			1 => format!("hello world{}", " x".repeat(i % 9)),
			2 => format!("world{}", " world".repeat(i % 3)),
			3 => format!("hello there{}", " y".repeat(i % 6)),
			_ => "filler".to_string(),
		};
		setup.push_str(&format!(" CREATE t:{i} SET body = '{body}';"));
	}
	let writes = "CREATE t:1000 SET body = 'hello hello hello world'; \
		UPDATE t:0 SET body = 'world'; UPDATE t:6 SET body = 'hello hello world world'; \
		DELETE t:11; DELETE t:15;";

	// The store that moves onto posting blocks, and the one that stays.
	let (moved, kept) = (upgraded().await, upgraded().await);
	for ds in [&moved, &kept] {
		run(ds, &setup).await;
		compact(ds).await;
		assert_eq!(stamped(ds, "ft").await, LEGACY_LEVEL_INDEX_FORMAT_VERSION);
	}
	let before = answers(&kept).await;
	assert_eq!(answers(&moved).await, before, "the two stores before the rebuild");

	moved.raise_feature_level(crate::kvs::Release::FIRST_STORED).await.unwrap();
	run(&moved, "REBUILD INDEX ft ON t").await;
	compact(&moved).await;
	assert_eq!(stamped(&moved, "ft").await, FULLTEXT_POSTING_BLOCKS_FORMAT_VERSION);
	assert_eq!(answers(&moved).await, before, "right after the rebuild");

	for ds in [&moved, &kept] {
		run(ds, writes).await;
	}
	assert_eq!(answers(&moved).await, answers(&kept).await, "with the writes uncompacted");
	for ds in [&moved, &kept] {
		compact(ds).await;
	}
	assert_eq!(answers(&moved).await, answers(&kept).await, "with the writes folded");
	moved.shutdown().await.unwrap();
	kept.shutdown().await.unwrap();
}
