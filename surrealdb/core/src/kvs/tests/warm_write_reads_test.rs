//! What a warm write reads of the catalog. A write to an indexed table, once
//! the catalog epoch holds its definitions, its index shapes and what a delete
//! checks of the database's references, reads none of them from the store; a
//! delete reads the record's doc-ID mapping once.

#![cfg(feature = "kv-mem")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use super::latency::{Class, LatencyComposer, LatencyStats};
use crate::dbs::Session;
use crate::kvs::Datastore;

const SETUP: &str = "
	DEFINE TABLE person SCHEMALESS;
	DEFINE INDEX age_idx ON person FIELDS age;
	DEFINE INDEX email_uniq ON person FIELDS email UNIQUE;
	DEFINE TABLE refd SCHEMALESS;
	DEFINE FIELD target ON refd TYPE record<person> REFERENCE;
	FOR $i IN 0..20 {
		CREATE type::record('person', $i) SET age = $i, email = string::concat('p', $i, '@x.io');
	};
";

async fn run(ds: &Datastore, ses: &Session, sql: &str) {
	for r in ds.execute(sql, ses, None).await.unwrap() {
		r.result.unwrap();
	}
}

/// The traced keys of `sql` run once warm, as `(class, key)`; `setup` runs before each
/// run, outside the window.
async fn trace_of(
	ds: &Datastore,
	ses: &Session,
	stats: &Arc<LatencyStats>,
	setup: &str,
	sql: &str,
) -> Vec<(Class, String)> {
	// Run once with the window closed to warm the catalog and statement caches.
	run(ds, ses, setup).await;
	run(ds, ses, sql).await;
	run(ds, ses, setup).await;
	stats.reset();
	stats.set_enabled(true);
	stats.set_tracing(true);
	run(ds, ses, sql).await;
	stats.set_enabled(false);
	stats.take_trace().into_iter().map(|e| (e.class, e.key)).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_warm_write_reads_no_catalog_range_and_no_index_shape() {
	let stats = LatencyStats::new();
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_factory_path("memory", LatencyComposer(Arc::clone(&stats)))
		.await
		.unwrap();
	let ses = Session::owner().with_ns("t").with_db("t");
	ds.execute("DEFINE NAMESPACE t; DEFINE DATABASE t;", &ses, None).await.unwrap();
	run(&ds, &ses, SETUP).await;

	let statements = [
		("create", "DELETE person:100", "CREATE person:100 SET age = 1, email = 'n@x.io'"),
		("update", "", "UPDATE person:5 SET age += 1"),
		("delete", "CREATE person:200 SET age = 1, email = 'd@x.io'", "DELETE person:200"),
	];
	for (name, setup, sql) in statements {
		let trace = trace_of(&ds, &ses, &stats, setup, sql).await;
		for (class, key) in &trace {
			for catalog in ["!tb", "!cv", "!fd", "!it", "!ia", "!ir"] {
				assert!(
					!key.contains(catalog) || matches!(class, Class::BlindWrite),
					"{name}: a warm write reads `{catalog}`: {class:?} {key}"
				);
			}
		}
	}

	let create = "CREATE person:300 SET age = 1, email = 'm@x.io'";
	let trace = trace_of(&ds, &ses, &stats, create, "DELETE person:300").await;
	let mappings = trace.iter().filter(|(c, k)| *c == Class::Get && k.contains("!dj")).count();
	assert!(mappings <= 1, "a delete reads the record's doc-ID mapping at most once: {trace:?}");
}
