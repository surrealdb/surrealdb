//! The batches of an index build's initial scan commit without a sync of their own.
//!
//! A build commits one transaction per batch of records, and its scan, run one
//! batch at a time for most kinds, waits for each commit. A sync per batch made
//! the build wait for the disk once per batch, which background compaction
//! slows; the commit that completes the scan syncs, and makes the batches
//! before it durable with it. The table here is large enough for several
//! batches, and the build must commit more transactions than it syncs.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use super::latency::{Class, LatencyComposer, LatencyStats};
use crate::dbs::Session;
use crate::kvs::Datastore;

/// Records of the table, each with a field large enough that a batch, bounded
/// by its bytes, holds a few hundred of them.
const RECORDS: usize = 2000;

async fn run(ds: &Datastore, ses: &Session, sql: &str) {
	for r in ds.execute(sql, ses, None).await.unwrap() {
		if let Err(e) = r.result {
			panic!("{sql}: {e:?}");
		}
	}
}

/// The commits, and the synced commits, of building the index `define` defines.
async fn build_commits(define: &str) -> (u64, u64) {
	let stats = LatencyStats::new();
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.build_with_factory_path("memory", LatencyComposer(Arc::clone(&stats)))
		.await
		.unwrap();
	let ses = Session::owner().with_ns("t").with_db("t");
	run(&ds, &ses, "DEFINE NAMESPACE t; DEFINE DATABASE t;").await;
	run(
		&ds,
		&ses,
		&format!(
			"FOR $i IN 1..={RECORDS} {{ CREATE doc SET n = $i, words = 'hello world', \
			 pad = rand::string(16384) }};"
		),
	)
	.await;
	run(&ds, &ses, "DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;").await;
	stats.set_enabled(true);
	run(&ds, &ses, define).await;
	stats.set_enabled(false);
	let commits = stats.snapshot().class(Class::Commit);
	(commits, stats.synced_commits())
}

fn assert_batches_unsynced(kind: &str, (commits, synced): (u64, u64)) {
	assert!(
		commits >= synced + 3,
		"{kind}: {commits} commits, {synced} of them synced: the batches of the scan must \
		 commit without a sync of their own"
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn idx_build_batches_commit_unsynced() {
	assert_batches_unsynced("idx", build_commits("DEFINE INDEX ix ON doc FIELDS n;").await);
}

#[tokio::test(flavor = "multi_thread")]
async fn fulltext_build_batches_commit_unsynced() {
	assert_batches_unsynced(
		"fulltext",
		build_commits("DEFINE INDEX ft ON doc FIELDS words FULLTEXT ANALYZER simple BM25;").await,
	);
}
