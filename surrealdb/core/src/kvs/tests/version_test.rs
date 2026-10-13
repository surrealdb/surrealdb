//! The startup version bootstrap: what a datastore carrying no `!v` means, and
//! what it takes for a start to settle one.
//!
//! These drive a real `Datastore` through [`Datastore::check_version`] and
//! [`Datastore::get_version`], so they live here rather than beside the version
//! value type: the decision under test is about what else is in storage, not
//! about how a version encodes.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use surrealdb_kvs::TransactionType::Read;

use crate::key::KVKey;
use crate::key::schema::{BootstrapKey, NodePrefix, VersionKey};
use crate::kvs::testing::{NonRetryableErrorSite, inject_non_retryable_errors};
use crate::kvs::version::MajorVersion;
use crate::kvs::{Datastore, DatastoreError};

/// Built without maintenance tasks so the only writes in these tests are the
/// ones they make themselves: a background task landing a key mid-test is the
/// very condition under examination, and it has to be staged deliberately.
async fn ds() -> Arc<Datastore> {
	Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap()
}

/// Stands in for a writer that reached storage before the version bootstrap
/// did — the node-membership refresh is the one that does this in production,
/// blindly upserting this node's row on its own cadence.
async fn write_node_row(ds: &Datastore) {
	ds.update_node().await.unwrap();
}

async fn stored_major(ds: &Datastore) -> Option<MajorVersion> {
	let txn = ds.transaction(Read).await.unwrap();
	let version = txn.get_key(&VersionKey {}, None).await.unwrap();
	txn.cancel().await.unwrap();
	version
}

async fn stored_sentinel(ds: &Datastore) -> Option<MajorVersion> {
	let txn = ds.transaction(Read).await.unwrap();
	let sentinel = txn.get_key(&BootstrapKey {}, None).await.unwrap();
	txn.cancel().await.unwrap();
	sentinel
}

/// The cluster node rows, which the node-membership refresh writes on its own
/// cadence and is therefore the first evidence that the schedule is running.
async fn count_node_rows(ds: &Datastore) -> usize {
	let txn = ds.transaction(Read).await.unwrap();
	let count = txn.count(NodePrefix {}.range().unwrap(), None).await.unwrap();
	txn.cancel().await.unwrap();
	count
}

/// The sentinel has to sort below the range `get_version` scans when it asks
/// whether anything is in storage, or claiming a datastore would be the very
/// thing that made it look like it already held data of unknown provenance.
///
/// Asserted on the encoded bytes rather than on the behaviour that depends on
/// them, because the ordering is a property of the keyspace: a later edit that
/// renamed the route would break it without breaking any single flow.
#[test]
fn the_sentinel_sorts_below_the_emptiness_probe() {
	let sentinel = BootstrapKey {}.encode_key().unwrap();
	let version = VersionKey {}.encode_key().unwrap();
	assert!(
		sentinel < version,
		"the bootstrap sentinel {sentinel:?} does not sort before the version key {version:?}, \
		 so the emptiness probe would count it as data"
	);
}

/// A bootstrap interrupted after its sentinel landed is finished by the next
/// start, at this build's version, even though something else reached storage
/// in between.
///
/// This is the whole point of the sentinel. Without it the next start sees a
/// datastore holding keys and no `!v`, which is indistinguishable from storage
/// written before `!v` existed, and stamps it `v1` — permanently, because that
/// stamp is committed and every later start reads it back.
#[tokio::test]
async fn an_interrupted_bootstrap_is_completed_rather_than_refused() {
	let ds = ds().await;

	// Interrupt the bootstrap between committing the sentinel and stamping `!v`.
	let guard =
		inject_non_retryable_errors(NonRetryableErrorSite::VersionBootstrapStamp, ds.id(), 1);
	ds.get_version().await.unwrap_err();
	drop(guard);

	assert_eq!(
		stored_sentinel(&ds).await,
		Some(MajorVersion::latest()),
		"the interrupted bootstrap left behind no sentinel recording the version it targeted"
	);
	assert_eq!(stored_major(&ds).await, None, "the interrupted bootstrap stamped a version");

	// Something else reaches storage before the next start does.
	write_node_row(&ds).await;

	let (version, is_new) = ds.check_version().await.unwrap();
	assert_eq!(version, MajorVersion::latest());
	assert!(is_new, "the start that completed the initialisation did not report creating it");
	assert_eq!(stored_major(&ds).await, Some(MajorVersion::latest()));
	assert_eq!(stored_sentinel(&ds).await, None, "the sentinel outlived the stamp that retires it");
}

/// Completing an interrupted bootstrap is idempotent: a start that finds `!v`
/// already stamped reports the datastore as existing, not as one it created.
#[tokio::test]
async fn completing_an_interrupted_bootstrap_only_creates_the_datastore_once() {
	let ds = ds().await;

	let guard =
		inject_non_retryable_errors(NonRetryableErrorSite::VersionBootstrapStamp, ds.id(), 1);
	ds.get_version().await.unwrap_err();
	drop(guard);

	assert!(ds.get_version().await.unwrap().1, "the resuming start did not report creating it");
	assert_eq!(
		ds.get_version().await.unwrap(),
		(MajorVersion::latest(), false),
		"a start against a settled datastore reported creating it"
	);
}

/// Storage holding keys but neither `!v` nor a sentinel is still refused.
///
/// `!v` arrived in 2.0, so this shape is exactly what a datastore last written
/// before then looks like, and such a datastore does need the upgrade path. The
/// sentinel exists because nothing else in the bytes separates that from an
/// interrupted bootstrap; where it is absent, refusing remains the only safe
/// reading, and this pins that the fix for one did not swallow the other.
#[tokio::test]
async fn storage_with_no_sentinel_is_still_refused_as_out_of_date() {
	let ds = ds().await;
	write_node_row(&ds).await;

	let err = ds.check_version().await.unwrap_err();
	let Some(DatastoreError::OutdatedStorageVersion {
		expected,
		actual,
	}) = err.downcast_ref::<DatastoreError>()
	else {
		panic!("a datastore holding pre-versioning data was not refused: {err}");
	};
	assert_eq!(*expected, u16::from(MajorVersion::latest()));
	assert_eq!(*actual, u16::from(MajorVersion::v1()));
}

/// A storage layer that will not serve the bootstrap a transaction is waited
/// out, not treated as fatal.
///
/// The refusal a distributed backend raises while a view change is in flight is
/// not classified as a retryable conflict, so the conflict-only retry the rest
/// of startup uses returns it on the first attempt. It clears on its own within
/// seconds, and a start that gives up on it is a start that failed for no
/// durable reason.
#[tokio::test(start_paused = true)]
async fn the_bootstrap_waits_out_a_storage_layer_that_will_not_serve_it() {
	let ds = ds().await;
	// More refusals than the conflict-only retry would tolerate — it tolerates
	// none of them — and few enough that the budget absorbs them comfortably.
	let _guard =
		inject_non_retryable_errors(NonRetryableErrorSite::VersionBootstrapBegin, ds.id(), 6);

	let (version, is_new) = ds.check_version().await.unwrap();
	assert_eq!(version, MajorVersion::latest());
	assert!(is_new);
	assert_eq!(stored_major(&ds).await, Some(MajorVersion::latest()));
}

/// The retry has a floor as well as a ceiling: a failure that never clears is
/// reported rather than waited on forever.
#[tokio::test(start_paused = true)]
async fn the_bootstrap_gives_up_once_its_budget_is_spent() {
	let ds = ds().await;
	// Far more refusals than the budget can absorb at the capped back-off.
	let _guard =
		inject_non_retryable_errors(NonRetryableErrorSite::VersionBootstrapBegin, ds.id(), 100_000);

	ds.check_version().await.unwrap_err();
}

/// Maintenance tasks a caller asked the builder not to start stay stopped until
/// that caller starts them, and starting them twice does not double them up.
///
/// This is what lets the server put the whole maintenance schedule after
/// `check_version`: every one of those tasks writes, and a write that reaches a
/// datastore before its version is stamped is what makes a brand-new one read
/// as storage from before `!v` existed.
#[tokio::test]
async fn held_back_maintenance_tasks_write_nothing_until_they_are_started() {
	// The refresh interval is short enough that a running task would have
	// written many times over by the time the assertion below runs.
	let options = crate::options::EngineOptions {
		node_membership_refresh_interval: Duration::from_millis(1),
		..Default::default()
	};
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_engine_options(options)
		.build_with_path("memory")
		.await
		.unwrap();

	tokio::time::sleep(Duration::from_millis(100)).await;
	assert_eq!(count_node_rows(&ds).await, 0, "a held-back maintenance task wrote to storage");

	ds.start_maintenance_tasks();
	// Idempotent: the second call must not start a second set of tasks.
	ds.start_maintenance_tasks();

	let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
	while count_node_rows(&ds).await == 0 && tokio::time::Instant::now() < deadline {
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
	assert!(count_node_rows(&ds).await > 0, "the started maintenance tasks never wrote");

	ds.shutdown().await.unwrap();
}

/// The sentinel is written and retired inside the bootstrap, so a settled
/// datastore carries no trace of it and repeated starts do not rewrite it.
#[tokio::test]
async fn a_settled_datastore_carries_no_sentinel() {
	let ds = ds().await;
	ds.check_version().await.unwrap();
	assert_eq!(stored_sentinel(&ds).await, None);

	for _ in 0..3 {
		ds.check_version().await.unwrap();
		assert_eq!(stored_sentinel(&ds).await, None, "a settled start rewrote the sentinel");
	}

	// And the datastore is still stamped at this build, not at anything the
	// repeated starts inferred.
	assert_eq!(stored_major(&ds).await, Some(MajorVersion::latest()));
}

/// A marker this build cannot decode is corruption, not a storage layer that is
/// still coming up, so it is reported straight away rather than waited on.
///
/// The bootstrap retry is deliberately indiscriminate about storage failures,
/// because the refusals it exists to outlast are not distinguishable by class.
/// A malformed marker is the one failure `get_version` can raise that no amount
/// of waiting resolves, and burning the budget on it costs an embedder the wait
/// and costs the server the error itself: its own shorter ceiling fires first
/// and reports a timeout, which names nothing an operator can act on.
#[tokio::test(start_paused = true)]
async fn a_malformed_marker_is_reported_rather_than_waited_on() {
	let ds = ds().await;
	let txn = ds.transaction(surrealdb_kvs::TransactionType::Write).await.unwrap();
	txn.set(VersionKey {}.encode_key().unwrap(), vec![0xff]).await.unwrap();
	txn.commit().await.unwrap();

	let started = tokio::time::Instant::now();
	let err = ds.check_version().await.unwrap_err();
	let waited = started.elapsed();

	assert!(
		matches!(err.downcast_ref::<DatastoreError>(), Some(DatastoreError::InvalidStorageVersion)),
		"a malformed marker was not reported as one: {err}"
	);
	assert!(waited < Duration::from_secs(1), "a malformed marker was retried for {waited:?}");
}

/// A bootstrap begun by a build with a higher major version is left for that
/// build to finish, rather than being completed at this one's.
///
/// Completing it here would stamp the datastore *down* a major version, which
/// both lets this build past its own gate against bytes a newer build wrote and
/// leaves the newer build refusing the stamp it would then read back. Refusing
/// without writing keeps the sentinel intact, so the build that started the job
/// can still finish it.
#[tokio::test]
async fn a_bootstrap_begun_by_a_newer_build_is_left_for_that_build() {
	let ds = ds().await;
	let newer = MajorVersion::from(u16::from(MajorVersion::latest()) + 1);
	let txn = ds.transaction(surrealdb_kvs::TransactionType::Write).await.unwrap();
	txn.set_key(&BootstrapKey {}, &newer).await.unwrap();
	txn.commit().await.unwrap();

	let err = ds.check_version().await.unwrap_err();
	let Some(DatastoreError::OutdatedStorageVersion {
		expected,
		actual,
	}) = err.downcast_ref::<DatastoreError>()
	else {
		panic!("a bootstrap begun by a newer build was not refused: {err}");
	};
	assert_eq!(*expected, u16::from(MajorVersion::latest()));
	assert_eq!(*actual, u16::from(newer));

	assert_eq!(stored_major(&ds).await, None, "this build stamped a datastore it cannot complete");
	assert_eq!(
		stored_sentinel(&ds).await,
		Some(newer),
		"the sentinel the newer build needs to resume was retired by this one"
	);
}

/// A node that claimed an empty datastore and then found another node had
/// finished the job reports the datastore as existing, not as one it created.
///
/// Two nodes starting against the same empty backend both take the claim path;
/// whichever commits its sentinel first can have the other observe it and
/// complete the bootstrap before the first gets to stamp. `is_new` drives
/// default-namespace creation, so both reporting it has both of them running
/// `DEFINE NAMESPACE` and the loser failing startup on a namespace that already
/// exists.
#[tokio::test]
async fn a_bootstrap_another_node_finished_is_not_reported_as_created_here() {
	let ds = ds().await;
	// The state this node's second transaction opens into when a peer resumed
	// its claim and stamped the datastore first.
	let txn = ds.transaction(surrealdb_kvs::TransactionType::Write).await.unwrap();
	txn.set_key(&VersionKey {}, &MajorVersion::latest()).await.unwrap();
	txn.commit().await.unwrap();

	assert_eq!(
		ds.finish_bootstrap(MajorVersion::latest()).await.unwrap(),
		(MajorVersion::latest(), false),
		"a node reported creating a datastore another node had already stamped"
	);
}

/// A datastore built the default way carries its version marker before any
/// maintenance task exists to write, so the ordering does not depend on the
/// caller remembering it.
///
/// The heartbeat's first pass lands one refresh interval after construction, so
/// a caller that builds the default way and then delays `check_version` past
/// that interval used to race it — and the sentinel cannot recover a window it
/// has not yet landed in. Settling the marker in the branch that starts the
/// writers is what removes the race rather than narrowing it.
#[tokio::test]
async fn a_default_datastore_is_stamped_before_its_writers_start() {
	// Short enough that a maintenance write would land during the wait below.
	let options = crate::options::EngineOptions {
		node_membership_refresh_interval: Duration::from_millis(1),
		..Default::default()
	};
	let ds =
		Datastore::builder().with_engine_options(options).build_with_path("memory").await.unwrap();

	assert_eq!(
		stored_major(&ds).await,
		Some(MajorVersion::latest()),
		"a default-built datastore was handed over with no version marker"
	);

	// Let the writers run, then confirm the gate still passes: what they wrote
	// cannot be read as storage from before the marker, because the marker
	// predates all of it.
	tokio::time::sleep(Duration::from_millis(50)).await;
	assert!(count_node_rows(&ds).await > 0, "the maintenance tasks never wrote");
	assert_eq!(ds.check_version().await.unwrap().0, MajorVersion::latest());

	ds.shutdown().await.unwrap();
}

/// The datastore still reports that *this process* created it, even though the
/// marker was written during construction rather than by `check_version`.
///
/// `is_new` is what drives default-namespace creation, and `get_version` can
/// only report it on the call that wrote the marker — which is now the
/// builder's. The latch is what carries it across to the caller.
#[tokio::test]
async fn a_default_datastore_still_reports_that_this_process_created_it() {
	let ds = Datastore::builder().build_with_path("memory").await.unwrap();

	assert!(
		ds.check_version().await.unwrap().1,
		"the process that created the datastore did not report creating it"
	);

	ds.shutdown().await.unwrap();
}
