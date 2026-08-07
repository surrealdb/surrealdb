use super::*;

/// Ledger ids must be unique and ascending, because a datastore that has
/// already applied id N will skip whatever migration N later becomes.
#[test]
fn migration_ids_are_unique_and_ascending() {
	let mut previous: Option<u32> = None;
	for m in MIGRATIONS {
		if let Some(previous) = previous {
			assert!(
				m.id > previous,
				"migration `{}` has id {} which does not follow {previous}",
				m.name,
				m.id
			);
		}
		previous = Some(m.id);
	}
}

/// A migration declared for a version this build predates could never run, so
/// it is always a mistake.
#[test]
fn no_migration_is_declared_ahead_of_this_build() {
	let current = StorageVersion::current().triple();
	for m in MIGRATIONS {
		assert!(
			m.version <= current,
			"migration `{}` is declared for {:?}, ahead of this build's {current:?}",
			m.name,
			m.version
		);
	}
}

#[cfg(feature = "kv-mem")]
mod datastore {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicU32, Ordering};

	use super::*;
	use crate::key::KVKey;
	use crate::key::schema::{MigrationKey, VersionKey};
	use crate::kvs::version::MajorVersion;
	use crate::kvs::{Datastore, TransactionType};

	async fn mem() -> Arc<Datastore> {
		Datastore::new("memory").await.unwrap()
	}

	/// Stands in for a datastore last touched by the version named: both the
	/// legacy major-version key and the semantic version stamp, because a
	/// datastore holding data with no major version reads as a v1 store.
	async fn stamp(ds: &Datastore, version: &str) {
		let txn = ds.transaction(TransactionType::Write).await.unwrap();
		txn.set_key(&VersionKey {}, &MajorVersion::latest()).await.unwrap();
		txn.set_key(&StorageVersionKey {}, &StorageVersion::parse(version).unwrap()).await.unwrap();
		txn.commit().await.unwrap();
	}

	/// A brand-new datastore is stamped at the running version, which is what
	/// keeps historical migrations from ever being candidates for it.
	#[tokio::test]
	async fn a_new_datastore_is_stamped_at_the_current_version() {
		let ds = mem().await;
		ds.check_version().await.unwrap();

		let stamp = ds.storage_version().await.unwrap().expect("a new datastore is stamped");
		assert_eq!(stamp, StorageVersion::current());

		let history = ds.version_history().await.unwrap();
		assert_eq!(history.len(), 1);
		assert_eq!(history[0].from, None);
		assert_eq!(history[0].to, StorageVersion::current());
		assert_eq!(history[0].node, ds.id());
	}

	/// Starting repeatedly records one transition, not one entry per startup.
	#[tokio::test]
	async fn restarting_does_not_append_to_the_history() {
		let ds = mem().await;
		ds.check_version().await.unwrap();
		ds.check_version().await.unwrap();
		ds.check_version().await.unwrap();

		assert_eq!(ds.version_history().await.unwrap().len(), 1);
	}

	/// The legacy major-version key keeps being written, so a node older than
	/// 3.3 can still read the datastore.
	#[tokio::test]
	async fn the_legacy_major_version_key_is_still_written() {
		let ds = mem().await;
		ds.check_version().await.unwrap();

		let txn = ds.transaction(TransactionType::Read).await.unwrap();
		let major = txn.get_key(&VersionKey {}, None).await.unwrap();
		txn.cancel().await.unwrap();
		assert_eq!(major, Some(MajorVersion::latest()));
	}

	/// A datastore carrying data but no stamp predates 3.3, and is advanced
	/// with the transition recorded as coming from nowhere.
	#[tokio::test]
	async fn an_unstamped_datastore_is_advanced() {
		let ds = mem().await;
		stamp_nothing_but_data(&ds).await;

		ds.check_version().await.unwrap();

		assert_eq!(ds.storage_version().await.unwrap(), Some(StorageVersion::current()));
		let history = ds.version_history().await.unwrap();
		assert_eq!(history.len(), 1);
		assert_eq!(history[0].from, None);
	}

	/// Advancing an already-stamped datastore records where it came from.
	#[tokio::test]
	async fn advancing_records_the_previous_version() {
		let ds = mem().await;
		stamp(&ds, "3.2.4").await;

		ds.check_version().await.unwrap();

		let history = ds.version_history().await.unwrap();
		assert_eq!(history.len(), 1);
		assert_eq!(history[0].from, Some(StorageVersion::parse("3.2.4").unwrap()));
		assert_eq!(history[0].to, StorageVersion::current());
	}

	/// The stamp tracks the release triple, so a rebuild that only changes the
	/// pre-release tag is not a transition and adds no history.
	#[tokio::test]
	async fn a_pre_release_of_the_same_triple_is_not_a_transition() {
		let ds = mem().await;
		let current = StorageVersion::current();
		stamp(&ds, &StorageVersion::from_triple(current.triple()).to_string()).await;

		ds.check_version().await.unwrap();

		assert!(ds.version_history().await.unwrap().is_empty());
	}

	/// The release triple, not semver's own ordering, decides everything.
	///
	/// Pinned in the direction where the two rules disagree: a datastore stamped
	/// with a *pre-release* started by the *release* it precedes. Under semver
	/// `3.4.0-nightly < 3.4.0`, so full ordering would call this an upgrade and
	/// re-run the gap; under the triple rule the two are the same release and
	/// nothing is owed.
	#[tokio::test]
	async fn the_release_triple_decides_not_semver_ordering() {
		static RUNS: AtomicU32 = AtomicU32::new(0);

		fn bump(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async {
				RUNS.fetch_add(1, Ordering::SeqCst);
				Ok(())
			})
		}

		let registry = [Migration {
			id: 9008,
			name: "declared at the release",
			version: (3, 4, 0),
			run: bump,
		}];

		// A datastore a nightly of 3.4.0 already migrated.
		let ds = mem().await;
		stamp_nothing_but_data(&ds).await;
		run_with(&ds, &registry, &StorageVersion::parse("3.4.0-nightly").unwrap(), false)
			.await
			.unwrap();
		assert_eq!(
			RUNS.load(Ordering::SeqCst),
			1,
			"a migration declared at 3.4.0 must run on a 3.4.0 pre-release; 			 semver ordering would exclude it"
		);
		assert_eq!(
			ds.storage_version().await.unwrap(),
			Some(StorageVersion::parse("3.4.0-nightly").unwrap())
		);

		// The release itself then starts. Same triple, so no transition and no
		// re-run — semver ordering would see 3.4.0 > 3.4.0-nightly and advance.
		run_with(&ds, &registry, &StorageVersion::parse("3.4.0").unwrap(), false).await.unwrap();
		assert_eq!(RUNS.load(Ordering::SeqCst), 1);
		assert_eq!(
			ds.version_history().await.unwrap().len(),
			1,
			"a pre-release and its release are one version; advancing between them 			 would record a transition that did not happen"
		);
	}

	/// Each migration is recorded as it completes, so a later failure does not
	/// undo the ones before it. This is the crash-resume contract: a restart
	/// picks up where the run stopped rather than starting over.
	#[tokio::test]
	async fn each_migration_is_recorded_before_the_next_one_runs() {
		static RUNS: AtomicU32 = AtomicU32::new(0);

		fn ok(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async {
				RUNS.fetch_add(1, Ordering::SeqCst);
				Ok(())
			})
		}
		fn boom(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async { Err(anyhow::anyhow!("second migration exploded")) })
		}

		let registry = [
			Migration {
				id: 9201,
				name: "first",
				version: (3, 3, 0),
				run: ok,
			},
			Migration {
				id: 9202,
				name: "second",
				version: (3, 3, 0),
				run: boom,
			},
		];
		let current = StorageVersion::parse("3.3.0").unwrap();

		let ds = mem().await;
		stamp_nothing_but_data(&ds).await;
		assert!(run_with(&ds, &registry, &current, false).await.is_err());

		// The first is durably applied even though the run aborted, and the
		// stamp did not advance.
		let recorded = ds.applied_migrations().await.unwrap();
		assert_eq!(recorded.len(), 1);
		assert_eq!(recorded[0].0, 9201);
		assert_eq!(ds.storage_version().await.unwrap(), None);

		// A restart owes only the second, and does not repeat the first.
		assert!(run_with(&ds, &registry, &current, false).await.is_err());
		assert_eq!(RUNS.load(Ordering::SeqCst), 1, "the applied migration ran again");
	}

	/// A ledger entry this build cannot decode still refuses the start, and does
	/// it with the message written for that case rather than a decode error.
	#[tokio::test]
	async fn an_unreadable_ledger_entry_still_names_the_unknown_migration() {
		let ds = mem().await;
		// Stands in for a record written by a later release in a revision this
		// build does not know.
		let txn = ds.transaction(TransactionType::Write).await.unwrap();
		txn.set(MigrationKey::new(9999).encode_key().unwrap(), vec![0xff, 0xff, 0xff])
			.await
			.unwrap();
		txn.commit().await.unwrap();

		let err =
			run_with(&ds, &[], &StorageVersion::parse("3.3.0").unwrap(), false).await.unwrap_err();
		let message = err.to_string();
		assert!(
			message.contains("id 9999") && message.contains("unreadable"),
			"unexpected message: {message}"
		);
	}

	/// The stamp only ever moves forward, so a rollback to an older build does
	/// not rewrite it downwards.
	#[tokio::test]
	async fn the_stamp_is_not_regressed_by_an_older_build() {
		let ds = mem().await;
		stamp(&ds, "99.0.0").await;

		// No migration separates 99.0.0 from this build, so startup is allowed.
		ds.check_version().await.unwrap();

		assert_eq!(
			ds.storage_version().await.unwrap(),
			Some(StorageVersion::parse("99.0.0").unwrap())
		);
		assert!(ds.version_history().await.unwrap().is_empty());
	}

	/// The end-to-end path: a pre-stamp datastore runs the migration, records
	/// it, advances the stamp, and does not run it again.
	#[tokio::test]
	async fn a_pending_migration_runs_once_and_is_recorded() {
		static RUNS: AtomicU32 = AtomicU32::new(0);

		fn bump(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async {
				RUNS.fetch_add(1, Ordering::SeqCst);
				Ok(())
			})
		}

		let registry = [Migration {
			id: 9001,
			name: "test migration",
			version: (3, 3, 0),
			run: bump,
		}];
		let current = StorageVersion::parse("3.3.0").unwrap();

		let ds = mem().await;
		// A datastore with data but no stamp, so the migration is in the gap.
		stamp_nothing_but_data(&ds).await;

		run_with(&ds, &registry, &current, false).await.unwrap();
		assert_eq!(RUNS.load(Ordering::SeqCst), 1);

		let recorded = ds.applied_migrations().await.unwrap();
		assert_eq!(recorded.len(), 1);
		assert_eq!(recorded[0].0, 9001);
		assert_eq!(recorded[0].1.name, "test migration");
		assert_eq!(recorded[0].1.version, StorageVersion::parse("3.3.0").unwrap());
		assert_eq!(recorded[0].1.node, ds.id());
		assert_eq!(ds.storage_version().await.unwrap(), Some(current.clone()));

		// A second pass changes nothing.
		run_with(&ds, &registry, &current, false).await.unwrap();
		assert_eq!(RUNS.load(Ordering::SeqCst), 1);
		assert_eq!(ds.applied_migrations().await.unwrap().len(), 1);
		assert_eq!(ds.version_history().await.unwrap().len(), 1);
	}

	/// The ledger, not the version stamp, is what makes a migration run once:
	/// a stamp rolled back to before the migration still does not re-run it.
	#[tokio::test]
	async fn the_ledger_alone_prevents_a_rerun() {
		static RUNS: AtomicU32 = AtomicU32::new(0);

		fn bump(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async {
				RUNS.fetch_add(1, Ordering::SeqCst);
				Ok(())
			})
		}

		let registry = [Migration {
			id: 9004,
			name: "ledger guarded",
			version: (3, 3, 0),
			run: bump,
		}];
		let current = StorageVersion::parse("3.3.0").unwrap();

		let ds = mem().await;
		stamp_nothing_but_data(&ds).await;
		run_with(&ds, &registry, &current, false).await.unwrap();
		assert_eq!(RUNS.load(Ordering::SeqCst), 1);

		// Simulate the crash window: the migration ran and was recorded, but the
		// stamp never advanced.
		let txn = ds.transaction(TransactionType::Write).await.unwrap();
		txn.del_key(&StorageVersionKey {}).await.unwrap();
		txn.commit().await.unwrap();

		run_with(&ds, &registry, &current, false).await.unwrap();
		assert_eq!(RUNS.load(Ordering::SeqCst), 1);
	}

	/// A brand-new datastore is not a candidate for historical migrations even
	/// when the registry contains them.
	#[tokio::test]
	async fn a_new_datastore_skips_historical_migrations() {
		static RUNS: AtomicU32 = AtomicU32::new(0);

		fn bump(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async {
				RUNS.fetch_add(1, Ordering::SeqCst);
				Ok(())
			})
		}

		let registry = [Migration {
			id: 9005,
			name: "should never run on a fresh store",
			version: (3, 3, 0),
			run: bump,
		}];

		let ds = mem().await;
		run_with(&ds, &registry, &StorageVersion::parse("3.3.0").unwrap(), true).await.unwrap();

		assert_eq!(RUNS.load(Ordering::SeqCst), 0);
		// Recorded as applied without running, which is what keeps it off the
		// candidate list on every later start.
		assert_eq!(ds.applied_migrations().await.unwrap().len(), 1);
		assert_eq!(
			ds.storage_version().await.unwrap(),
			Some(StorageVersion::parse("3.3.0").unwrap())
		);
	}

	/// A failing migration aborts the run, leaves no ledger entry, and does not
	/// advance the stamp, so the next start retries it.
	#[tokio::test]
	async fn a_failing_migration_aborts_startup_and_is_not_recorded() {
		fn boom(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async { Err(anyhow::anyhow!("migration exploded")) })
		}

		let registry = [Migration {
			id: 9002,
			name: "failing migration",
			version: (3, 3, 0),
			run: boom,
		}];

		let ds = mem().await;
		stamp_nothing_but_data(&ds).await;

		let err = run_with(&ds, &registry, &StorageVersion::parse("3.3.0").unwrap(), false)
			.await
			.unwrap_err();
		assert!(err.to_string().contains("migration exploded"));
		assert!(ds.applied_migrations().await.unwrap().is_empty());
		assert_eq!(ds.storage_version().await.unwrap(), None);
	}

	/// Migrations run in registry order, and only those at or below the running
	/// version run. The datastore's stamp is not a lower bound — a migration
	/// declared below it is still owed until the ledger says otherwise.
	#[tokio::test]
	async fn only_migrations_at_or_below_the_running_version_run_in_order() {
		static ORDER: std::sync::Mutex<Vec<u32>> = std::sync::Mutex::new(Vec::new());

		fn record<const ID: u32>(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async {
				ORDER.lock().unwrap().push(ID);
				Ok(())
			})
		}

		let registry = [
			Migration {
				id: 9101,
				name: "declared below the stamp",
				version: (3, 2, 0),
				run: record::<9101>,
			},
			Migration {
				id: 9102,
				name: "at the running version",
				version: (3, 3, 0),
				run: record::<9102>,
			},
			Migration {
				id: 9103,
				name: "also below the running version",
				version: (3, 3, 5),
				run: record::<9103>,
			},
			Migration {
				id: 9104,
				name: "after the build",
				version: (3, 9, 0),
				run: record::<9104>,
			},
		];

		let ds = mem().await;
		stamp(&ds, "3.2.4").await;
		run_with(&ds, &registry, &StorageVersion::parse("3.4.0").unwrap(), false).await.unwrap();

		// 9104 is declared above the running version, so it is not yet owed.
		assert_eq!(*ORDER.lock().unwrap(), vec![9101, 9102, 9103]);
	}

	/// A build that lacks a migration the datastore has already applied must
	/// refuse to start rather than read data it has no code for.
	///
	/// The evidence is the ledger, not the version stamp: a rolled-back-from
	/// migration is by definition absent from this build's registry, so no
	/// comparison against that registry could name it.
	#[tokio::test]
	async fn a_build_behind_an_applied_migration_refuses_to_start() {
		let ds = mem().await;
		let newer = [Migration {
			id: 9003,
			name: "reshapes the widgets",
			version: (3, 4, 0),
			run: |_| Box::pin(async { Ok(()) }),
		}];
		// A 3.4.0 build applies it and records it.
		stamp_nothing_but_data(&ds).await;
		run_with(&ds, &newer, &StorageVersion::parse("3.4.0").unwrap(), false).await.unwrap();

		// Rolling back to a build whose registry has never heard of it must fail.
		let older: [Migration; 0] = [];
		let err = run_with(&ds, &older, &StorageVersion::parse("3.3.5").unwrap(), false)
			.await
			.unwrap_err();
		let message = err.to_string();
		assert!(message.contains("reshapes the widgets"), "unexpected message: {message}");
	}

	/// A rollback with nothing unknown in the ledger is allowed, so a plain
	/// minor-version downgrade is not turned into a startup failure.
	#[tokio::test]
	async fn a_rollback_with_no_unknown_migration_is_allowed() {
		let ds = mem().await;
		stamp(&ds, "3.4.0").await;

		run_with(&ds, &[], &StorageVersion::parse("3.3.5").unwrap(), false).await.unwrap();

		// The stamp is not dragged backwards by the older build.
		assert_eq!(
			ds.storage_version().await.unwrap(),
			Some(StorageVersion::parse("3.4.0").unwrap())
		);
	}

	/// A migration declared for a release below the datastore's stamp still
	/// runs. A fix authored on a release branch carries that branch's version,
	/// so gating on the stamp would skip it forever.
	#[tokio::test]
	async fn a_migration_below_the_stamp_still_runs() {
		static RUNS: AtomicU32 = AtomicU32::new(0);

		fn bump(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async {
				RUNS.fetch_add(1, Ordering::SeqCst);
				Ok(())
			})
		}

		let ds = mem().await;
		// The store is already stamped past the migration's declared version.
		stamp(&ds, "3.4.0").await;
		let backported = [Migration {
			id: 9006,
			name: "backported fix",
			version: (3, 3, 7),
			run: bump,
		}];

		run_with(&ds, &backported, &StorageVersion::parse("3.4.1").unwrap(), false).await.unwrap();

		assert_eq!(RUNS.load(Ordering::SeqCst), 1);
		assert_eq!(ds.applied_migrations().await.unwrap().len(), 1);
	}

	/// A datastore this start created records its whole registry as applied, so
	/// an interrupted first start cannot turn it into an upgrade candidate.
	#[tokio::test]
	async fn a_new_datastore_records_a_durable_baseline() {
		static RUNS: AtomicU32 = AtomicU32::new(0);

		fn bump(_: &Datastore) -> MigrationFuture<'_> {
			Box::pin(async {
				RUNS.fetch_add(1, Ordering::SeqCst);
				Ok(())
			})
		}

		let registry = [Migration {
			id: 9007,
			name: "never runs on a fresh store",
			version: (3, 3, 0),
			run: bump,
		}];
		let current = StorageVersion::parse("3.3.0").unwrap();

		let ds = mem().await;
		run_with(&ds, &registry, &current, true).await.unwrap();
		assert_eq!(RUNS.load(Ordering::SeqCst), 0);
		assert_eq!(ds.applied_migrations().await.unwrap().len(), 1);

		// Simulate a crash before the stamp landed: the ledger is what carries
		// the baseline, so the migration must still not run.
		let txn = ds.transaction(TransactionType::Write).await.unwrap();
		txn.del_key(&StorageVersionKey {}).await.unwrap();
		txn.commit().await.unwrap();

		run_with(&ds, &registry, &current, false).await.unwrap();
		assert_eq!(RUNS.load(Ordering::SeqCst), 0);
	}

	/// Stands in for an upgraded datastore: real data, a major version, and no
	/// semantic version stamp.
	async fn stamp_nothing_but_data(ds: &Datastore) {
		let txn = ds.transaction(TransactionType::Write).await.unwrap();
		txn.set_key(&VersionKey {}, &MajorVersion::latest()).await.unwrap();
		txn.commit().await.unwrap();
	}
}
