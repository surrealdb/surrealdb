//! Copying one datastore's keyspace into another.
//!
//! A copy moves stored bytes, so it carries every key below `0xff`, declared or
//! not, without decoding any of them. What it needs from the engine is narrower:
//! which keys must land at a particular moment, and whether the keyspace is one
//! this build can start from at all. A source serves a copy over [`keyspace`],
//! placing each key by [`key_role`], and describes itself with
//! [`KeyspaceVersion::read`]. A target checks that description with
//! [`KeyspaceVersion::check`] before it writes its first key, and writes the keys
//! in the order [`KeyRole`] gives them.
//!
//! Those rules are what make a copy cut short safe to restart from. The version
//! gate in [`Datastore::check_version`] reads a datastore that holds keys but no
//! version key as one written before versioning existed, and refuses it. So a
//! target that writes the version key last is refused until the copy is whole,
//! rather than started from with keys missing. The bootstrap sentinel would undo
//! that: next to a missing version key it tells the gate that a bootstrap was
//! interrupted, and the gate finishes it by recording every data migration as
//! applied without running any. A copy therefore never carries it.
//!
//! This release records no feature level. A later release records one at `!fl`,
//! outside the `/` root, and a copy carries it like any other key when the source
//! holds it and carries nothing when it does not, so a target settles its own
//! feature level only after the copy, from what the copy left. A target whose
//! feature level was settled before the copy would keep its own record over data
//! the source wrote at another level.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::key::schema::{MigrationKey, MigrationPrefix, StorageVersionKey, VersionKey};
use crate::key::{KVKeyDecode, RawRange};
use crate::kvs::migration::{MIGRATIONS, unknown_migrations};
use crate::kvs::version::{MajorVersion, StorageVersion};
use crate::kvs::{Datastore, DatastoreError, Transaction, TransactionType};

/// The encoded major storage version key, `!v`.
pub const VERSION_KEY: &[u8] = b"!v";

/// The encoded bootstrap sentinel, `!bv`.
pub const BOOTSTRAP_KEY: &[u8] = b"!bv";

/// Every key a copy carries: from `0x00` up to but excluding `0xff`.
///
/// Spans undeclared keys as well as declared ones, so a source holding data
/// this release does not describe hands it on, and the target's version gate
/// judges it exactly as the source's would. Keys at or above `0xff` are outside
/// it. A composer keeps state of its own there, which is what keeps that state
/// out of every copy taken from the store that holds it.
pub fn keyspace() -> RawRange {
	RawRange::every_key()
}

/// When a key is written into the target of a copy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyRole {
	/// Written in key order.
	Data,
	/// The major storage version, [`VERSION_KEY`]: written after every other key,
	/// so its presence on the target means the copy is whole.
	Version,
	/// The bootstrap sentinel, [`BOOTSTRAP_KEY`]: never written. See the module
	/// documentation.
	Skipped,
}

/// The role of `key` in a copy.
pub fn key_role(key: &[u8]) -> KeyRole {
	if key == VERSION_KEY {
		KeyRole::Version
	} else if key == BOOTSTRAP_KEY {
		KeyRole::Skipped
	} else {
		KeyRole::Data
	}
}

/// What decides whether this build can start from a copy of a keyspace.
///
/// Read from the source in the same snapshot as the keys it describes, and
/// checked by the target before it writes anything, so a source this build would
/// refuse is refused before the copy rather than after it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyspaceVersion {
	/// The major storage version stored at [`VERSION_KEY`], absent when the
	/// keyspace carries none.
	pub major: Option<u16>,
	/// The semantic version stamp, absent on a keyspace last written before 3.3.
	/// Reported in errors; it decides nothing.
	pub stamp: Option<String>,
	/// The ledger id of every data migration the keyspace records as applied.
	pub migrations: Vec<u32>,
}

impl KeyspaceVersion {
	/// Reads the description of the keyspace `tx` sees.
	pub async fn read(tx: &Transaction) -> Result<Self> {
		let major = tx.get_key(&VersionKey {}, None).await?.map(u16::from);
		let stamp = tx.get_key(&StorageVersionKey {}, None).await?.map(|stamp| stamp.to_string());
		let keys = tx.keys_raw(MigrationPrefix {}.range()?, u32::MAX, 0, None).await?;
		let migrations =
			keys.iter().map(|key| Ok(MigrationKey::decode_key(key)?.id)).collect::<Result<_>>()?;
		Ok(Self {
			major,
			stamp,
			migrations,
		})
	}

	/// Refuses a keyspace this build could not start from once the copy had
	/// landed: one [`Datastore::check_version`] would refuse, with the error it
	/// would raise.
	///
	/// The major version must be the one this build opens, and the ledger is judged
	/// by the rule the version gate applies: an entry this build has no row for
	/// refuses the keyspace. A keyspace without a major version reads as one
	/// written before versioning existed, which is how the version gate would read
	/// the copy.
	pub fn check(&self) -> Result<()> {
		let major = self.major.map(MajorVersion::from).unwrap_or_else(MajorVersion::v1);
		if !major.is_latest() {
			bail!(DatastoreError::OutdatedStorageVersion {
				expected: MajorVersion::latest().into(),
				actual: major.into(),
			});
		}
		let entries = self.migrations.iter().map(|id| (*id, format!("id {id}")));
		let unknown = unknown_migrations(entries, MIGRATIONS);
		if !unknown.is_empty() {
			bail!(DatastoreError::MigratedBeyondStorageVersion {
				stored: self.stamp.clone().unwrap_or_else(|| "unknown".to_owned()),
				running: StorageVersion::current().to_string(),
				migrations: unknown.join(", "),
			});
		}
		Ok(())
	}
}

/// Whether `datastore` answers reads at a past version, and so holds history
/// that a copy of each key's latest value would drop.
///
/// Probed with one read at an early version: a backend that keeps no history
/// refuses it as unsupported, and one that keeps history answers it.
pub async fn keeps_history(datastore: &Datastore) -> Result<bool> {
	let tx = datastore.transaction(TransactionType::Read).await?;
	let probe = tx.get_key(&VersionKey {}, Some(1)).await;
	tx.cancel().await?;
	match probe {
		Ok(_) => Ok(true),
		Err(err)
			if matches!(
				surrealdb_datastore::storage_error(&err),
				Some(surrealdb_kvs::Error::UnsupportedVersionedQueries)
			) =>
		{
			Ok(false)
		}
		Err(err) => Err(err),
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_datastore::key::schema::KEYSPACE_MAP;

	use super::*;
	use crate::dbs::Session;
	use crate::key::KVKey;
	use crate::key::schema::BootstrapKey;

	#[test]
	fn the_named_keys_are_the_ones_the_schema_encodes() -> Result<()> {
		assert_eq!(&*VersionKey {}.encode_key()?, VERSION_KEY);
		assert_eq!(&*BootstrapKey {}.encode_key()?, BOOTSTRAP_KEY);
		assert_eq!(key_role(VERSION_KEY), KeyRole::Version);
		assert_eq!(key_role(BOOTSTRAP_KEY), KeyRole::Skipped);
		// A key this release does not declare is data, wherever it sits
		assert_eq!(key_role(b"!fl"), KeyRole::Data);
		assert_eq!(key_role(b"/!vs"), KeyRole::Data);
		assert_eq!(key_role(b"/!mg"), KeyRole::Data);
		Ok(())
	}

	/// Every declared key begins with `/` or `!`, so inside the copied range, and
	/// the only ones outside the `/` root are the ones whose role in a copy this
	/// module decides. A new key outside the root fails here, because its place
	/// in a copy has to be decided before it is written.
	#[test]
	fn every_declared_key_outside_the_root_has_a_role() {
		let routes: Vec<&str> = KEYSPACE_MAP
			.lines()
			.filter(|line| line.contains(" -> "))
			.filter_map(|line| line.split_whitespace().next())
			.collect();
		assert!(routes.len() > 10, "the keyspace map did not parse: {routes:?}");
		let range = keyspace();
		for route in &routes {
			let first = &route.as_bytes()[..1];
			assert!(
				matches!(first, b"/" | b"!") && &**range.start() <= first && first < &**range.end(),
				"{route} is outside the copied range"
			);
		}
		let outside: Vec<&str> =
			routes.iter().copied().filter(|route| !route.starts_with('/')).collect();
		assert_eq!(outside, ["!bv", "!v"]);
	}

	/// A store holding keys from the families a keyspace is made of: the catalog,
	/// records, a graph edge, a reference, a unique and a full-text index and a
	/// change feed.
	async fn seeded() -> Result<std::sync::Arc<Datastore>> {
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		ds.check_version().await?;
		let session = Session::owner().with_ns("t").with_db("t");
		let sql = "
			DEFINE NAMESPACE t; DEFINE DATABASE t;
			DEFINE USER root_reader ON ROOT PASSWORD 'x' ROLES VIEWER;
			DEFINE TABLE person SCHEMAFULL CHANGEFEED 1d;
			DEFINE FIELD name ON person TYPE string;
			DEFINE FIELD best ON person TYPE option<record<person>> REFERENCE;
			DEFINE INDEX person_name ON person FIELDS name UNIQUE;
			DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
			DEFINE INDEX person_search ON person FIELDS name FULLTEXT ANALYZER simple BM25;
			DEFINE TABLE knows TYPE RELATION;
			DEFINE EVENT noted ON person WHEN true THEN (CREATE note SET at = time::now());
			DEFINE FUNCTION fn::one() { RETURN 1 };
			DEFINE PARAM $answer VALUE 42;
			DEFINE SEQUENCE ids;
			CREATE person:1 SET name = 'one';
			CREATE person:2 SET name = 'two', best = person:1;
			RELATE person:1->knows->person:2;
		";
		for result in ds.execute(sql, &session, None).await? {
			result.result.map_err(|err| anyhow::anyhow!("seeding failed: {err}"))?;
		}
		Ok(ds)
	}

	/// Every key the engine writes lies inside the copied range, and the only one
	/// outside the `/` root is the version key, so the roles above are all a copy
	/// has to decide and nothing is stored at or above `0xff` for it to miss.
	#[tokio::test]
	async fn every_key_the_engine_writes_is_inside_the_copied_range() -> Result<()> {
		let ds = seeded().await?;
		let tx = ds.transaction(TransactionType::Read).await?;
		let copied = tx.keys_raw(keyspace(), u32::MAX, 0, None).await;
		let above = tx.keys_raw(RawRange::of_bytes(&[0xff], &[0xff; 64]), u32::MAX, 0, None).await;
		tx.cancel().await?;
		let copied = copied?;
		assert!(copied.len() > 40, "the seed did not write a keyspace worth checking");
		let outside: Vec<&Vec<u8>> = copied.iter().filter(|key| !key.starts_with(b"/")).collect();
		assert_eq!(outside, [&VERSION_KEY.to_vec()]);
		assert!(copied.iter().all(|key| key_role(key) != KeyRole::Skipped));
		assert!(above?.is_empty());
		Ok(())
	}

	fn version(major: Option<u16>, migrations: &[u32]) -> KeyspaceVersion {
		KeyspaceVersion {
			major,
			stamp: Some("9.9.9".to_owned()),
			migrations: migrations.to_vec(),
		}
	}

	#[test]
	fn a_keyspace_this_build_can_start_from_passes() -> Result<()> {
		let every: Vec<u32> = MIGRATIONS.iter().map(|migration| migration.id).collect();
		version(Some(MajorVersion::LATEST), &every).check()?;
		version(Some(MajorVersion::LATEST), &[]).check()?;
		Ok(())
	}

	#[test]
	fn an_unversioned_or_other_major_is_refused_as_out_of_date() {
		for major in [None, Some(1), Some(2), Some(MajorVersion::LATEST + 1)] {
			let err = version(major, &[]).check().unwrap_err();
			assert!(
				matches!(
					err.downcast_ref::<DatastoreError>(),
					Some(DatastoreError::OutdatedStorageVersion { .. })
				),
				"{major:?}: {err}"
			);
		}
	}

	/// A ledger entry this build has no row for refuses the keyspace, as the
	/// version gate refuses it.
	#[test]
	fn an_unknown_ledger_entry_is_refused() {
		let err = version(Some(MajorVersion::LATEST), &[1, 1_000_000]).check().unwrap_err();
		match err.downcast_ref::<DatastoreError>() {
			Some(DatastoreError::MigratedBeyondStorageVersion {
				stored,
				migrations,
				..
			}) => {
				assert_eq!(stored, "9.9.9");
				assert_eq!(migrations, "id 1000000");
			}
			other => panic!("refused with {other:?}: {err}"),
		}
		let unstamped = KeyspaceVersion {
			stamp: None,
			..version(Some(MajorVersion::LATEST), &[1_000_000])
		};
		let err = unstamped.check().unwrap_err();
		assert!(
			matches!(
				err.downcast_ref::<DatastoreError>(),
				Some(DatastoreError::MigratedBeyondStorageVersion { stored, .. }) if stored == "unknown"
			),
			"{err}"
		);
	}

	#[tokio::test]
	async fn a_running_store_describes_itself_as_startable() -> Result<()> {
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		ds.check_version().await?;
		let tx = ds.transaction(TransactionType::Read).await?;
		let described = KeyspaceVersion::read(&tx).await;
		tx.cancel().await?;
		let described = described?;
		assert_eq!(described.major, Some(MajorVersion::LATEST));
		assert_eq!(described.stamp, Some(StorageVersion::current().to_string()));
		let mut applied: Vec<u32> =
			ds.applied_migration_ids().await?.into_iter().map(|(id, _)| id).collect();
		applied.sort_unstable();
		let mut described_ids = described.migrations.clone();
		described_ids.sort_unstable();
		assert_eq!(described_ids, applied);
		described.check()?;
		Ok(())
	}

	/// The serialised description is the header a target reads, so its shape is
	/// pinned.
	#[test]
	fn the_description_serialises_with_a_fixed_shape() -> Result<()> {
		let described = KeyspaceVersion {
			major: Some(3),
			stamp: Some("3.3.0".to_owned()),
			migrations: vec![1, 7],
		};
		assert_eq!(
			serde_json::to_string(&described)?,
			r#"{"major":3,"stamp":"3.3.0","migrations":[1,7]}"#
		);
		assert_eq!(
			serde_json::from_str::<KeyspaceVersion>(&serde_json::to_string(&described)?)?,
			described
		);
		assert_eq!(
			serde_json::to_string(&KeyspaceVersion::default())?,
			r#"{"major":null,"stamp":null,"migrations":[]}"#
		);
		Ok(())
	}

	async fn described(ds: &Datastore) -> Result<KeyspaceVersion> {
		let tx = ds.transaction(TransactionType::Read).await?;
		let described = KeyspaceVersion::read(&tx).await;
		tx.cancel().await?;
		described
	}

	/// A keyspace that a later release advanced names every entry of its ledger,
	/// and only those: the keys beside the ledger are not entries of it.
	#[tokio::test]
	async fn the_ledger_a_later_release_wrote_is_named() -> Result<()> {
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		ds.check_version().await?;
		let mut expected: Vec<u32> =
			ds.applied_migration_ids().await?.into_iter().map(|(id, _)| id).collect();
		let tx = ds.transaction(TransactionType::Write).await?;
		for id in [300u32, 2_000_000_000, u32::MAX] {
			tx.set(MigrationKey::new(id).encode_key()?, Vec::new()).await?;
			expected.push(id);
		}
		// Neighbours of the ledger that are not entries of it
		for key in [&b"/!md"[..], b"/!mf\xff\xff\xff\xff", b"/!mh\0\0\0\x01"] {
			tx.set(surrealdb_kvs::Key::from(key.to_vec()), b"x".to_vec()).await?;
		}
		tx.commit().await?;
		let mut named = described(&ds).await?.migrations;
		named.sort_unstable();
		expected.sort_unstable();
		assert_eq!(named, expected);
		Ok(())
	}

	#[tokio::test]
	async fn a_malformed_ledger_key_is_not_described_as_a_ledger() -> Result<()> {
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		ds.check_version().await?;
		let tx = ds.transaction(TransactionType::Write).await?;
		tx.set(surrealdb_kvs::Key::from(b"/!mg\0\0\x01".to_vec()), b"x".to_vec()).await?;
		tx.commit().await?;
		assert!(described(&ds).await.is_err());
		Ok(())
	}

	#[tokio::test]
	async fn a_memory_store_keeps_no_history() -> Result<()> {
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		assert!(!keeps_history(&ds).await?);
		Ok(())
	}

	/// A store keeps history exactly when it was opened to: on the same backend,
	/// the probe tells a versioned store from one that is not. The memory backend
	/// refuses to be versioned, so this runs on disk.
	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test]
	async fn a_store_keeps_history_only_when_it_is_versioned() -> Result<()> {
		let plain_dir = temp_dir::TempDir::new()?;
		let plain = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path(&format!("rocksdb://{}", plain_dir.path().to_string_lossy()))
			.await?;
		assert!(!keeps_history(&plain).await?);

		let versioned_dir = temp_dir::TempDir::new()?;
		let versioned = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path(&format!(
				"rocksdb://{}?versioned=true",
				versioned_dir.path().to_string_lossy()
			))
			.await?;
		assert!(keeps_history(&versioned).await?);
		// Whether the version key is stored or not does not change the answer
		versioned.check_version().await?;
		assert!(keeps_history(&versioned).await?);
		Ok(())
	}
}
