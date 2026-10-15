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
//! rather than started from with keys missing. The bootstrap sentinel `!bv`, which
//! releases after this one write next to a missing version key to mark an
//! interrupted bootstrap, would undo that: a gate that finds it completes the
//! partial copy as a new store. A copy therefore never carries it, whatever the
//! source holds.
//!
//! # What this release records
//!
//! This release keeps one storage version key, `!v`. It keeps no storage-version
//! stamp and no ledger of applied data migrations: those belong to later releases,
//! which write them under the root keyspace at `/!vs` and `/!mg{id}`. A keyspace
//! this release wrote therefore carries neither, and [`KeyspaceVersion`] reports
//! none. A keyspace that a later release has advanced carries the ledger, and
//! [`KeyspaceVersion::read`] names it from the keys, so a target whose build does
//! not know a migration the source has applied refuses the copy before it starts.

use std::ops::Range;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::kvs::version::MajorVersion;
use crate::kvs::{Datastore, Error as KvsError, Key, LockType, Transaction, TransactionType};

/// The encoded major storage version key, `!v`.
pub const VERSION_KEY: &[u8] = b"!v";

/// The encoded bootstrap sentinel, `!bv`. This release never writes it; a later
/// one does, and a copy never carries it.
pub const BOOTSTRAP_KEY: &[u8] = b"!bv";

/// The first byte of the ledger's keys, `/!mg`, followed by the migration id as a
/// big-endian `u32`.
const LEDGER_PREFIX: &[u8] = b"/!mg";

/// The exclusive end of the ledger's keys: [`LEDGER_PREFIX`] with its last byte
/// incremented, which orders after every id the prefix can be followed by.
const LEDGER_END: &[u8] = b"/!mh";

/// The first byte of the copied range.
const KEYSPACE_START: u8 = 0x00;

/// The first byte outside the copied range.
const KEYSPACE_END: u8 = 0xff;

/// Every key a copy carries: from `0x00` up to but excluding `0xff`.
///
/// Spans undeclared keys as well as declared ones, so a source holding data
/// this release does not describe hands it on, and the target's version gate
/// judges it exactly as the source's would. Every key the engine writes begins
/// with `/` or `!`, so nothing it stores is missed. Keys at or above `0xff` are
/// outside the range: a composer keeps state of its own there, which is what
/// keeps that state out of every copy taken from the store that holds it.
pub fn keyspace() -> Range<Key> {
	vec![KEYSPACE_START]..vec![KEYSPACE_END]
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
	/// The semantic version stamp of the release that last advanced the keyspace.
	/// Always absent here: this release writes none, and the stamp decides
	/// nothing, it is only quoted in a target's errors.
	pub stamp: Option<String>,
	/// The ledger id of every data migration the keyspace records as applied, in
	/// ascending order. Empty for a keyspace this release wrote.
	pub migrations: Vec<u32>,
}

impl KeyspaceVersion {
	/// Reads the description of the keyspace `tx` sees.
	///
	/// # Errors
	///
	/// Fails when the stored version is not a version, or when a key in the
	/// ledger's range is not a ledger entry.
	pub async fn read(tx: &Transaction) -> Result<Self> {
		let major = tx.get(&crate::key::version::new(), None).await?.map(u16::from);
		let keys = tx.keys(LEDGER_PREFIX.to_vec()..LEDGER_END.to_vec(), u32::MAX, 0, None).await?;
		let migrations = keys.iter().map(|key| ledger_id(key)).collect::<Result<_>>()?;
		Ok(Self {
			major,
			stamp: None,
			migrations,
		})
	}

	/// Refuses a keyspace this build could not start from, with the error
	/// [`Datastore::check_version`] would raise once the copy had landed.
	///
	/// A keyspace without a major version reads as one written before versioning
	/// existed, which is how the version gate reads a copy that holds keys and no
	/// version key. This build applies no data migrations and so has no ledger to
	/// hold a source's against; a target that does compares [`Self::migrations`]
	/// with its own.
	pub fn check(&self) -> Result<()> {
		let major = self.major.map(MajorVersion::from).unwrap_or_else(MajorVersion::v1);
		if !major.is_latest() {
			bail!(Error::OutdatedStorageVersion {
				expected: MajorVersion::latest().into(),
				actual: major.into(),
			});
		}
		Ok(())
	}
}

/// The migration id a ledger key names.
fn ledger_id(key: &[u8]) -> Result<u32> {
	let Some(id) = key.strip_prefix(LEDGER_PREFIX).and_then(|id| <[u8; 4]>::try_from(id).ok())
	else {
		bail!("a key in the migration ledger's range is not a ledger entry");
	};
	Ok(u32::from_be_bytes(id))
}

/// Whether `datastore` answers reads at a past version, and so holds history
/// that a copy of each key's latest value would drop.
///
/// Probed with one read at an early version: a backend that keeps no history
/// refuses it as unsupported, and one that keeps history answers it.
pub async fn keeps_history(datastore: &Datastore) -> Result<bool> {
	let tx = datastore.transaction(TransactionType::Read, LockType::Optimistic).await?;
	let probe = tx.get(&crate::key::version::new(), Some(1)).await;
	tx.cancel().await?;
	match probe {
		Ok(_) => Ok(true),
		Err(err) if refuses_versioned_reads(&err) => Ok(false),
		Err(err) => Err(err),
	}
}

/// Whether a failed read says the backend keeps no history.
fn refuses_versioned_reads(err: &anyhow::Error) -> bool {
	matches!(
		err.downcast_ref::<Error>(),
		Some(
			Error::Kvs(KvsError::UnsupportedVersionedQueries) | Error::UnsupportedVersionedQueries
		)
	) || matches!(err.downcast_ref::<KvsError>(), Some(KvsError::UnsupportedVersionedQueries))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::kvs::KVKey;

	/// A memory datastore with its version settled and a keyspace broad enough
	/// to reach the catalog, record, graph, index and queue key families.
	async fn seeded(path: &str) -> Result<Datastore> {
		let ds = Datastore::new(path).await?;
		ds.check_version().await?;
		let session = Session::owner().with_ns("t").with_db("t");
		let sql = "
			DEFINE NAMESPACE t; DEFINE DATABASE t;
			DEFINE USER root_reader ON ROOT PASSWORD 'x' ROLES VIEWER;
			DEFINE TABLE person SCHEMAFULL CHANGEFEED 1d;
			DEFINE FIELD name ON person TYPE string;
			DEFINE INDEX person_name ON person FIELDS name UNIQUE;
			DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
			DEFINE INDEX person_search ON person FIELDS name FULLTEXT ANALYZER simple BM25;
			DEFINE TABLE knows TYPE RELATION;
			DEFINE EVENT noted ON person WHEN true THEN (CREATE note SET at = time::now());
			DEFINE FUNCTION fn::one() { RETURN 1 };
			DEFINE PARAM $answer VALUE 42;
			DEFINE SEQUENCE ids;
			CREATE person:1 SET name = 'one';
			CREATE person:2 SET name = 'two';
			RELATE person:1->knows->person:2;
		";
		for result in ds.execute(sql, &session, None).await? {
			result.result.map_err(|err| anyhow::anyhow!("seeding failed: {err}"))?;
		}
		Ok(ds)
	}

	#[test]
	fn the_named_keys_are_the_ones_the_engine_encodes() -> Result<()> {
		assert_eq!(crate::key::version::new().encode_key()?, VERSION_KEY);
		assert_eq!(key_role(VERSION_KEY), KeyRole::Version);
		assert_eq!(key_role(BOOTSTRAP_KEY), KeyRole::Skipped);
		assert_eq!(key_role(b"/!vs"), KeyRole::Data);
		assert_eq!(key_role(b"/!mg\0\0\0\x01"), KeyRole::Data);
		assert_eq!(key_role(b"!vv"), KeyRole::Data);
		assert_eq!(keyspace(), vec![0x00]..vec![0xff]);
		Ok(())
	}

	/// Every key the engine writes begins with `/`, except the version key, so it
	/// lies inside the copied range, the only key outside the `/` root is the one
	/// whose role in a copy this module decides, and nothing is stored at or
	/// above `0xff` for the copy to miss.
	#[tokio::test]
	async fn every_key_the_engine_writes_is_inside_the_copied_range() -> Result<()> {
		let ds = seeded("memory").await?;
		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let keyspace = tx.keys(keyspace(), u32::MAX, 0, None).await;
		let above = tx.keys(vec![KEYSPACE_END]..vec![KEYSPACE_END; 64], u32::MAX, 0, None).await;
		tx.cancel().await?;
		let keyspace = keyspace?;
		assert!(keyspace.len() > 40, "the seed did not write a keyspace worth checking");
		let outside: Vec<&Key> = keyspace.iter().filter(|key| !key.starts_with(b"/")).collect();
		assert_eq!(outside, [&VERSION_KEY.to_vec()]);
		assert!(keyspace.iter().all(|key| key_role(key) != KeyRole::Skipped));
		assert_eq!(above?, Vec::<Key>::new());
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
		version(Some(MajorVersion::LATEST), &[1, 2, 3]).check()?;
		version(Some(MajorVersion::LATEST), &[]).check()?;
		Ok(())
	}

	#[test]
	fn an_older_or_unversioned_major_is_refused_as_out_of_date() {
		for major in [None, Some(1), Some(2)] {
			let err = version(major, &[]).check().unwrap_err();
			assert!(
				matches!(err.downcast_ref::<Error>(), Some(Error::OutdatedStorageVersion { .. })),
				"{major:?}: {err}"
			);
		}
	}

	#[tokio::test]
	async fn a_running_store_describes_itself_as_startable() -> Result<()> {
		let ds = seeded("memory").await?;
		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let described = KeyspaceVersion::read(&tx).await;
		tx.cancel().await?;
		let described = described?;
		assert_eq!(
			described,
			KeyspaceVersion {
				major: Some(MajorVersion::LATEST),
				stamp: None,
				migrations: Vec::new(),
			}
		);
		described.check()?;
		Ok(())
	}

	/// The serialised description is the header a target reads, so its shape is
	/// pinned.
	#[test]
	fn the_description_serialises_with_a_fixed_shape() -> Result<()> {
		let described = KeyspaceVersion {
			major: Some(3),
			stamp: None,
			migrations: vec![1, 7],
		};
		assert_eq!(
			serde_json::to_string(&described)?,
			r#"{"major":3,"stamp":null,"migrations":[1,7]}"#
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
		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await?;
		let described = KeyspaceVersion::read(&tx).await;
		tx.cancel().await?;
		described
	}

	/// A keyspace that a later release advanced names every entry of its ledger,
	/// and only those, in ascending order: the keys beside the ledger are not
	/// entries of it.
	#[tokio::test]
	async fn the_ledger_a_later_release_wrote_is_named() -> Result<()> {
		let ds = seeded("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		for id in [300u32, 2_000_000_000, u32::MAX, 1] {
			let mut key = LEDGER_PREFIX.to_vec();
			key.extend_from_slice(&id.to_be_bytes());
			tx.set(&key, &b"record".to_vec()).await?;
		}
		// Neighbours of the ledger that are not entries of it
		for key in [&b"/!md"[..], b"/!mf\xff\xff\xff\xff", b"/!mh\0\0\0\x01", b"/!vs"] {
			tx.set(&key.to_vec(), &b"x".to_vec()).await?;
		}
		tx.commit().await?;
		assert_eq!(described(&ds).await?.migrations, [1, 300, 2_000_000_000, u32::MAX]);
		Ok(())
	}

	#[tokio::test]
	async fn a_malformed_ledger_key_is_not_described_as_a_ledger() -> Result<()> {
		let ds = seeded("memory").await?;
		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await?;
		tx.set(&b"/!mg\0\0\x01".to_vec(), &b"x".to_vec()).await?;
		tx.commit().await?;
		assert!(described(&ds).await.is_err());
		Ok(())
	}

	#[tokio::test]
	async fn a_memory_store_keeps_no_history() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		assert!(!keeps_history(&ds).await?);
		Ok(())
	}

	/// A store keeps history exactly when it was opened to: on the same backend,
	/// the probe tells a versioned store from one that is not.
	#[tokio::test]
	async fn a_store_keeps_history_only_when_it_is_versioned() -> Result<()> {
		let plain = Datastore::new("memory").await?;
		assert!(!keeps_history(&plain).await?);

		let versioned = Datastore::new("memory?versioned=true").await?;
		assert!(keeps_history(&versioned).await?);
		// Whether the version key is stored or not does not change the answer
		versioned.check_version().await?;
		assert!(keeps_history(&versioned).await?);
		Ok(())
	}
}
