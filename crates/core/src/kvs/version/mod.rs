use super::{Datastore, LockType, TransactionType};
use crate::err::Error;
use std::sync::Arc;

mod fixes;
pub mod v3;

#[derive(Copy, Debug, Clone)]
pub struct Version(u16);

impl From<u16> for Version {
	fn from(version: u16) -> Self {
		Version(version)
	}
}

impl From<Option<u16>> for Version {
	fn from(v: Option<u16>) -> Self {
		v.unwrap_or(0).into()
	}
}

impl From<Version> for u16 {
	fn from(v: Version) -> Self {
		v.0
	}
}

impl From<Version> for Vec<u8> {
	fn from(v: Version) -> Self {
		v.0.to_be_bytes().to_vec()
	}
}

impl TryFrom<Vec<u8>> for Version {
	type Error = Error;
	fn try_from(v: Vec<u8>) -> Result<Self, Self::Error> {
		let bin = v.try_into().map_err(|_| Error::InvalidStorageVersion)?;
		let val = u16::from_be_bytes(bin).into();
		Ok(val)
	}
}

/// How far the datastore's layout has moved within its major storage version.
///
/// The major version at `!v` is allocated across the whole product: its next
/// value belongs to the next major release of SurrealDB, which stamps it on
/// datastores holding an entirely different layout. A format change made on a
/// maintenance line therefore cannot be expressed there without claiming a
/// number another release already uses, and a datastore carrying that number
/// would be opened by that release as one of its own. The revision at `!vr` is
/// the line's own counter, and is absent until something bumps it.
///
/// A build refuses to open a datastore whose revision is above
/// [`Revision::KNOWN`]: every revision names a layout change that the builds
/// before it read incorrectly rather than not at all.
#[derive(Copy, Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Revision(u16);

impl From<u16> for Revision {
	fn from(revision: u16) -> Self {
		Revision(revision)
	}
}

impl From<Revision> for u16 {
	fn from(r: Revision) -> Self {
		r.0
	}
}

impl From<Revision> for Vec<u8> {
	fn from(r: Revision) -> Self {
		r.0.to_be_bytes().to_vec()
	}
}

impl TryFrom<Vec<u8>> for Revision {
	type Error = Error;
	fn try_from(v: Vec<u8>) -> Result<Self, Self::Error> {
		let bin = v.try_into().map_err(|_| Error::InvalidStorageVersion)?;
		Ok(Revision(u16::from_be_bytes(bin)))
	}
}

impl Default for Revision {
	/// The revision of a datastore with no `!vr` key: nothing has changed the
	/// layout since the major version was stamped.
	fn default() -> Self {
		Revision(0)
	}
}

impl Revision {
	/// The newest revision this build can read.
	///
	/// Revision 1 is deferred reclaim. `REMOVE NAMESPACE` and `REMOVE DATABASE`
	/// leave the removed object's data on disk and enqueue its prefix at `/!rc`;
	/// reading such a datastore correctly means running the reaper that destroys
	/// those prefixes, and refusing to reuse a name whose entry is still queued.
	/// A build without that code hands a recreated namespace or database the
	/// removed one's records, users and access grants.
	///
	/// This build holds that code, so it reads a datastore at revision 1. It
	/// does not stamp one. A stamp only binds builds that read `!vr`, and a
	/// release that predates the key opens the datastore whatever it says, so
	/// the reader has to be deployed before anything writes a revision.
	pub const KNOWN: u16 = 1;

	/// Whether this build can read a datastore at this revision.
	pub fn is_known(&self) -> bool {
		self.0 <= Self::KNOWN
	}
}

impl Version {
	/// The latest version
	pub const LATEST: u16 = 2;
	/// The latest version
	pub fn latest() -> Self {
		Self(2)
	}
	/// SurrealDB version 1
	pub fn v1() -> Self {
		Self(1)
	}
	/// SurrealDB version 2
	pub fn v2() -> Self {
		Self(2)
	}
	/// Check if we are running the latest version
	pub fn is_latest(&self) -> bool {
		self.0 == Self::LATEST
	}
	/// Fix
	pub async fn fix(&self, ds: Arc<Datastore>) -> Result<(), Error> {
		// We iterate through each version from the current to the latest
		// and apply the fixes for each version. We update storage version
		// and commit changes each iteration, to keep transactions as small
		// as possible.
		//
		// We commit all fixes and the storage version update in one transaction,
		// because we never want to leave storage in a broken state where half of
		// the fixes are applied and the storage version is not updated.
		//
		for v in self.0..Version::LATEST {
			// Create a new transaction
			let tx = Arc::new(ds.transaction(TransactionType::Write, LockType::Pessimistic).await?);

			// Easy shortcut to apply a fix
			macro_rules! apply_fix {
				($name:ident) => {{
					match fixes::$name(tx.clone()).await {
						// Fail early and cancel transaction if the fix failed
						Err(e) => {
							tx.cancel().await?;
							return Err(e);
						}
						_ => {}
					};
				}};
			}

			// Apply fixes based on the current version
			if v == 1 {
				apply_fix!(v1_to_2_id_uuid);
				apply_fix!(v1_to_2_migrate_to_access);
			}

			// Obtain storage version key and value
			let key = crate::key::version::new();
			let val: Vec<u8> = Version::from(v + 1).into();
			// Attempt to set the current version in storage
			tx.replace(key, val).await?;

			// Commit the transaction
			tx.commit().await?;
		}

		Ok(())
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::{Revision, Version};
	use crate::err::Error;
	use crate::kvs::{Datastore, LockType::*, TransactionType::*};

	/// Stamps a datastore with its major version, as the first startup does.
	async fn settle(ds: &Datastore) {
		ds.check_version().await.unwrap();
	}

	/// Stamps a datastore with a raw revision value.
	async fn stamp(ds: &Datastore, val: Vec<u8>) {
		let txn = ds.transaction(Write, Optimistic).await.unwrap();
		txn.replace(crate::key::version::revision(), val).await.unwrap();
		txn.commit().await.unwrap();
	}

	#[tokio::test]
	async fn a_datastore_with_no_revision_reads_as_zero() {
		let ds = Datastore::new("memory").await.unwrap();
		assert_eq!(u16::from(ds.get_revision().await.unwrap()), 0);
		ds.check_version().await.unwrap();
	}

	#[tokio::test]
	async fn a_revision_this_build_reads_is_accepted() {
		let ds = Datastore::new("memory").await.unwrap();
		settle(&ds).await;
		stamp(&ds, Revision::from(Revision::KNOWN).into()).await;
		assert_eq!(u16::from(ds.get_revision().await.unwrap()), Revision::KNOWN);
		ds.check_version().await.unwrap();
	}

	#[tokio::test]
	async fn a_revision_beyond_this_build_refuses_the_startup() {
		let ds = Datastore::new("memory").await.unwrap();
		settle(&ds).await;
		let ahead = Revision::KNOWN + 1;
		stamp(&ds, Revision::from(ahead).into()).await;
		match ds.check_version().await {
			Err(Error::NewerStorageRevision {
				stored,
				known,
			}) => {
				assert_eq!(stored, ahead);
				assert_eq!(known, Revision::KNOWN);
			}
			other => panic!("a datastore written by a later release was opened: {other:?}"),
		}
	}

	#[tokio::test]
	async fn a_revision_which_does_not_decode_refuses_the_startup() {
		let ds = Datastore::new("memory").await.unwrap();
		settle(&ds).await;
		stamp(&ds, vec![0x00, 0x00, 0x00]).await;
		match ds.check_version().await {
			Err(Error::InvalidStorageVersion) => {}
			other => panic!("a revision of the wrong width was accepted: {other:?}"),
		}
	}

	/// The two gates are independent: a datastore behind on its major version is
	/// refused for that reason whatever its revision says.
	#[tokio::test]
	async fn the_major_version_is_checked_before_the_revision() {
		let ds = Datastore::new("memory").await.unwrap();
		settle(&ds).await;
		let txn = ds.transaction(Write, Optimistic).await.unwrap();
		let val: Vec<u8> = Version::v1().into();
		txn.replace(crate::key::version::new(), val).await.unwrap();
		txn.commit().await.unwrap();
		stamp(&ds, Revision::from(Revision::KNOWN + 1).into()).await;
		match ds.check_version().await {
			Err(Error::OutdatedStorageVersion) => {}
			other => panic!("an outdated datastore was not reported as such: {other:?}"),
		}
	}

	/// A revision is only ever written after the major version, so one standing
	/// alone is a datastore from before versioning existed with a key that
	/// happens to sort there. The startup refuses it rather than stamping the
	/// current version over whatever else is on disk.
	#[tokio::test]
	async fn a_revision_without_a_major_version_refuses_the_startup() {
		let ds = Datastore::new("memory").await.unwrap();
		stamp(&ds, Revision::from(Revision::KNOWN).into()).await;
		match ds.check_version().await {
			Err(Error::OutdatedStorageVersion) => {}
			other => panic!("a datastore holding unversioned data was opened: {other:?}"),
		}
	}
}
