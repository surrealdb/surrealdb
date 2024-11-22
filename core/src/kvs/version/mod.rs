use super::{Datastore, LockType, TransactionType};
use crate::err::Error;
use std::sync::Arc;

mod fixes;

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
