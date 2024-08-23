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
		macro_rules! apply_fix {
			($name:ident) => {{
				let tx =
					Arc::new(ds.transaction(TransactionType::Write, LockType::Pessimistic).await?);
				match fixes::$name(tx.clone()).await {
					Ok(_) => {
						tx.commit().await?;
					}
					Err(e) => {
						tx.cancel().await?;
						return Err(e);
					}
				};
			}};
		}

		for v in self.0..Version::LATEST {
			println!("Applying fixes for version: {}", v);
			match v {
				1 => {
					println!("Applying v1_to_2_id_uuid");
					apply_fix!(v1_to_2_id_uuid)
				}
				_ => {}
			}
		}

		println!("Setting new storage version");
		ds.set_version_latest().await?;
		Ok(())
	}
}
