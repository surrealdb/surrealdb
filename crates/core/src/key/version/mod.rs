//! Stores the storage format of the datastore
//!
//! `!v` holds the major storage version. `!vr` holds the storage revision,
//! which counts the format changes made within one major version.
//!
//! Both sit outside the `/` band so that they survive anything that clears the
//! datastore's contents. `!vr` sorts after `!v`, and so falls inside
//! [`proceeding`]: a datastore carrying a revision but no major version reads
//! as one holding data from before versioning existed, which refuses the
//! startup rather than stamping over it.
use crate::key::category::Categorise;
use crate::key::category::Category;
use crate::kvs::impl_key;
use serde::{Deserialize, Serialize};
use std::ops::Range;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Version {
	__: u8,
	_a: u8,
}

impl_key!(Version);

pub fn new() -> Version {
	Version::new()
}

pub fn proceeding() -> Range<Vec<u8>> {
	vec![b'!', b'v', 0x00]..vec![0xff]
}

impl Categorise for Version {
	fn categorise(&self) -> Category {
		Category::Version
	}
}

impl Version {
	pub fn new() -> Self {
		Self {
			__: b'!',
			_a: b'v',
		}
	}
}

impl Default for Version {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Revision {
	__: u8,
	_a: u8,
	_b: u8,
}

impl_key!(Revision);

pub fn revision() -> Revision {
	Revision::new()
}

impl Categorise for Revision {
	fn categorise(&self) -> Category {
		Category::VersionRevision
	}
}

impl Revision {
	pub fn new() -> Self {
		Self {
			__: b'!',
			_a: b'v',
			_b: b'r',
		}
	}
}

impl Default for Revision {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::{KeyDecode, KeyEncode};

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Version::new();
		let enc = Version::encode(&val).unwrap();
		assert_eq!(enc, b"!v");

		let dec = Version::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn revision_key() {
		use super::*;
		let val = Revision::new();
		let enc = Revision::encode(&val).unwrap();
		assert_eq!(enc, b"!vr");

		let dec = Revision::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}

	#[test]
	fn the_revision_sorts_inside_the_range_that_follows_the_version() {
		use super::*;
		let enc = Revision::encode(&Revision::new()).unwrap();
		let rng = proceeding();
		assert!(rng.contains(&enc));
	}
}
