use crate::err::Error;

mod patches;

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
}
