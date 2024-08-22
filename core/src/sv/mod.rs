#[derive(Debug, Clone)]
pub struct StorageVersion(u16);

impl From<u16> for StorageVersion {
	fn from(version: u16) -> Self {
		if version <= StorageVersion::LATEST {
			StorageVersion(version)
		} else {
			panic!("Invalid storage version: {}", version);
		}
	}
}

impl From<Option<u16>> for StorageVersion {
	fn from(version: Option<u16>) -> Self {
		version.unwrap_or_else(|| 0).into()
	}
}

impl Into<u16> for StorageVersion {
	fn into(self) -> u16 {
		self.0
	}
}

impl StorageVersion {
	pub const LATEST: u16 = 1;

	pub fn latest() -> Self {
		Self(Self::LATEST)
	}

	pub fn is_latest(&self) -> bool {
		self.0 == Self::LATEST
	}
}
