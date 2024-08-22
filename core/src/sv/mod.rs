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
		version.unwrap_or(0).into()
	}
}

impl From<StorageVersion> for u16 {
	fn from(version: StorageVersion) -> Self {
		version.0
	}
}

impl StorageVersion {
	pub const FIRST: u16 = 0;
	pub const LATEST: u16 = 1;

	pub fn first() -> Self {
		Self(Self::FIRST)
	}

	pub fn latest() -> Self {
		Self(Self::LATEST)
	}

	pub fn is_latest(&self) -> bool {
		self.0 == Self::LATEST
	}
}
