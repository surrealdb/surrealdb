//! Releases and the store-wide feature level.
//!
//! A [`Release`] names the newest feature set a binary understands. A store's [`FeatureLevel`] is
//! the newest release whose readers every write to that store must stay readable by. A binary newer
//! than the store's level writes only what the level's release reads.

use std::fmt;

/// A SurrealDB release: its major and minor number.
///
/// Patch releases never add features, so the patch number is not part of a release. Releases order
/// by major, then minor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Release {
	major: u16,
	minor: u16,
}

impl Release {
	/// The release of every catalog item that existed when the stored grammar shipped.
	pub const FIRST_STORED: Release = Release::new(4, 0);

	/// This build's release: the crate version's major and minor, with the 3.4 development line
	/// read as 4.0. A later version maps to itself.
	pub const CURRENT: Release = {
		let major = const_parse_u16(env!("CARGO_PKG_VERSION_MAJOR"));
		let minor = const_parse_u16(env!("CARGO_PKG_VERSION_MINOR"));
		if major == 3 && minor == 4 {
			Release::new(4, 0)
		} else {
			Release::new(major, minor)
		}
	};

	/// A release with the given major and minor number.
	pub const fn new(major: u16, minor: u16) -> Self {
		Self {
			major,
			minor,
		}
	}

	/// The major number.
	pub const fn major(self) -> u16 {
		self.major
	}

	/// The minor number.
	pub const fn minor(self) -> u16 {
		self.minor
	}

	/// The release as one integer, `major << 16 | minor`. Integers compare in release order.
	pub const fn to_u32(self) -> u32 {
		((self.major as u32) << 16) | self.minor as u32
	}

	/// The release of an integer written by [`Self::to_u32`]. Every `u32` names a release.
	pub const fn from_u32(v: u32) -> Self {
		Self {
			major: (v >> 16) as u16,
			minor: (v & 0xFFFF) as u16,
		}
	}

	/// Parses `"M.m"`: two non-empty runs of ASCII decimal digits separated by one `.`, each
	/// fitting a `u16`. A sign, whitespace, a patch number and any other character are refused.
	pub fn parse(s: &str) -> Option<Self> {
		let (major, minor) = s.split_once('.')?;
		Some(Self::new(parse_decimal_u16(major)?, parse_decimal_u16(minor)?))
	}
}

impl fmt::Display for Release {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}.{}", self.major, self.minor)
	}
}

const _: () = assert!(Release::CURRENT.to_u32() >= Release::FIRST_STORED.to_u32());

/// Parses a non-empty run of ASCII decimal digits that fits a `u16`.
fn parse_decimal_u16(s: &str) -> Option<u16> {
	if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	s.parse().ok()
}

/// Parses a decimal `u16` at compile time. A malformed or overflowing input fails the build.
const fn const_parse_u16(s: &str) -> u16 {
	let bytes = s.as_bytes();
	assert!(!bytes.is_empty(), "a version component must not be empty");
	let mut value: u32 = 0;
	let mut i = 0;
	while i < bytes.len() {
		let b = bytes[i];
		assert!(b.is_ascii_digit(), "a version component must be decimal digits");
		value = value * 10 + (b - b'0') as u32;
		assert!(value <= u16::MAX as u32, "a version component must fit a u16");
		i += 1;
	}
	value as u16
}

/// The newest release whose readers a store's catalog writes must stay readable by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeatureLevel {
	/// No level recorded: catalog entries are written in `revision`, which every 3.x reader reads.
	Legacy,
	/// Writes stay readable by the given release.
	At(Release),
}

impl FeatureLevel {
	/// Whether this is the Legacy level, where only `revision` is written and no stored-only key
	/// may be.
	pub const fn is_legacy(self) -> bool {
		matches!(self, Self::Legacy)
	}

	/// Whether a write whose floor is `floor` may happen at this level. At Legacy the value must
	/// also fit `revision` (no non-default field `revision` cannot hold); the entry encoder checks
	/// that separately.
	pub fn admits(self, floor: Release) -> bool {
		match self {
			Self::Legacy => floor <= Release::FIRST_STORED,
			Self::At(level) => floor <= level,
		}
	}

	/// This level, lowered to `r` if above it. Legacy stays Legacy.
	pub fn capped(self, r: Release) -> Self {
		match self {
			Self::Legacy => Self::Legacy,
			Self::At(level) => Self::At(level.min(r)),
		}
	}
}

impl fmt::Display for FeatureLevel {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Legacy => f.write_str("3.x"),
			Self::At(release) => release.fmt(f),
		}
	}
}
