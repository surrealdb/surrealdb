use std::io::{Read, Write};
use std::ops::Deref;
use std::time;

use revision::{
	BorrowedReader, DeserializeRevisioned, Error, LeafWalker, Revisioned, SerializeRevisioned,
	SkipRevisioned, WalkRevisioned,
};

use super::Duration;
use super::duration::read_std_duration;

/// A `std::time::Duration` held by a catalog entry, read from `revision` bytes without panicking.
///
/// The bytes are exactly those of `std::time::Duration`: seconds as a varint `u64`, then the
/// sub-second nanoseconds as a varint `u32`, with no revision header. `std`'s own reader panics on
/// a seconds count near `u64::MAX` with nanoseconds at or above one second; this reader carries
/// the nanoseconds into the seconds and refuses a carry that overflows as a
/// [`revision::Error::Deserialize`], exactly as [`Duration`]'s reader does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct BoundedDuration(pub time::Duration);

impl Deref for BoundedDuration {
	type Target = time::Duration;

	fn deref(&self) -> &time::Duration {
		&self.0
	}
}

impl From<time::Duration> for BoundedDuration {
	fn from(d: time::Duration) -> Self {
		Self(d)
	}
}

impl From<BoundedDuration> for time::Duration {
	fn from(d: BoundedDuration) -> Self {
		d.0
	}
}

impl From<BoundedDuration> for Duration {
	fn from(d: BoundedDuration) -> Self {
		Self(d.0)
	}
}

impl Revisioned for BoundedDuration {
	fn revision() -> u16 {
		<time::Duration as Revisioned>::revision()
	}
}

impl SerializeRevisioned for BoundedDuration {
	#[inline]
	fn serialize_revisioned<W: Write>(&self, writer: &mut W) -> Result<(), Error> {
		self.0.serialize_revisioned(writer)
	}
}

impl DeserializeRevisioned for BoundedDuration {
	#[inline]
	fn deserialize_revisioned<R: Read>(reader: &mut R) -> Result<Self, Error> {
		read_std_duration(reader).map(Self)
	}
}

impl SkipRevisioned for BoundedDuration {
	#[inline]
	fn skip_revisioned<R: Read>(reader: &mut R) -> Result<(), Error> {
		Self::deserialize_revisioned(reader).map(drop)
	}

	#[inline]
	fn skip_revisioned_slice(
		reader: &mut revision::SliceReader<'_>,
	) -> Result<(), revision::Error> {
		let mut rest = reader.remaining();
		let before = rest.len();
		Self::deserialize_revisioned(&mut rest)?;
		reader.consume(before - rest.len())
	}
}

impl WalkRevisioned for BoundedDuration {
	type Walker<'r, R: BorrowedReader + 'r> = LeafWalker<'r, BoundedDuration, R>;

	#[inline]
	fn walk_revisioned<'r, R: BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, Error> {
		Ok(LeafWalker::new(reader))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn slice_skip_agrees_with_reader_skip() {
		let value = BoundedDuration(time::Duration::new(90_000, 123_456_789));
		let mut bytes = Vec::new();
		value.serialize_revisioned(&mut bytes).unwrap();
		assert_eq!(revision::skip_slice::<BoundedDuration>(&bytes).unwrap(), bytes.len());
		let cut = &bytes[..bytes.len() - 1];
		assert!(revision::skip_slice::<BoundedDuration>(cut).is_err());
		assert!(BoundedDuration::skip_revisioned(&mut &cut[..]).is_err());
	}
}
