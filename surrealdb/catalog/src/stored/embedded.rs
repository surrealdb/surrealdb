//! A catalog entry held inside a `revision` container.
//!
//! A few `revision` containers hold a copy of a catalog entry: a changefeed's table mutation holds
//! a table definition, and an async-event record holds an event definition. The copy is an
//! [`EmbeddedEntry<T>`], which is in one of two forms:
//!
//! ```text
//! embedded = T's revision bytes                    when the value fits `revision` exactly
//!          | FE, len, entry                        otherwise
//! len      = the revision varint of entry's length
//! entry    = a complete catalog entry, written at this build's release with no prior
//! ```
//!
//! A value fits `revision` when it has no non-default `Unrevisioned` field. Such a value can exist
//! only in a store whose feature level allows the field, so a container written at the Legacy level
//! holds `revision` bytes that every 3.x reader reads. A 3.x reader that meets the marker where
//! the type's revision header belongs fails with `InvalidIntegerEncoding`: a refusal, never a
//! misread.
//!
//! A copy holds no unknown ignorable fields, because an in-memory value carries none. It is a
//! snapshot of what this build understands.
//!
//! Every read proves the extent of the copy against the bytes present before it allocates, and the
//! frame's entry is read through [`surrealdb_collections::bounded`]. A refusal of the frame's entry
//! keeps its type through a `revision` container's read, as the I/O error that
//! [`EntryError::of_cause`] recovers.

use std::io::{Read, Write};
use std::ops::Deref;

use revision::{
	BorrowedReader, DeserializeRevisioned, Error as RevisionError, LeafWalker, Revisioned,
	SerializeRevisioned, SkipCheckRevisioned, SkipRevisioned, SliceReader, WalkRevisioned,
	advance_read,
};
use surrealdb_collections::bounded;
use surrealdb_expr::val::stored::schema::StoredType;
use surrealdb_kvs::{EncodeContext, FeatureLevel};

use super::entry::{CATALOG_MARKER, decode_entry, encode_stored, read_revision_prefix};
use super::error::EntryError;
use crate::Error;

/// A catalog entry inside a `revision` container: see the [module documentation](self).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EmbeddedEntry<T>(pub Box<T>);

impl<T> EmbeddedEntry<T> {
	/// The held value.
	pub fn into_inner(self) -> T {
		*self.0
	}
}

impl<T> Deref for EmbeddedEntry<T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.0
	}
}

impl<T> From<T> for EmbeddedEntry<T> {
	fn from(value: T) -> Self {
		Self(Box::new(value))
	}
}

/// Writes `value` as a container copy: its `revision` bytes when it fits, else the framed entry.
pub(crate) fn write_embedded<T, W>(value: &T, writer: &mut W) -> anyhow::Result<()>
where
	T: StoredType + SerializeRevisioned,
	W: Write,
{
	if value.unrevisioned_floor().is_none() {
		value.serialize_revisioned(writer)?;
		return Ok(());
	}
	let entry = encode_stored(value, &EncodeContext::CURRENT).map_err(Error::Entry)?;
	writer.write_all(&[CATALOG_MARKER]).map_err(RevisionError::Io)?;
	entry.bytes.len().serialize_revisioned(writer)?;
	writer.write_all(&entry.bytes).map_err(RevisionError::Io)?;
	Ok(())
}

/// Writes `value` as a container copy of a type that is never framed: its `revision` bytes, which
/// are refused for a value that `revision` cannot hold.
pub(crate) fn write_plain<T, W>(value: &T, writer: &mut W) -> anyhow::Result<()>
where
	T: StoredType + SerializeRevisioned,
	W: Write,
{
	if let Some(needed) = value.unrevisioned_floor() {
		return Err(Error::Entry(EntryError::AboveLevel {
			ty: T::SCHEMA.name,
			needed,
			level: FeatureLevel::Legacy,
		})
		.into());
	}
	value.serialize_revisioned(writer)?;
	Ok(())
}

/// Reads a container copy from the front of `cursor` and advances `cursor` past it.
///
/// The copy's extent is proved against the bytes present before anything is read, and a refusal
/// of the framed entry keeps its type.
pub(crate) fn read_embedded<T>(cursor: &mut &[u8]) -> anyhow::Result<T>
where
	T: StoredType + DeserializeRevisioned + SkipRevisioned,
{
	let bytes: &[u8] = cursor;
	match bytes.split_first() {
		Some((&CATALOG_MARKER, after_marker)) => {
			let mut rest = after_marker;
			let len = usize::deserialize_revisioned(&mut rest)?;
			let (entry, tail) = rest
				.split_at_checked(len)
				.ok_or_else(|| RevisionError::Io(std::io::ErrorKind::UnexpectedEof.into()))?;
			let value = decode_entry::<T>(entry).map_err(Error::Entry)?;
			*cursor = tail;
			Ok(value)
		}
		_ => {
			let (value, tail) = read_revision_prefix::<T>(bytes)?;
			*cursor = tail;
			Ok(value)
		}
	}
}

impl<T: Revisioned> Revisioned for EmbeddedEntry<T> {
	fn revision() -> u16 {
		T::revision()
	}
}

impl<T: StoredType + SerializeRevisioned> SerializeRevisioned for EmbeddedEntry<T> {
	fn serialize_revisioned<W: Write>(&self, writer: &mut W) -> Result<(), RevisionError> {
		write_embedded(&*self.0, writer).map_err(|error| match error.downcast::<RevisionError>() {
			Ok(error) => error,
			Err(error) => RevisionError::Serialize(format!("{error:#}")),
		})
	}
}

impl<T: StoredType + DeserializeRevisioned> DeserializeRevisioned for EmbeddedEntry<T> {
	fn deserialize_revisioned<R: Read>(reader: &mut R) -> Result<Self, RevisionError> {
		let mut first = [0u8; 1];
		reader.read_exact(&mut first).map_err(RevisionError::Io)?;
		if first == [CATALOG_MARKER] {
			let len = bounded::read_len(reader)?;
			let entry = bounded::read_bytes(reader, len)?;
			let value =
				decode_entry::<T>(&entry).map_err(|error| RevisionError::Io(error.into_io()))?;
			Ok(Self(Box::new(value)))
		} else {
			let mut chained = first.as_slice().chain(reader);
			T::deserialize_revisioned(&mut chained).map(|value| Self(Box::new(value)))
		}
	}
}

impl<T: StoredType + SkipRevisioned> SkipRevisioned for EmbeddedEntry<T> {
	fn skip_revisioned<R: Read>(reader: &mut R) -> Result<(), RevisionError> {
		let mut first = [0u8; 1];
		reader.read_exact(&mut first).map_err(RevisionError::Io)?;
		if first == [CATALOG_MARKER] {
			let len = bounded::read_len(reader)?;
			advance_read(reader, len)
		} else {
			T::skip_revisioned(&mut first.as_slice().chain(reader))
		}
	}

	fn skip_revisioned_slice(reader: &mut SliceReader<'_>) -> Result<(), RevisionError> {
		if reader.remaining().first() == Some(&CATALOG_MARKER) {
			reader.consume(1)?;
			let len = bounded::read_len(reader)?;
			reader.consume(len)
		} else {
			T::skip_revisioned_slice(reader)
		}
	}
}

impl<T: StoredType + DeserializeRevisioned> SkipCheckRevisioned for EmbeddedEntry<T> {
	fn skip_check_revisioned<R: Read>(reader: &mut R) -> Result<(), RevisionError> {
		Self::deserialize_revisioned(reader).map(drop)
	}
}

impl<T: Revisioned> WalkRevisioned for EmbeddedEntry<T> {
	type Walker<'r, R: BorrowedReader + 'r> = LeafWalker<'r, EmbeddedEntry<T>, R>;

	#[inline]
	fn walk_revisioned<'r, R: BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, RevisionError> {
		Ok(LeafWalker::new(reader))
	}
}
