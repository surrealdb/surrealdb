//! Why a catalog entry could not be read, or could not be written at a store's feature level.

use surrealdb_expr::val::stored::schema::SchemaError;
use surrealdb_expr::val::stored::varint::VarintError;
use surrealdb_kvs::release::{FeatureLevel, Release};

/// A refusal to read or write a catalog entry.
///
/// Every refusal is typed so that an operator is told what to do about it: upgrade the binary
/// ([`EntryError::NeedsRelease`]), raise the store's feature level ([`EntryError::AboveLevel`]), or
/// inspect a damaged value. An error names the entry's Rust type and, for a body refusal, the field
/// number concerned. It never carries a decoded value, because catalog entries hold secrets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EntryError {
	/// The entry holds no bytes, or ends before its header byte.
	#[error("a catalog entry ends before its header")]
	NoHeader,
	/// The entry does not start with the entry marker.
	#[error("a catalog entry starts with {0:#04x}, which is not the entry marker")]
	Marker(u8),
	/// Header bits that no release defines are set.
	#[error("a catalog entry has the reserved header bits {0:#04x} set")]
	ReservedBits(u8),
	/// The release floor is not a canonical varint.
	#[error("a catalog entry's release floor is malformed: {0}")]
	Floor(VarintError),
	/// The release floor is not a release this format can name.
	#[error("a catalog entry names {0:#x} as its release floor, which is no release")]
	NotARelease(u64),
	/// The entry was written by a release newer than this build.
	#[error("a {ty} entry needs release {needed} or later to read; this build is {have}")]
	NeedsRelease {
		ty: &'static str,
		needed: Release,
		have: Release,
	},
	/// The value uses something the store's feature level does not yet allow.
	#[error("this {ty} needs feature level {needed}; the store's feature level is {level}")]
	AboveLevel {
		ty: &'static str,
		needed: Release,
		level: FeatureLevel,
	},
	/// A `revision` entry holds bytes beyond the value it encodes.
	#[error("a {ty} entry in the revision encoding has {extra} trailing bytes")]
	RevisionTrailing {
		ty: &'static str,
		extra: usize,
	},
	/// The value has no `revision` form and was encoded outside a levelled write.
	#[error("a {ty} is written only through a levelled transaction write")]
	Unlevelled {
		ty: &'static str,
	},
	/// The value could not be written in the `revision` encoding.
	#[error("a {ty} could not be written in the revision encoding")]
	RevisionWrite {
		ty: &'static str,
	},
	/// The entry's body is malformed, or the value cannot be encoded.
	#[error("a {ty} entry is malformed: {error}")]
	Body {
		ty: &'static str,
		error: SchemaError,
	},
}

impl EntryError {
	/// The refusal `cause` is or carries, found by a walk of an error's source chain.
	///
	/// A refusal travels as a catalog [`crate::Error::Entry`], or, through a `revision` container
	/// whose error type has no room for one, inside the I/O error that container's read raised
	/// ([`EntryError::into_io`]). The first walks past neither: `std::io::Error` hides its inner
	/// error from `source`, so the walk reads it directly.
	pub fn of_cause(cause: &(dyn std::error::Error + 'static)) -> Option<Self> {
		if let Some(crate::Error::Entry(entry)) = cause.downcast_ref::<crate::Error>() {
			return Some(*entry);
		}
		cause.downcast_ref::<std::io::Error>()?.get_ref()?.downcast_ref::<Self>().copied()
	}

	/// This refusal as the I/O error a `revision` container read raises, which
	/// [`EntryError::of_cause`] recovers.
	pub fn into_io(self) -> std::io::Error {
		std::io::Error::new(std::io::ErrorKind::InvalidData, self)
	}

	/// The refusal of a body or value of `ty`.
	pub(crate) const fn body(ty: &'static str, error: SchemaError) -> Self {
		Self::Body {
			ty,
			error,
		}
	}
}
