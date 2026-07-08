//! Index key encoding and prefixes for the KV store.
//!
//! This module defines the on-disk key layout for secondary indexes and helpers
//! to construct prefixes and full keys. Field values are serialized via
//! key::value::Array, which normalizes numeric values across Number
//! variants (Int/Float/Decimal) using a lexicographic encoding so that byte
//! order aligns with numeric order. As a consequence, numerically-equal values
//! (e.g., 0, 0.0, 0dec) map to identical key bytes and are treated as equal by
//! UNIQUE indexes and during scans.
//!
//! Helper functions like prefix_beg/prefix_end/prefix_ids_* build range bounds
//! for scanning the KV store. Keys are designed to be concatenation-friendly,
//! using zero-terminated components where appropriate to ensure parsers stop at
//! the correct boundaries when decoding.
pub mod all;
pub mod dc;
#[cfg(diskann)]
pub mod dd;
#[cfg(diskann)]
pub mod de;
#[cfg(diskann)]
pub mod dg;
#[cfg(diskann)]
pub mod dh;
#[cfg(diskann)]
pub mod di;
pub mod dl;
#[cfg(diskann)]
pub mod dn;
// `!dp` is the legacy pending-state guard; the current sharded layout tracks pending state in
// `!dy` (see `dy`). New code never constructs `!dp` keys — old nodes wrote them and tests
// construct them to simulate a pre-change node — so its items read as dead in a non-test build.
#[cfg(diskann)]
#[cfg(diskann)]
pub mod dp;
#[cfg(diskann)]
pub mod dq;
#[cfg(diskann)]
pub mod dr;
#[cfg(diskann)]
pub mod ds;
pub mod dv;
#[cfg(diskann)]
pub mod dw;
#[cfg(diskann)]
pub mod dy;
pub mod hd;
pub mod he;
pub mod hg;
pub mod hh;
pub mod hi;
pub mod hl;
pub mod hn;
pub mod hp;
pub mod hr;
pub mod hs;
pub mod hv;
pub mod ib;
pub mod id;
pub mod ig;
pub mod ii;
pub mod ip;
pub mod is;
pub mod iu;
pub mod iv;
pub mod td;
pub mod tt;
pub mod tv;

use std::borrow::Cow;
use std::io::Write;

use anyhow::Result;
use storekey::{BorrowDecode, DecodeError, Encode, EncodeError, Writer};

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, KVRange, Key, key};
use crate::val::{IndexFormat, RecordId, RecordIdKey, TableName, Value};

key! {
	#[derive(Clone, Debug, PartialEq, PartialOrd)]
	pub struct IndexPrefix<'a> for IndexFormat {
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'+',
		pub ix: IndexId,
		b'*',
	}
}

impl KVRange for IndexPrefix<'_> {
	fn encode_bound(&self) -> Result<super::Key<'static>> {
		let data = storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(Key::from(data))
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct IndexPrefixTerminated<'a> {
	pub prefix: DatabaseRoot,
	pub tb: Cow<'a, TableName>,
	pub ix: IndexId,
	pub fd: Cow<'a, [Value]>,
}

impl<'a> Encode<IndexFormat> for IndexPrefixTerminated<'a> {
	fn encode<W>(&self, w: &mut Writer<W>) -> Result<(), EncodeError>
	where
		W: Write,
	{
		Encode::<IndexFormat>::encode(&self.prefix, w)?;
		Encode::<IndexFormat>::encode(&b'*', w)?;
		Encode::<IndexFormat>::encode(&self.tb, w)?;
		Encode::<IndexFormat>::encode(&b'+', w)?;
		Encode::<IndexFormat>::encode(&self.ix, w)?;
		Encode::<IndexFormat>::encode(&b'*', w)?;
		Encode::<IndexFormat>::encode(&self.fd, w)?;
		Ok(())
	}
}

impl KVRange for IndexPrefixTerminated<'_> {
	fn encode_bound(&self) -> Result<super::Key<'static>> {
		let data = storekey::encode_vec_format::<IndexFormat, _>(self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(Key::from(data))
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct IndexPrefixUnterminated<'a> {
	pub prefix: DatabaseRoot,
	pub tb: Cow<'a, TableName>,
	pub ix: IndexId,
	/// Encoded index field values. Uses Array which normalizes numeric
	/// types (Int/Float/Decimal) into a lexicographically ordered byte form so
	/// equal numeric values compare equal in index keys.
	pub fd: Cow<'a, [Value]>,
}

impl Encode<IndexFormat> for IndexPrefixUnterminated<'_> {
	fn encode<W: Write>(&self, w: &mut Writer<W>) -> Result<(), storekey::EncodeError> {
		Encode::<IndexFormat>::encode(&self.prefix, w)?;
		w.write_u8(b'*')?;
		Encode::<IndexFormat>::encode(&self.tb, w)?;
		w.write_u8(b'+')?;
		Encode::<IndexFormat>::encode(&self.ix, w)?;
		w.write_u8(b'*')?;
		for v in self.fd.iter() {
			w.mark_terminator();
			Encode::<IndexFormat>::encode(v, w)?;
		}
		// We do not write the terminating value as would be required if this was a normal key
		// because this key will be used as a prefix of a partial array.
		Ok(())
	}
}
impl KVRange for IndexPrefixUnterminated<'_> {
	fn encode_bound(&self) -> Result<crate::key::Key<'static>> {
		let key = storekey::encode_vec_format::<IndexFormat, Self>(self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(crate::key::Key::from(key))
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub(crate) struct Index<'a> {
	pub prefix: DatabaseRoot,
	pub tb: Cow<'a, TableName>,
	pub ix: IndexId,
	/// Encoded index field values. Uses Array which normalizes numeric
	/// types (Int/Float/Decimal) into a lexicographically ordered byte form so
	/// equal numeric values compare equal in index keys.
	///
	/// This is an inefficient design as we are using a dynamic length for the key yet we
	/// can know the amount of values that are part of the key as this is static for an index.
	///
	/// However we cannot changes this without breaking existing indexes.
	pub fd: Cow<'a, [Value]>,
	pub id: Cow<'a, RecordIdKey>,
}

impl KVKey for Index<'_> {
	type Value = RecordId;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> ::anyhow::Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}
impl<'a> KVKeyDecode<'a> for Index<'a> {
	fn decode_key(bytes: &'a [u8]) -> anyhow::Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes)
			.map_err(|_| crate::err::Error::Corrupted("Index key cannot be decoded"))?)
	}
}

impl Categorise for Index<'_> {
	fn categorise(&self) -> Category {
		Category::Index
	}
}

// Manual implementation as storekey doesn't implement encoding for `Cow<'a,[T]>`
impl Encode<IndexFormat> for Index<'_> {
	fn encode<W: Write>(&self, w: &mut Writer<W>) -> Result<(), storekey::EncodeError> {
		Encode::<IndexFormat>::encode(&self.prefix, w)?;
		w.write_u8(b'*')?;
		Encode::<IndexFormat>::encode(&self.tb, w)?;
		w.write_u8(b'+')?;
		Encode::<IndexFormat>::encode(&self.ix, w)?;
		w.write_u8(b'*')?;
		for v in self.fd.iter() {
			w.mark_terminator();
			Encode::<IndexFormat>::encode(v, w)?;
		}
		w.write_terminator()?;

		// Required for backwards compatiblity.
		// Previously UniqueIndex and Index used the same key, the id field would be none for unique
		// indecies and Some for non-unique indecies. This value is to replicate the discriminator
		// of Some
		w.write_u8(3)?;
		Encode::<IndexFormat>::encode(&self.id, w)?;

		Ok(())
	}
}

impl<'de> BorrowDecode<'de, IndexFormat> for Index<'de> {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> Result<Self, DecodeError> {
		let prefix = BorrowDecode::<IndexFormat>::borrow_decode(r)?;
		if r.read_u8()? != b'*' {
			return Err(DecodeError::InvalidFormat);
		}
		let tb = BorrowDecode::<IndexFormat>::borrow_decode(r)?;
		if r.read_u8()? != b'+' {
			return Err(DecodeError::InvalidFormat);
		}
		let ix = BorrowDecode::<IndexFormat>::borrow_decode(r)?;
		if r.read_u8()? != b'*' {
			return Err(DecodeError::InvalidFormat);
		}
		let fd = Cow::Owned(BorrowDecode::<IndexFormat>::borrow_decode(r)?);
		if r.read_u8()? != 3 {
			return Err(DecodeError::InvalidFormat);
		}
		let id = BorrowDecode::<IndexFormat>::borrow_decode(r)?;
		Ok(Index {
			prefix,
			tb,
			ix,
			fd,
			id,
		})
	}
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub(crate) struct UniqueIndex<'a> {
	pub prefix: DatabaseRoot,
	pub tb: Cow<'a, TableName>,
	pub ix: IndexId,
	/// Encoded index field values. Uses Array which normalizes numeric
	/// types (Int/Float/Decimal) into a lexicographically ordered byte form so
	/// equal numeric values compare equal in index keys.
	///
	/// This is an inefficient design as we are using a dynamic length for the key yet we
	/// can know the amount of values that are part of the key as this is static for an index.
	///
	/// However we cannot changes this without breaking existing indexes.
	pub fd: Cow<'a, [Value]>,
}

// Manual implementation as storekey doesn't implement encoding for `Cow<'a,[T]>`
impl Encode<IndexFormat> for UniqueIndex<'_> {
	fn encode<W: Write>(&self, w: &mut Writer<W>) -> Result<(), storekey::EncodeError> {
		Encode::<IndexFormat>::encode(&self.prefix, w)?;
		w.write_u8(b'*')?;
		Encode::<IndexFormat>::encode(&self.tb, w)?;
		w.write_u8(b'+')?;
		Encode::<IndexFormat>::encode(&self.ix, w)?;
		w.write_u8(b'*')?;
		for v in self.fd.iter() {
			w.mark_terminator();
			Encode::<IndexFormat>::encode(v, w)?;
		}
		w.write_terminator()?;

		// Required for backwards compatiblity.
		// Previously UniqueIndex and Index used the same key, the id field would be none for unique
		// indecies and Some for non-unique indecies. This value is to replicate the encoding with
		// a trailing None field.
		w.write_u8(2)?;
		Ok(())
	}
}

impl<'de> BorrowDecode<'de, IndexFormat> for UniqueIndex<'de> {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> Result<Self, DecodeError> {
		let prefix = BorrowDecode::<IndexFormat>::borrow_decode(r)?;
		if r.read_u8()? != b'*' {
			return Err(DecodeError::InvalidFormat);
		}
		let tb = BorrowDecode::<IndexFormat>::borrow_decode(r)?;
		if r.read_u8()? != b'+' {
			return Err(DecodeError::InvalidFormat);
		}
		let ix = BorrowDecode::<IndexFormat>::borrow_decode(r)?;
		if r.read_u8()? != b'*' {
			return Err(DecodeError::InvalidFormat);
		}
		let fd = Cow::Owned(BorrowDecode::<IndexFormat>::borrow_decode(r)?);
		if r.read_u8()? != 2 {
			return Err(DecodeError::InvalidFormat);
		}
		Ok(UniqueIndex {
			prefix,
			tb,
			ix,
			fd,
		})
	}
}

impl KVKey for UniqueIndex<'_> {
	type Value = RecordId;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> ::anyhow::Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(buffer, self)
			.map_err(|_| crate::err::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}
impl<'a> KVKeyDecode<'a> for UniqueIndex<'a> {
	fn decode_key(bytes: &'a [u8]) -> anyhow::Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes)
			.map_err(|_| crate::err::Error::Corrupted("Unique Index key cannot be decoded"))?)
	}
}

impl Categorise for UniqueIndex<'_> {
	fn categorise(&self) -> Category {
		Category::Index
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::val::Array;

	#[test]
	fn key() {
		let fd: Array = vec!["testfd1", "testfd2"].into();
		let id = RecordIdKey::String(Strand::new_static("testid"));
		let tb = TableName::from("testtb");
		let val = Index {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			fd: Cow::Borrowed(&fd),
			id: Cow::Borrowed(&id),
		};
		let enc = Index::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\x00\x00\x00\x01*\x00\x00\x00\x02*testtb\0+\0\0\0\x03*\x06testfd1\0\x06testfd2\0\0\x03\x03testid\0"
		);
	}

	#[test]
	fn key_none() {
		let fd: Array = vec!["testfd1", "testfd2"].into();
		let tb = TableName::from("testtb");
		let val = UniqueIndex {
			prefix: DatabaseRoot {
				ns: NamespaceId(1),
				db: DatabaseId(2),
			},
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
			fd: Cow::Borrowed(&fd),
		};
		let enc = UniqueIndex::encode_key(&val).unwrap();
		assert_eq!(
			&*enc,
			b"/*\0\0\0\x01*\0\0\0\x02*testtb\0+\0\0\0\x03*\x06testfd1\0\x06testfd2\0\0\x02"
		);
	}
}
