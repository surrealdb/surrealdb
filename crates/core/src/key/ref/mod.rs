//! Stores a graph edge pointer
use crate::key::category::{Categorise, Category};
use crate::kvs::{KeyEncode, impl_key};
use crate::val::RecordIdKey;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Prefix<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
}
impl_key!(Prefix<'a>);

impl<'a> Prefix<'a> {
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &RecordIdKey) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'&',
			id: id.clone(),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PrefixFt<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
	pub ft: &'a str,
}
impl_key!(PrefixFt<'a>);

// Code here is used in references which is temporarly disabled
#[allow(dead_code)]
impl<'a> PrefixFt<'a> {
	#[allow(dead_code)]
	fn new(ns: &'a str, db: &'a str, tb: &'a str, id: &RecordIdKey, ft: &'a str) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'&',
			id: id.clone(),
			ft,
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PrefixFf<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
	pub ft: &'a str,
	pub ff: &'a str,
}
impl_key!(PrefixFf<'a>);

// Code here is used in references which is temporarly removed
#[allow(dead_code)]
impl<'a> PrefixFf<'a> {
	#[allow(dead_code)]
	fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		id: &RecordIdKey,
		ft: &'a str,
		ff: &'a str,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'&',
			id: id.clone(),
			ft,
			ff,
		}
	}
}

// The order in this key is made so we can scan:
// - all references for a given record
// - all references for a given record, filtered by a origin table
// - all references for a given record, filtered by a origin table and an origin field

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Ref<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub id: RecordIdKey,
	pub ft: &'a str,
	pub ff: &'a str,
	pub fk: RecordIdKey,
}
impl_key!(Ref<'a>);

pub fn new<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	id: &RecordIdKey,
	ft: &'a str,
	ff: &'a str,
	fk: &RecordIdKey,
) -> Ref<'a> {
	Ref::new(ns, db, tb, id.to_owned(), ft, ff, fk.to_owned())
}

pub fn prefix(ns: &str, db: &str, tb: &str, id: &RecordIdKey) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_owned()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

pub fn suffix(ns: &str, db: &str, tb: &str, id: &RecordIdKey) -> Result<Vec<u8>> {
	let mut k = Prefix::new(ns, db, tb, id).encode_owned()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

#[allow(dead_code)]
pub fn ftprefix(ns: &str, db: &str, tb: &str, id: &RecordIdKey, ft: &str) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, ft).encode_owned()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

#[allow(dead_code)]
pub fn ftsuffix(ns: &str, db: &str, tb: &str, id: &RecordIdKey, ft: &str) -> Result<Vec<u8>> {
	let mut k = PrefixFt::new(ns, db, tb, id, ft).encode_owned()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

#[allow(dead_code)]
pub fn ffprefix(
	ns: &str,
	db: &str,
	tb: &str,
	id: &RecordIdKey,
	ft: &str,
	ff: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFf::new(ns, db, tb, id, ft, ff).encode()?;
	k.extend_from_slice(&[0x00]);
	Ok(k)
}

#[allow(dead_code)]
pub fn ffsuffix(
	ns: &str,
	db: &str,
	tb: &str,
	id: &RecordIdKey,
	ft: &str,
	ff: &str,
) -> Result<Vec<u8>> {
	let mut k = PrefixFf::new(ns, db, tb, id, ft, ff).encode()?;
	k.extend_from_slice(&[0xff]);
	Ok(k)
}

impl Categorise for Ref<'_> {
	fn categorise(&self) -> Category {
		Category::Ref
	}
}

impl<'a> Ref<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		id: RecordIdKey,
		ft: &'a str,
		ff: &'a str,
		fk: RecordIdKey,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'&',
			id,
			ft,
			ff,
			fk,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::kvs::KeyDecode as _;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
		let val = Ref::new(
			"testns",
			"testdb",
			"testtb",
			RecordIdKey::String("testid".to_owned()),
			"othertb",
			"test.*",
			RecordIdKey::String("otherid".to_owned()),
		);
		let enc = Ref::encode(&val).unwrap();
		assert_eq!(
			enc,
			b"/*testns\0*testdb\0*testtb\x00&\0\0\0\x01testid\0othertb\0test.*\0\0\0\0\x01otherid\0"
		);

		let dec = Ref::decode(&enc).unwrap();
		assert_eq!(val, dec);
	}
}
