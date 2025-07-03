//! Stores sequence batches for FullTextIndex / DocIDs
use crate::key::category::{Categorise, Category};
use crate::kvs::{KeyEncode, impl_key};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Ib<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	pub ix: &'a str,
	_e: u8,
	_f: u8,
	_g: u8,
	pub start: i64,
}
impl_key!(Ib<'a>);

impl Categorise for Ib<'_> {
	fn categorise(&self) -> Category {
		Category::SequenceBatch
	}
}

impl<'a> Ib<'a> {
	pub(crate) fn new(ns: &'a str, db: &'a str, tb: &'a str, ix: &'a str, start: i64) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'+',
			ix,
			_e: b'!',
			_f: b'i',
			_g: b'b',
			start,
		}
	}

	pub(crate) fn new_range(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		ix: &'a str,
	) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
		let mut beg = Self::new(ns, db, tb, ix, b'i', b'b').encode()?;
		let mut end = Self::new(ns, db, tb, ix, b'i', b'b').encode()?;
		beg.extend_from_slice(&[0x00; 9]);
		end.extend_from_slice(&[0xFF; 9]);
		Ok((beg, end))
	}
}
