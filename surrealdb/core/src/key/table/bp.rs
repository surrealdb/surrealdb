//! Stores the primary appending ticket for a record during initial index build.
//!
//! `!bp{ix}{generation}{record}` points from a record to its first durable
//! appending ticket. During the initial scan, this lets the builder index the
//! queued old state instead of a newer record version.
use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::IndexId;
use crate::key::category::{Categorise, Category};
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKey, KVKeyDecode, KVRange, Key, key};
use crate::kvs::index::{BuildGeneration, PrimaryAppendingTicket};
use crate::val::{IndexFormat, RecordIdKey, TableName};

key! {
	/// A key mapping a record to its first queued mutation ticket.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct Bp<'a> for IndexFormat{
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'p',
		pub ix: IndexId,
		pub generation: BuildGeneration,
		pub id: Cow<'a,RecordIdKey>,
	}
}

impl KVKey for Bp<'_> {
	type Value = PrimaryAppendingTicket;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> anyhow::Result<()> {
		storekey::encode_format::<IndexFormat, _, _>(&mut *buffer, self)
			.map_err(|_| crate::key::Error::Unencodable)?;
		Ok(())
	}

	fn value_context(&self) {}
}
impl<'a> KVKeyDecode<'a> for Bp<'a> {
	fn decode_key(bytes: &'a [u8]) -> anyhow::Result<Self> {
		Ok(storekey::decode_borrow_format::<IndexFormat, _>(bytes).map_err(|_| {
			crate::key::Error::Corrupted("Index build generation key cannot be decoded")
		})?)
	}
}

impl Categorise for Bp<'_> {
	fn categorise(&self) -> Category {
		Category::IndexBuildPrimaryAppending
	}
}

key! {
	/// A key mapping a record to its first queued mutation ticket.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub struct BpIdPrefix<'a> for IndexFormat{
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'p',
		pub ix: IndexId,
		pub generation: BuildGeneration,
	}
}

impl KVRange for BpIdPrefix<'_> {
	fn encode_bound(&self) -> Result<Key<'static>> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map(Key::from)
			.map_err(|_| crate::key::Error::Unencodable)?)
	}
}

key! {
	/// A key mapping a record to its first queued mutation ticket.
	#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
	pub(crate) struct BpGenerationPrefix<'a> for IndexFormat{
		pub prefix: DatabaseRoot,
		b'*',
		pub tb: Cow<'a, TableName>,
		b'!',
		b'b',
		b'p',
		pub ix: IndexId,
	}
}

impl KVRange for BpGenerationPrefix<'_> {
	fn encode_bound(&self) -> anyhow::Result<Key<'static>> {
		Ok(storekey::encode_vec_format::<IndexFormat, _>(self)
			.map(Key::from)
			.map_err(|_| crate::key::Error::Unencodable)?)
	}
}
