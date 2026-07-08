use std::mem;
use std::sync::Arc;

use anyhow::Result;

use crate::key::{KVValue, Key, KeyRange};
use crate::kvs::Transaction;

/// Takes an iterator of byte slices and deserializes the byte slices to the
/// expected type, returning an error if any of the values fail to serialize.
///
/// Bound to `KeyContext = ()` to prevent accidental use on `Record`
/// (whose decode requires a `RecordId` from the storage key).
pub fn deserialize_cache<'a, I, T>(iter: I) -> Result<Arc<[T]>>
where
	T: KVValue<KeyContext = ()>,
	I: Iterator<Item = &'a [u8]>,
{
	let mut buf = Vec::new();
	for slice in iter {
		buf.push(T::kv_decode_value(slice, ())?)
	}
	Ok(Arc::from(buf))
}

/// Returns true if the range can only contain a single key.
///
/// This can be the case if range consists of keys x as the start and key x ++ 0x00 as the end.
fn range_is_single_key(range: &KeyRange<'_>) -> bool {
	range.start.as_slice().len() == range.end.as_slice().len() - 1
		&& range.start.as_slice() == &range.end.as_slice()[..range.end.len() - 1]
		&& range.end.as_slice()[range.end.len() - 1] == 0
}

pub async fn scan(
	range: &mut KeyRange<'static>,
	tx: &Transaction,
	limit: u32,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	if range.is_empty() {
		return Ok(Vec::new());
	}

	// Fast path to avoid a full scan if the key can only be a single value.
	// Avoids costly iterator creation on rocksdb.
	//
	// FIXME: The kvs themselves should probably be the one to implement this optimisation
	if range_is_single_key(range) {
		let key = mem::replace(&mut range.start, Key::empty());
		let res = if let Some(res) = tx.get(key.as_borrowed(), None).await? {
			vec![(key.into_vec(), res)]
		} else {
			Vec::new()
		};
		range.end = Key::empty();
		return Ok(res);
	}

	let res = tx.scan(range.as_borrowed(), limit, 0, None).await?;

	if limit as usize != res.len() {
		range.end = Key::empty();
	} else if let Some((key, _)) = res.last() {
		range.start.clone_from_slice(key);
		range.start.advance();
	}

	Ok(res)
}

pub async fn scan_keys(
	range: &mut KeyRange<'static>,
	tx: &Transaction,
	limit: u32,
) -> Result<Vec<Vec<u8>>> {
	if range.is_empty() {
		return Ok(Vec::new());
	}

	// Fast path to avoid a full scan if the key can only be a single value.
	// Avoids costly iterator creation on rocksdb.
	if range_is_single_key(range) {
		let key = mem::replace(&mut range.start, Key::empty());
		let res = if tx.exists(key.as_borrowed(), None).await? {
			vec![key.into_vec()]
		} else {
			Vec::new()
		};
		range.end = Key::empty();
		return Ok(res);
	}

	let res = tx.keys(range.as_borrowed(), limit, 0, None).await?;

	if limit as usize != res.len() {
		range.end = Key::empty();
	} else if let Some(key) = res.last() {
		range.start.clone_from_slice(key);
		range.start.advance();
	}

	Ok(res)
}

pub async fn scanr(
	range: &mut KeyRange<'static>,
	tx: &Transaction,
	limit: u32,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	if range.is_empty() {
		return Ok(Vec::new());
	}

	// Fast path to avoid a full scan if the key can only be a single value.
	// Avoids costly iterator creation on rocksdb.
	if range_is_single_key(range) {
		let key = mem::replace(&mut range.start, Key::empty());
		let res = if let Some(res) = tx.get(key.as_borrowed(), None).await? {
			vec![(key.into_vec(), res)]
		} else {
			Vec::new()
		};
		range.end = Key::empty();
		return Ok(res);
	}

	let res = tx.scanr(range.as_borrowed(), limit, 0, None).await?;

	if limit as usize != res.len() {
		range.end = Key::empty();
	} else if let Some((key, _)) = res.last() {
		range.end.clone_from_slice(key);
	}

	Ok(res)
}

pub async fn scanr_keys(
	range: &mut KeyRange<'static>,
	tx: &Transaction,
	limit: u32,
) -> Result<Vec<Vec<u8>>> {
	if range.is_empty() {
		return Ok(Vec::new());
	}

	// Fast path to avoid a full scan if the key can only be a single value.
	// Avoids costly iterator creation on rocksdb.
	if range_is_single_key(range) {
		let key = mem::replace(&mut range.start, Key::empty());
		let res = if tx.exists(key.as_borrowed(), None).await? {
			vec![key.into_vec()]
		} else {
			Vec::new()
		};
		range.end = Key::empty();
		return Ok(res);
	}

	let res = tx.keysr(range.as_borrowed(), limit, 0, None).await?;

	if limit as usize != res.len() {
		range.end = Key::empty();
	} else if let Some(key) = res.last() {
		range.end.clone_from_slice(key);
	}

	Ok(res)
}
