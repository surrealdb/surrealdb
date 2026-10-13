use std::sync::Arc;

use anyhow::Result;
use surrealdb_kvs::KeyRange;
use surrealdb_kvs::key::{AnyRange, Resumable};
use surrealdb_kvs::value::KVValue;

use crate::{Direction, Transaction};

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

/// Moves whichever end of `range` a scan in `dir` reads from past `key`, so the
/// next read over it starts after the entry `key` addresses.
fn resume<R>(range: &mut R, key: &[u8], dir: Direction)
where
	R: Resumable + Clone,
{
	*range = range.clone().resume_after(key, dir);
}

/// Narrows `range` to nothing, so a later read over it returns no entries.
///
/// A forward scan is drained by moving its start past its end, a backward scan by
/// pulling its end back to its start; either way the range can no longer contain a
/// key, which is what the emptiness check each helper opens with reads.
fn exhaust<R>(range: &mut R, bytes: &KeyRange<'_>, dir: Direction)
where
	R: Resumable + Clone,
{
	let edge = match dir {
		Direction::Forward => bytes.end.as_slice(),
		Direction::Backward => bytes.start.as_slice(),
	};
	resume(range, edge, dir);
}

/// Reads up to `limit` entries from the start of `range` as bytes, narrowing it in
/// place to what is left to read. Once the scan has drained the range it is left
/// empty, so a further call over it reads nothing.
pub async fn scan<R>(range: &mut R, tx: &Transaction, limit: u32) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
where
	R: AnyRange + Resumable + Clone,
{
	let bytes = range.clone().into_key_range();
	if bytes.is_empty() {
		return Ok(Vec::new());
	}

	// Fast path to avoid a full scan if the key can only be a single value.
	// Avoids costly iterator creation on rocksdb.
	//
	// FIXME: The kvs themselves should probably be the one to implement this optimisation
	if range_is_single_key(&bytes) {
		let res = if let Some(res) = tx.get(bytes.start.as_borrowed(), None).await? {
			vec![(bytes.start.as_slice().to_vec(), res)]
		} else {
			Vec::new()
		};
		exhaust(range, &bytes, Direction::Forward);
		return Ok(res);
	}

	let res = tx.scan_raw(range.clone(), limit, 0, None).await?;

	if limit as usize != res.len() {
		exhaust(range, &bytes, Direction::Forward);
	} else if let Some((key, _)) = res.last() {
		resume(range, key, Direction::Forward);
	}

	Ok(res)
}

/// As [`scan`], reading only the keys.
pub async fn scan_keys<R>(range: &mut R, tx: &Transaction, limit: u32) -> Result<Vec<Vec<u8>>>
where
	R: AnyRange + Resumable + Clone,
{
	let bytes = range.clone().into_key_range();
	if bytes.is_empty() {
		return Ok(Vec::new());
	}

	// Fast path to avoid a full scan if the key can only be a single value.
	// Avoids costly iterator creation on rocksdb.
	if range_is_single_key(&bytes) {
		let res = if tx.exists(bytes.start.as_borrowed(), None).await? {
			vec![bytes.start.as_slice().to_vec()]
		} else {
			Vec::new()
		};
		exhaust(range, &bytes, Direction::Forward);
		return Ok(res);
	}

	let res = tx.keys_raw(range.clone(), limit, 0, None).await?;

	if limit as usize != res.len() {
		exhaust(range, &bytes, Direction::Forward);
	} else if let Some(key) = res.last() {
		resume(range, key, Direction::Forward);
	}

	Ok(res)
}

/// As [`scan`], reading from the end of `range` backwards.
pub async fn scanr<R>(
	range: &mut R,
	tx: &Transaction,
	limit: u32,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
where
	R: AnyRange + Resumable + Clone,
{
	let bytes = range.clone().into_key_range();
	if bytes.is_empty() {
		return Ok(Vec::new());
	}

	// Fast path to avoid a full scan if the key can only be a single value.
	// Avoids costly iterator creation on rocksdb.
	if range_is_single_key(&bytes) {
		let res = if let Some(res) = tx.get(bytes.start.as_borrowed(), None).await? {
			vec![(bytes.start.as_slice().to_vec(), res)]
		} else {
			Vec::new()
		};
		exhaust(range, &bytes, Direction::Backward);
		return Ok(res);
	}

	let res = tx.scanr_raw(range.clone(), limit, 0, None).await?;

	if limit as usize != res.len() {
		exhaust(range, &bytes, Direction::Backward);
	} else if let Some((key, _)) = res.last() {
		resume(range, key, Direction::Backward);
	}

	Ok(res)
}

/// As [`scanr`], reading only the keys.
pub async fn scanr_keys<R>(range: &mut R, tx: &Transaction, limit: u32) -> Result<Vec<Vec<u8>>>
where
	R: AnyRange + Resumable + Clone,
{
	let bytes = range.clone().into_key_range();
	if bytes.is_empty() {
		return Ok(Vec::new());
	}

	// Fast path to avoid a full scan if the key can only be a single value.
	// Avoids costly iterator creation on rocksdb.
	if range_is_single_key(&bytes) {
		let res = if tx.exists(bytes.start.as_borrowed(), None).await? {
			vec![bytes.start.as_slice().to_vec()]
		} else {
			Vec::new()
		};
		exhaust(range, &bytes, Direction::Backward);
		return Ok(res);
	}

	let res = tx.keysr_raw(range.clone(), limit, 0, None).await?;

	if limit as usize != res.len() {
		exhaust(range, &bytes, Direction::Backward);
	} else if let Some(key) = res.last() {
		resume(range, key, Direction::Backward);
	}

	Ok(res)
}
