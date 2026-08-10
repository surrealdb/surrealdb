use std::collections::btree_map::{BTreeMap, Entry};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};

use surrealdb_kvs::api::{BoxFut, KeysResult, ScanResult, Transactable};
use surrealdb_kvs::{Error as KvsError, Key, KeyRange, Result as KvsResult, TransactionType, Val};
use tokio::sync::Mutex;
use tracing::instrument;

use crate::{ffi, kvs_error};

// Flag values shared with `src/js/db.js`: an entry's flag byte packs the
// write state in the high nibble and the read state in the low nibble.
const READ_UNKNOWN: u8 = 0;
const READ_EXISTS: u8 = 1;
const READ_READ: u8 = 2;
const READ_EMPTY: u8 = 3;

const WRITE_UNCHANGED: u8 = 0;
const WRITE_WRITTEN: u8 = 1;
const WRITE_DELETED: u8 = 2;

/// What this transaction has observed about a key in the database.
///
/// Everything observed is re-validated inside the single IndexedDB
/// transaction that applies the writes at commit time, turning the
/// snapshot-less per-read lookups into optimistic concurrency control.
#[derive(Debug)]
enum ReadState {
	/// The key was read and had this value.
	Read(Vec<u8>),
	/// The key was observed to exist, value unknown.
	Exists,
	/// The key was observed to be absent.
	Empty,
	/// The key was never observed.
	Unknown,
}

/// The pending local mutation of a key.
#[derive(Debug)]
enum WriteState {
	/// The key will be set to this value on commit.
	Written(Vec<u8>),
	/// The key will be deleted on commit.
	Deleted,
	/// No pending mutation.
	Unchanged,
}

#[derive(Debug)]
struct KeyState {
	read: ReadState,
	write: WriteState,
}

/// How a locally-known key affects a range scan.
#[derive(Debug)]
enum Overlay {
	/// The key is visible with this value.
	/// None if we are doing a key only scan.
	Value(Option<Vec<u8>>),
	/// The key is known absent: shadow any database row.
	Hidden,
	/// Nothing useful known locally: use the database row, if any.
	Passthrough,
}

struct State {
	tx: ffi::Tx,
	keys: BTreeMap<Vec<u8>, KeyState>,
	savepoints: Vec<BTreeMap<Vec<u8>, WriteState>>,
}

impl State {
	/// Record what a database lookup observed about a key, without
	/// downgrading existing knowledge or touching the pending write state.
	fn record_read(&mut self, key: &[u8], read: ReadState) {
		if let Some(ent) = self.keys.get_mut(key) {
			match (&ent.read, &read) {
				// Only upgrades: a value read supersedes a bare existence
				// check, anything supersedes no knowledge.
				(ReadState::Unknown, _) | (ReadState::Exists, ReadState::Read(_)) => {
					ent.read = read
				}
				_ => {}
			}
		} else {
			self.keys.insert(
				key.to_vec(),
				KeyState {
					read,
					write: WriteState::Unchanged,
				},
			);
		}
	}

	/// Set the pending write state of a key, capturing the previous state in
	/// the current savepoint the first time the key is touched under it.
	fn set_write(&mut self, key: Vec<u8>, write: WriteState) {
		match self.keys.entry(key) {
			Entry::Occupied(mut ent) => {
				let old = std::mem::replace(&mut ent.get_mut().write, write);
				if let Some(sp) = self.savepoints.last_mut() {
					sp.entry(ent.key().clone()).or_insert(old);
				}
			}
			Entry::Vacant(ent) => {
				if let Some(sp) = self.savepoints.last_mut() {
					sp.entry(ent.key().clone()).or_insert(WriteState::Unchanged);
				}
				ent.insert(KeyState {
					read: ReadState::Unknown,
					write,
				});
			}
		}
	}

	/// Undo every write recorded since the last savepoint. Errors when no
	/// savepoint is on the stack.
	fn rollback_to_savepoint(&mut self) -> KvsResult<()> {
		let Some(sp) = self.savepoints.pop() else {
			return Err(KvsError::NoSavepoint);
		};
		for (key, write) in sp {
			if let Some(ent) = self.keys.get_mut(&key) {
				ent.write = write;
			}
		}
		Ok(())
	}

	/// The value of a key as visible to this transaction, reading through to
	/// the database when the key was not observed yet.
	async fn visible_get(&mut self, key: &[u8]) -> KvsResult<Option<Vec<u8>>> {
		if let Some(x) = self.keys.get(key) {
			match &x.write {
				WriteState::Written(v) => return Ok(Some(v.clone())),
				WriteState::Deleted => return Ok(None),
				WriteState::Unchanged => match &x.read {
					ReadState::Read(v) => return Ok(Some(v.clone())),
					ReadState::Empty => return Ok(None),
					ReadState::Unknown | ReadState::Exists => {}
				},
			}
		}

		match self.tx.read(key).await.map_err(kvs_error)? {
			Some(v) => {
				self.record_read(key, ReadState::Read(v.clone()));
				Ok(Some(v))
			}
			None => {
				self.record_read(key, ReadState::Empty);
				Ok(None)
			}
		}
	}

	/// Whether a key exists as visible to this transaction, reading through
	/// to the database (without fetching the value) when the key was not
	/// observed yet.
	async fn visible_exists(&mut self, key: &[u8]) -> KvsResult<bool> {
		if let Some(x) = self.keys.get(key) {
			match &x.write {
				WriteState::Written(_) => return Ok(true),
				WriteState::Deleted => return Ok(false),
				WriteState::Unchanged => match &x.read {
					ReadState::Read(_) | ReadState::Exists => return Ok(true),
					ReadState::Empty => return Ok(false),
					ReadState::Unknown => {}
				},
			}
		}

		let res = self.tx.has(key).await.map_err(kvs_error)?;
		self.record_read(
			key,
			if res {
				ReadState::Exists
			} else {
				ReadState::Empty
			},
		);
		Ok(res)
	}

	/// Fetch up to `skip + limit` entries of the range in scan direction,
	/// merging database rows with this transaction's pending writes and read
	/// cache. Every database row consumed is recorded in the read-set so the
	/// scan participates in commit-time conflict validation.
	///
	/// For keys-only scans the returned values are `None` for database rows;
	/// they may still be `Some` for locally cached entries.
	async fn scan_merge(
		&mut self,
		mut rng: KeyRange<'_>,
		mut limit: u32,
		mut skip: u32,
		reverse: bool,
		keys_only: bool,
	) -> KvsResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
		let mut out: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

		if limit == 0 || rng.is_empty() {
			return Ok(out);
		}

		// Snapshot how every locally-known key in the range affects the scan.
		let mut overlay: Vec<(Vec<u8>, Overlay)> = {
			// Need to use the bounds tuple as the tuple is the only type that has `RangeBound<T>`
			// implemented for T: ?Sized
			let range =
				(Bound::Included(rng.start.as_slice()), Bound::Excluded(rng.end.as_slice()));
			let iter = self.keys.range::<[u8], _>(range).map(|(k, st)| {
				let vis = match &st.write {
					WriteState::Written(v) => {
						if keys_only {
							Overlay::Value(None)
						} else {
							Overlay::Value(Some(v.clone()))
						}
					}
					WriteState::Deleted => Overlay::Hidden,
					WriteState::Unchanged => match &st.read {
						ReadState::Read(v) => {
							if keys_only {
								Overlay::Value(None)
							} else {
								Overlay::Value(Some(v.clone()))
							}
						}
						ReadState::Empty => Overlay::Hidden,
						ReadState::Exists | ReadState::Unknown => Overlay::Passthrough,
					},
				};
				(k.clone(), vis)
			});

			// invert the iterations so that we can pop values back to front.
			if reverse {
				iter.collect()
			} else {
				iter.rev().collect()
			}
		};

		// fetch loop, running possibly over multiple batches.
		'fetch: loop {
			let batch = self
				.tx
				.scan(&rng.start, &rng.end, reverse, limit.saturating_add(skip), keys_only)
				.await
				.map_err(kvs_error)?;

			if batch.keys.is_empty() {
				break;
			}

			let len = batch
				.values
				.as_ref()
				.map(|x| x.len().min(batch.keys.len()))
				.unwrap_or(batch.keys.len());

			let iter = batch
				.values
				.map(|v| v.into_iter())
				.unwrap_or(Vec::new().into_iter())
				.map(Some)
				.chain(std::iter::repeat(None));

			// Value keeping track of the last skipped key
			// Only set if the last key is not in the output.
			let mut last_key = None;

			'cursor: for (idx, (key, val)) in batch.keys.into_iter().zip(iter).enumerate() {
				while skip > 0 {
					if let Some((overlay_key, o)) = overlay.last() {
						if overlay_key == &key {
							// Value only exists in the overlay, we still need to count it as a
							// skipped value.
							skip -= !matches!(o, Overlay::Hidden) as u32;
							overlay.pop();
							last_key = Some(key.clone());
							// We consumed the key, move onto the next one.
							continue 'cursor;
						}

						// Is the key the current or a previous key in the overlay.
						if reverse && overlay_key > &key || !reverse && overlay_key < &key {
							skip -= matches!(o, Overlay::Value(_)) as u32;
							overlay.pop();
							continue;
						}
					}
					// new key not in the overlay,

					skip -= 1;

					// We need some key to advance the scan if we skipped over all of them
					// By checking if we are at the last key we avoid cloning every key.
					if idx == len - 1 {
						last_key = Some(key.clone());
					}

					// We only observe skipped keys as existing.
					self.keys.insert(
						key,
						KeyState {
							read: ReadState::Exists,
							write: WriteState::Unchanged,
						},
					);

					// Skipped the scan returned key so continue to the next.
					continue 'cursor;
				}

				while let Some((overlay_key, _)) = overlay.last() {
					if reverse && overlay_key > &key || !reverse && overlay_key < &key {
						// Handle any overlay key which we skipped over in between the previous scan
						// returned key and the current.
						let (k, o) = overlay.pop().expect("There should be an entry");
						match o {
							Overlay::Value(v) => {
								out.push((k, v));
								limit -= 1;
								if limit == 0 {
									break 'fetch;
								}
							}
							// Nothing to do in this case.
							Overlay::Hidden | Overlay::Passthrough => {}
						}
					} else if overlay_key == &key {
						let (k, o) = overlay.pop().expect("There should be an entry");
						match o {
							Overlay::Value(v) => {
								last_key = None;
								out.push((k, v));
								limit -= 1;
								if limit == 0 {
									break 'fetch;
								}
							}
							Overlay::Hidden => {
								last_key = Some(k);
							}
							Overlay::Passthrough => {
								// update the witness entry as we have now read the key.
								let witness = self
									.keys
									.get_mut(&key)
									.expect("key is in overlay so it must be in keys");
								if let Some(v) = val.as_ref() {
									witness.read = ReadState::Read(v.clone());
								} else {
									witness.read = ReadState::Exists
								}

								last_key = None;
								out.push((key, val));
								limit -= 1;
								if limit == 0 {
									break 'fetch;
								}
							}
						}
						// Handled the scan returned key so continue to the next.
						continue 'cursor;
					} else {
						break;
					}
				}

				// At this point the key has not been handled yet and there is no overlay entry for
				// it.

				if let Some(v) = val.as_ref() {
					self.keys.insert(
						key.clone(),
						KeyState {
							read: ReadState::Read(v.clone()),
							write: WriteState::Unchanged,
						},
					);
				} else {
					self.keys.insert(
						key.clone(),
						KeyState {
							read: ReadState::Exists,
							write: WriteState::Unchanged,
						},
					);
				}

				last_key = None;
				out.push((key, val));

				limit -= 1;
				if limit == 0 {
					break 'fetch;
				}
			}

			// since there was a value in the batch we must be able to advance the range.
			let last_key = last_key
				.as_ref()
				.or_else(|| out.last().map(|x| &x.0))
				.expect("a single key to be present in the batch");

			if reverse {
				rng.end.clone_from_slice(last_key.as_slice());
			} else {
				rng.start.clone_from_slice(last_key.as_slice());
				rng.start.advance();
			}
		}

		// at this point the iterator over the kv store has ran out, however we might still have
		// skip, limit and value's in the overlay left.
		while skip > 0
			&& let Some((_, o)) = overlay.pop()
		{
			match o {
				Overlay::Value(_) => {
					skip -= 1;
				}
				Overlay::Hidden | Overlay::Passthrough => {}
			}
		}

		while limit > 0
			&& let Some((k, o)) = overlay.pop()
		{
			match o {
				Overlay::Value(v) => {
					out.push((k, v));
					limit -= 1;
				}
				Overlay::Hidden | Overlay::Passthrough => {}
			}
		}

		Ok(out)
	}
}

pub struct IndxdbTx {
	mode: TransactionType,
	done: AtomicBool,
	state: Mutex<Option<State>>,
}

impl IndxdbTx {
	pub(crate) fn new(mode: TransactionType, tx: ffi::Tx) -> Self {
		IndxdbTx {
			mode,
			done: AtomicBool::new(false),
			state: Mutex::new(Some(State {
				tx,
				keys: BTreeMap::new(),
				savepoints: Vec::new(),
			})),
		}
	}

	async fn cancel_op(&self) -> KvsResult<()> {
		let mut lock = self.state.lock().await;
		if lock.take().is_none() {
			return Err(KvsError::TransactionFinished);
		}
		self.done.store(true, Ordering::Release);
		Ok(())
	}

	async fn commit_op(&self) -> KvsResult<()> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.take() else {
			return Err(KvsError::TransactionFinished);
		};
		self.done.store(true, Ordering::Release);

		if self.mode != TransactionType::Write {
			return Err(KvsError::TransactionReadonly);
		}

		let State {
			tx,
			keys: key_map,
			..
		} = inner;

		// Pack the read-set and write-set into flat buffers: per entry a key
		// slice, a flag byte, and — depending on the flags — a read-value
		// and/or written-value slice.
		let mut keys = Vec::new();
		let mut key_slices: Vec<u32> = Vec::new();
		let mut read_buf = Vec::new();
		let mut read_slices: Vec<u32> = Vec::new();
		let mut written_buf = Vec::new();
		let mut written_slices: Vec<u32> = Vec::new();
		let mut flags: Vec<u8> = Vec::new();

		for (key, st) in key_map {
			let KeyState {
				read,
				write,
			} = st;

			let read_flag = match read {
				ReadState::Unknown => READ_UNKNOWN,
				ReadState::Exists => READ_EXISTS,
				ReadState::Read(_) => READ_READ,
				ReadState::Empty => READ_EMPTY,
			};
			let write_flag = match write {
				WriteState::Unchanged => WRITE_UNCHANGED,
				WriteState::Written(_) => WRITE_WRITTEN,
				WriteState::Deleted => WRITE_DELETED,
			};

			// Nothing observed and nothing written: no reason to ship it.
			if read_flag == READ_UNKNOWN && write_flag == WRITE_UNCHANGED {
				continue;
			}

			key_slices.push(keys.len() as u32);
			keys.extend_from_slice(&key);
			key_slices.push(keys.len() as u32);

			if let ReadState::Read(v) = read {
				read_slices.push(read_buf.len() as u32);
				read_buf.extend_from_slice(&v);
				read_slices.push(read_buf.len() as u32);
			}
			if let WriteState::Written(v) = write {
				written_slices.push(written_buf.len() as u32);
				written_buf.extend_from_slice(&v);
				written_slices.push(written_buf.len() as u32);
			}

			flags.push(write_flag << 4 | read_flag);
		}

		if flags.is_empty() {
			return Ok(());
		}

		let applied = tx
			.commit(
				&keys,
				&key_slices,
				&read_buf,
				&read_slices,
				&written_buf,
				&written_slices,
				&flags,
			)
			.await
			.map_err(kvs_error)?;

		if applied {
			Ok(())
		} else {
			Err(KvsError::TransactionConflict(
				"a key accessed by the transaction was concurrently modified".to_string(),
			))
		}
	}

	async fn exists_op(&self, key: Key<'_>) -> KvsResult<bool> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.as_mut() else {
			return Err(KvsError::TransactionFinished);
		};
		inner.visible_exists(&key).await
	}

	async fn get_op(&self, key: Key<'_>) -> KvsResult<Option<Val>> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.as_mut() else {
			return Err(KvsError::TransactionFinished);
		};
		inner.visible_get(&key).await
	}

	async fn set_op(&self, key: Key<'_>, val: Val) -> KvsResult<()> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.as_mut() else {
			return Err(KvsError::TransactionFinished);
		};
		if self.mode != TransactionType::Write {
			return Err(KvsError::TransactionReadonly);
		}
		inner.set_write(key.into_vec(), WriteState::Written(val));
		Ok(())
	}

	async fn put_op(&self, key: Key<'_>, val: Val) -> KvsResult<()> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.as_mut() else {
			return Err(KvsError::TransactionFinished);
		};
		if self.mode != TransactionType::Write {
			return Err(KvsError::TransactionReadonly);
		}
		if inner.visible_exists(&key).await? {
			return Err(KvsError::TransactionKeyAlreadyExists);
		}
		inner.set_write(key.into_vec(), WriteState::Written(val));
		Ok(())
	}

	async fn putc_op(&self, key: Key<'_>, val: Val, chk: Option<Val>) -> KvsResult<()> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.as_mut() else {
			return Err(KvsError::TransactionFinished);
		};
		if self.mode != TransactionType::Write {
			return Err(KvsError::TransactionReadonly);
		}
		match (inner.visible_get(&key).await?, chk) {
			(Some(v), Some(w)) if v == w => {}
			(None, None) => {}
			_ => return Err(KvsError::TransactionConditionNotMet),
		}
		inner.set_write(key.into_vec(), WriteState::Written(val));
		Ok(())
	}

	async fn del_op(&self, key: Key<'_>) -> KvsResult<()> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.as_mut() else {
			return Err(KvsError::TransactionFinished);
		};
		if self.mode != TransactionType::Write {
			return Err(KvsError::TransactionReadonly);
		}
		inner.set_write(key.into_vec(), WriteState::Deleted);
		Ok(())
	}

	async fn delc_op(&self, key: Key<'_>, chk: Option<&[u8]>) -> KvsResult<()> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.as_mut() else {
			return Err(KvsError::TransactionFinished);
		};
		if self.mode != TransactionType::Write {
			return Err(KvsError::TransactionReadonly);
		}
		match (inner.visible_get(&key).await?, chk) {
			(Some(v), Some(w)) if v == w => {}
			(None, None) => {}
			_ => return Err(KvsError::TransactionConditionNotMet),
		}
		inner.set_write(key.into_vec(), WriteState::Deleted);
		Ok(())
	}

	async fn keys_op(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		reverse: bool,
	) -> KvsResult<KeysResult> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.as_mut() else {
			return Err(KvsError::TransactionFinished);
		};
		let out = inner.scan_merge(rng, limit, skip, reverse, true).await?;

		let mut keys = Vec::with_capacity(out.len());
		let mut key_bytes = 0u64;
		for (key, _) in out {
			key_bytes += key.len() as u64;
			keys.push(key);
		}
		Ok(KeysResult {
			keys,
			key_bytes,
		})
	}

	async fn scan_op(
		&self,
		rng: KeyRange<'_>,
		limit: u32,
		skip: u32,
		reverse: bool,
	) -> KvsResult<ScanResult> {
		let mut lock = self.state.lock().await;
		let Some(inner) = lock.as_mut() else {
			return Err(KvsError::TransactionFinished);
		};
		let out = inner.scan_merge(rng, limit, skip, reverse, false).await?;

		let mut values = Vec::with_capacity(out.len());
		let mut key_bytes = 0u64;
		let mut value_bytes = 0u64;
		for (key, val) in out {
			let val = val.ok_or_else(|| KvsError::internal("scan entry without a value"))?;
			key_bytes += key.len() as u64;
			value_bytes += val.len() as u64;
			values.push((key, val));
		}
		Ok(ScanResult {
			values,
			key_bytes,
			value_bytes,
		})
	}
}

/// Rejects versioned queries: IndexedDB has no MVCC support.
fn ensure_unversioned(version: Option<u64>) -> KvsResult<()> {
	if version.is_some() {
		return Err(KvsError::UnsupportedVersionedQueries);
	}
	Ok(())
}

impl Transactable for IndxdbTx {
	fn kind(&self) -> &'static str {
		"indxdb"
	}

	fn closed(&self) -> bool {
		self.done.load(Ordering::Acquire)
	}

	fn writeable(&self) -> bool {
		self.mode == TransactionType::Write
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn cancel<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(self.cancel_op())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn commit<'a>(&'a self) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(self.commit_op())
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, KvsResult<bool>> {
		Box::pin(async move {
			ensure_unversioned(version)?;
			self.exists_op(key).await
		})
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, KvsResult<Option<Val>>> {
		Box::pin(async move {
			ensure_unversioned(version)?;
			self.get_op(key).await
		})
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(self.set_op(key, val))
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(self.put_op(key, val))
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(self.putc_op(key, val, chk))
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(self.del_op(key))
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(key = key.to_string()))]
	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, KvsResult<()>> {
		Box::pin(self.delc_op(key, chk))
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<KeysResult>> {
		Box::pin(async move {
			ensure_unversioned(version)?;
			self.keys_op(rng, limit, skip, false).await
		})
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn keysr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<KeysResult>> {
		Box::pin(async move {
			ensure_unversioned(version)?;
			self.keys_op(rng, limit, skip, true).await
		})
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn scan<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<ScanResult>> {
		Box::pin(async move {
			ensure_unversioned(version)?;
			self.scan_op(rng, limit, skip, false).await
		})
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self), fields(rng = rng.to_string()))]
	fn scanr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, KvsResult<ScanResult>> {
		Box::pin(async move {
			ensure_unversioned(version)?;
			self.scan_op(rng, limit, skip, true).await
		})
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn new_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
		Box::pin(async {
			let mut lock = self.state.lock().await;
			let Some(inner) = lock.as_mut() else {
				return Err(KvsError::TransactionFinished);
			};
			// No need to do anything for read only transactions as there is no modification to
			// rollback.
			if self.mode == TransactionType::Write {
				inner.savepoints.push(BTreeMap::new());
			}
			Ok(())
		})
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn release_last_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
		Box::pin(async {
			let mut lock = self.state.lock().await;
			let Some(inner) = lock.as_mut() else {
				return Err(KvsError::TransactionFinished);
			};
			// Releasing keeps the savepoint's writes, but a rollback to an
			// outer savepoint must still be able to undo them: merge the
			// undo records into the parent instead of discarding them. The
			// parent keeps its own record when both observed the same key,
			// as it holds the older state.
			if let Some(sp) = inner.savepoints.pop()
				&& let Some(parent) = inner.savepoints.last_mut()
			{
				for (key, write) in sp {
					parent.entry(key).or_insert(write);
				}
			}
			Ok(())
		})
	}

	#[instrument(level = "trace", target = "surrealdb::core::kvs::api", skip(self))]
	fn rollback_to_save_point(&self) -> BoxFut<'_, KvsResult<()>> {
		Box::pin(async {
			let mut lock = self.state.lock().await;
			let Some(inner) = lock.as_mut() else {
				return Err(KvsError::TransactionFinished);
			};
			inner.rollback_to_savepoint()
		})
	}
}

const _: () = {
	const fn is_send_sync<T: Send + Sync>() {}
	is_send_sync::<IndxdbTx>();
};
