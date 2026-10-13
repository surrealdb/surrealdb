//! Typed, `Send + Sync` wrappers around the JavaScript IndexedDB driver in
//! `src/js/db.js`.
//!
//! All communication with JS happens through packed `Uint8Array` /
//! `Uint32Array` buffers to keep the number of wasm<->js crossings and
//! marshalled objects to a minimum.

use std::future::IntoFuture;

use js_sys::{Uint8Array, Uint32Array};
use wasm_bindgen::prelude::*;

mod bindings {
	use js_sys::{JsString, Promise, Uint8Array, Uint32Array};
	use wasm_bindgen::prelude::*;

	#[wasm_bindgen(module = "/src/js/db.js")]
	extern "C" {
		pub fn to_string(name: JsValue) -> JsString;

		pub fn open(name: &str) -> Promise<Db>;

		pub type Db;

		#[wasm_bindgen(method, catch)]
		pub fn begin(this: &Db) -> Result<Tx, JsValue>;

		#[wasm_bindgen(method, catch)]
		pub fn close(this: &Db) -> Result<(), JsValue>;

		pub type Tx;

		#[wasm_bindgen(method)]
		pub fn read(this: &Tx, key: &[u8]) -> Promise<Option<Uint8Array>>;

		#[wasm_bindgen(method)]
		pub fn has(this: &Tx, key: &[u8]) -> Promise<bool>;

		#[wasm_bindgen(method)]
		pub fn scan(
			this: &Tx,
			start: &[u8],
			end: &[u8],
			reverse: bool,
			limit: u32,
			keys_only: bool,
		) -> Promise<ScanBatch>;

		#[wasm_bindgen(method)]
		pub fn commit(
			this: &Tx,
			keys: &[u8],
			key_slices: &[u32],
			read: &[u8],
			read_slices: &[u32],
			written: &[u8],
			written_slices: &[u32],
			flags: &[u8],
		) -> Promise<bool>;
	}

	// The object returned by `Tx::scan`: entry bytes concatenated into single
	// buffers with an end-offset per entry.
	#[wasm_bindgen]
	extern "C" {
		pub type ScanBatch;

		#[wasm_bindgen(method, getter)]
		pub fn keys(this: &ScanBatch) -> Uint8Array;

		#[wasm_bindgen(method, getter, js_name = keyEnds)]
		pub fn key_ends(this: &ScanBatch) -> Uint32Array;

		#[wasm_bindgen(method, getter)]
		pub fn values(this: &ScanBatch) -> Option<Uint8Array>;

		#[wasm_bindgen(method, getter, js_name = valueEnds)]
		pub fn value_ends(this: &ScanBatch) -> Option<Uint32Array>;
	}
}

/// Splits a packed buffer of concatenated entries into owned per-entry
/// vectors. `ends[i]` is the exclusive end offset of entry `i`; entry `i`
/// starts where entry `i - 1` ended.
fn split_packed(data: &Uint8Array, ends: &Uint32Array) -> Vec<Vec<u8>> {
	let data = data.to_vec();
	let ends = ends.to_vec();
	let mut out = Vec::with_capacity(ends.len());
	let mut start = 0usize;
	for end in ends {
		let end = end as usize;
		out.push(data[start..end].to_vec());
		start = end;
	}
	out
}

/// The unpacked result of a [`Tx::scan`] call.
pub struct ScanBatch {
	/// The fetched keys, in scan order.
	pub keys: Vec<Vec<u8>>,
	/// The fetched values, index-matched with `keys`. `None` for keys-only
	/// scans.
	pub values: Option<Vec<Vec<u8>>>,
}

/// An open IndexedDB database.
pub struct Db {
	inner: bindings::Db,
}

impl Db {
	/// Open (creating if necessary) the IndexedDB database with the given
	/// name.
	pub async fn open(name: &str) -> Result<Db, JsValue> {
		let val = common::future::assert_send(bindings::open(name).into_future()).await?;
		Ok(Db {
			inner: val.unchecked_into(),
		})
	}

	/// Begin a new transaction handle.
	pub fn begin(&self) -> Result<Tx, JsValue> {
		Ok(Tx {
			inner: self.inner.begin()?,
		})
	}

	/// Close the database handle.
	pub fn close(&self) -> Result<(), JsValue> {
		self.inner.close()
	}
}

/// A logical transaction handle.
///
/// This does not correspond to a live IndexedDB transaction: IndexedDB
/// transactions go inactive whenever an await crosses a non-IndexedDB
/// microtask, so each read opens a short-lived transaction and consistency is
/// enforced by validating the read-set inside [`Tx::commit`].
pub struct Tx {
	inner: bindings::Tx,
}

impl Tx {
	/// Read a single key.
	pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, JsValue> {
		if let Some(val) = common::future::assert_send(self.inner.read(key).into_future()).await? {
			Ok(Some(val.to_vec()))
		} else {
			Ok(None)
		}
	}

	/// Check whether a key exists without fetching its value.
	pub async fn has(&self, key: &[u8]) -> Result<bool, JsValue> {
		common::future::assert_send(self.inner.has(key).into_future()).await
	}

	/// Fetch up to `limit` entries in `start..end` (start inclusive, end
	/// exclusive), in reverse key order when `reverse` is set. When
	/// `keys_only` is set values are neither read nor returned.
	pub async fn scan(
		&self,
		start: &[u8],
		end: &[u8],
		reverse: bool,
		limit: u32,
		keys_only: bool,
	) -> Result<ScanBatch, JsValue> {
		let val = common::future::assert_send(
			self.inner.scan(start, end, reverse, limit, keys_only).into_future(),
		)
		.await?;
		let batch: bindings::ScanBatch = val.unchecked_into();
		let keys = split_packed(&batch.keys(), &batch.key_ends());
		let values = match (batch.values(), batch.value_ends()) {
			(Some(data), Some(ends)) => Some(split_packed(&data, &ends)),
			_ => None,
		};
		Ok(ScanBatch {
			keys,
			values,
		})
	}

	/// Atomically validate the read-set and apply the write-set.
	///
	/// Entry `i` covers `keys[key_slices[2 * i]..key_slices[2 * i + 1]]`, with
	/// its read/write state packed into `flags[i]` (write state in the high
	/// nibble, read state in the low nibble). Entries flagged as read or
	/// written additionally consume the next slice of `read` / `written`
	/// respectively.
	///
	/// Returns `true` when the transaction was applied and `false` on a
	/// read-set conflict.
	#[expect(clippy::too_many_arguments)]
	pub async fn commit(
		&self,
		keys: &[u8],
		key_slices: &[u32],
		read: &[u8],
		read_slices: &[u32],
		written: &[u8],
		written_slices: &[u32],
		flags: &[u8],
	) -> Result<bool, JsValue> {
		common::future::assert_send(
			self.inner
				.commit(keys, key_slices, read, read_slices, written, written_slices, flags)
				.into_future(),
		)
		.await
	}
}

pub fn to_string(v: JsValue) -> Option<String> {
	bindings::to_string(v).as_string()
}

const _: () = {
	const fn is_send_sync<T: Send + Sync>() {}
	is_send_sync::<Db>();
	is_send_sync::<Tx>();
};
