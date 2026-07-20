//! Typed, `Send + Sync` wrappers around the JavaScript IndexedDB driver in
//! `src/js/db.js`.
//!
//! All communication with JS happens through packed `Uint8Array` /
//! `Uint32Array` buffers to keep the number of wasm<->js crossings and
//! marshalled objects to a minimum.

use js_sys::{Uint8Array, Uint32Array};
use wasm_bindgen::prelude::*;

use crate::send_future::SendFuture;

/// Marks a JS handle as `Send + Sync`.
///
/// SAFETY: wasm32-unknown-unknown without the `atomics` target feature is
/// single-threaded; every wrapped handle is created and used on the one and
/// only thread, so no data race can occur.
struct SendJs<T>(T);

unsafe impl<T> Send for SendJs<T> {}
unsafe impl<T> Sync for SendJs<T> {}

mod bindings {
	use js_sys::{JsString, Promise, Uint8Array, Uint32Array};
	use wasm_bindgen::prelude::*;

	#[wasm_bindgen(module = "/src/js/db.js")]
	extern "C" {
		pub fn to_string(name: JsValue) -> JsString;

		pub fn open(name: &str) -> Promise;

		pub type Db;

		#[wasm_bindgen(method, catch)]
		pub fn begin(this: &Db) -> Result<Tx, JsValue>;

		#[wasm_bindgen(method, catch)]
		pub fn close(this: &Db) -> Result<(), JsValue>;

		pub type Tx;

		#[wasm_bindgen(method)]
		pub fn read(this: &Tx, key: &[u8]) -> Promise;

		#[wasm_bindgen(method)]
		pub fn has(this: &Tx, key: &[u8]) -> Promise;

		#[wasm_bindgen(method)]
		pub fn scan(
			this: &Tx,
			start: &[u8],
			end: &[u8],
			reverse: bool,
			limit: u32,
			keys_only: bool,
		) -> Promise;

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
		) -> Promise;
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
	inner: SendJs<bindings::Db>,
}

impl Db {
	/// Open (creating if necessary) the IndexedDB database with the given
	/// name.
	pub async fn open(name: &str) -> Result<Db, JsValue> {
		let val = SendFuture::new(bindings::open(name)).await?;
		Ok(Db {
			inner: SendJs(val.unchecked_into()),
		})
	}

	/// Begin a new transaction handle.
	pub fn begin(&self) -> Result<Tx, JsValue> {
		Ok(Tx {
			inner: SendJs(self.inner.0.begin()?),
		})
	}

	/// Close the database handle.
	pub fn close(&self) -> Result<(), JsValue> {
		self.inner.0.close()
	}
}

/// A logical transaction handle.
///
/// This does not correspond to a live IndexedDB transaction: IndexedDB
/// transactions go inactive whenever an await crosses a non-IndexedDB
/// microtask, so each read opens a short-lived transaction and consistency is
/// enforced by validating the read-set inside [`Tx::commit`].
pub struct Tx {
	inner: SendJs<bindings::Tx>,
}

impl Tx {
	/// Read a single key.
	pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, JsValue> {
		let val = SendFuture::new(self.inner.0.read(key)).await?;
		if val.is_null() || val.is_undefined() {
			Ok(None)
		} else {
			Ok(Some(val.unchecked_into::<Uint8Array>().to_vec()))
		}
	}

	/// Check whether a key exists without fetching its value.
	pub async fn has(&self, key: &[u8]) -> Result<bool, JsValue> {
		let val = SendFuture::new(self.inner.0.has(key)).await?;
		// A non-boolean can only mean a driver bug: surface it instead of
		// silently treating the key as absent.
		val.as_bool()
			.ok_or_else(|| JsValue::from_str("indexeddb driver returned a non-boolean from has()"))
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
		let val = SendFuture::new(self.inner.0.scan(start, end, reverse, limit, keys_only)).await?;
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
		let val = SendFuture::new(self.inner.0.commit(
			keys,
			key_slices,
			read,
			read_slices,
			written,
			written_slices,
			flags,
		))
		.await?;
		// Anything but the two defined return codes is a driver bug: surface
		// it instead of misreporting it as a (retryable) conflict.
		let code = val.as_f64();
		if code == Some(0.0) {
			Ok(true)
		} else if code == Some(1.0) {
			Ok(false)
		} else {
			Err(JsValue::from_str(
				"indexeddb driver returned an unexpected result code from commit()",
			))
		}
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
