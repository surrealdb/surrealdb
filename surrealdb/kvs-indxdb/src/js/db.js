// IndexedDB driver for the surrealdb-kvs-indxdb backend.
//
// IndexedDB transactions go inactive whenever an await crosses a
// non-IndexedDB microtask, so a single IndexedDB transaction cannot span a
// Rust-side kvs transaction. Instead every read runs in its own short-lived
// readonly transaction, and `Tx.commit` validates the accumulated read-set
// and applies the write-set atomically inside one readwrite transaction that
// only ever awaits IndexedDB requests (which keeps it active).
//
// The database layout matches the layout previously created by the `indxdb`
// crate: a single object store named "kv" (database version 1) with
// out-of-line binary keys and Uint8Array values, so databases created by
// older versions open without migration. Future layout changes must bump the
// database version and migrate in `onupgradeneeded`.

// Read-state flags, low nibble of a commit entry flag byte.
const READ_UNKNOWN = 0;
const READ_EXISTS = 1;
const READ_READ = 2;
const READ_EMPTY = 3;

// Write-state flags, high nibble of a commit entry flag byte.
const WRITE_UNCHANGED = 0;
const WRITE_WRITTEN = 1;
const WRITE_DELETED = 2;

function promiseReq(req) {
	return new Promise((resolve, reject) => {
		req.onsuccess = () => resolve(req.result);
		req.onerror = () => reject(req.error);
	});
}

// IndexedDB hands binary keys back as ArrayBuffer and values in whatever
// shape they were stored in; normalize both to Uint8Array.
function toBytes(value) {
	if (value instanceof Uint8Array) {
		return value;
	}
	return new Uint8Array(value);
}

function bytesEqual(a, b) {
	if (a.length !== b.length) {
		return false;
	}
	for (let i = 0; i < a.length; i += 1) {
		if (a[i] !== b[i]) {
			return false;
		}
	}
	return true;
}

const EMPTY_BATCH = {
	keys: new Uint8Array(0),
	keyEnds: new Uint32Array(0),
	values: null,
	valueEnds: null,
};

export class Tx {
	constructor(db) {
		this.db = db;
	}

	store(mode) {
		return this.db.transaction("kv", mode).objectStore("kv");
	}

	async read(key) {
		const value = await promiseReq(this.store("readonly").get(key));
		return value === undefined ? null : toBytes(value);
	}

	async has(key) {
		const count = await promiseReq(this.store("readonly").count(key));
		return count > 0;
	}

	scan(start, end, reverse, limit, keysOnly) {
		if (limit === 0) {
			// ensure we always return a promise.
			return new Promise((res) => res(EMPTY_BATCH));
		}

		let range;
		try {
			// Start inclusive, end exclusive.
			range = IDBKeyRange.bound(start, end, false, true);
		} catch {
			// Thrown when start >= end: an empty range.
			// ensure we always return a promise.
			return new Promise((res) => res(EMPTY_BATCH));
		}

		const keys = [];
		const values = keysOnly ? null : [];

		const store = this.store("readonly");
		const direction = reverse ? "prev" : "next";
		const req = keysOnly
			? store.getAllKeys({ query: range, count: limit, direction })
			: store.getAllRecords({ query: range, count: limit, direction })

		return new Promise((resolve, reject) => {
			req.onsuccess = () => {

				const results = req.result;
				const resLength = results.length;

				let valuesLen = 0;
				let keysLen = 0;

				if (keysOnly){
					for(let i = 0;i < resLength;i++){
						keysLen += results[i].byteLength;
					}
				}else{
					for(let i = 0;i < resLength;i++){
						keysLen += results[i].key.byteLength;
						valuesLen += results[i].value.byteLength;
					}
				}

				const keyEnds = new Uint32Array(resLength);
				const valueEnds = new Uint32Array(resLength);

				const keys = new Uint8Array(keysLen);
				const values = new Uint8Array(valuesLen);


				if (keysOnly) {
					let offset = 0;
					for(let i = 0;i < resLength;i++){
						const bytes = toBytes(results[i]);
						keys.set(bytes,offset);
						offset += bytes.length;
						keyEnds[i] = offset;
					}
				}else{
					let koffset = 0;
					let voffset = 0;
					for(let i = 0;i < resLength;i++){

						const kbytes = toBytes(results[i].key);
						keys.set(kbytes,koffset);
						koffset += kbytes.length;
						keyEnds[i] = koffset;

						const vbytes = toBytes(results[i].value);
						values.set(vbytes,voffset);
						voffset += vbytes.length;
						valueEnds[i] = voffset;
					}
				}

				return resolve({
					keys,
					keyEnds,
					values,
					valueEnds,
				});


			};
			req.onerror = () => reject(req.error);
		});

	}

	async commit(keys, keySlices, read, readSlices, written, writtenSlices, flags) {
		// The typed-array arguments may be views into wasm memory. While this
		// function awaits, other wasm tasks can run and grow the wasm memory,
		// detaching those views. So we need to copy read before the first await as it
		// might be used after an await.
		read = read.slice();

		const tx = this.db.transaction("kv", "readwrite");
		const store = tx.objectStore("kv");

		let readIdx = 0;
		let writeIdx = 0;

		// To improve speed we issue all read checks in parallel and do not wait for
		// the request to return instead checking once at the end if all the checks
		// succeeded.
		let conflict = false;
		let pendingChecks = 0;

		const fail = () => {
			if (!conflict) {
				conflict = true;
				tx.abort();
			}
		};

		// Explicitly committing skips one idle event-loop turn, but is only
		// legal once no validation can still abort: after commit() the
		// transaction enters the committing state where abort() throws.
		const checkDone = () => {
			pendingChecks -= 1;
			if (pendingChecks === 0 && !conflict && tx.commit) {
				tx.commit();
			}
		};

		// All requests are fired without awaiting; IndexedDB executes them in
		// issue order within the transaction, so every validation read of a
		// key observes the pre-transaction state (each key appears at most
		// once). This pipelines the whole commit into a single pass instead
		// of one event-loop round trip per validated key.
		for (let i = 0; i < flags.length; i += 1) {
			const readFlag = flags[i] & 0xf;
			const writeFlag = flags[i] >> 4;

			const key = keys.subarray(keySlices[i * 2], keySlices[i * 2 + 1]);

			switch (readFlag) {
				case READ_EMPTY: {
					pendingChecks += 1;
					const req = store.count(key);
					req.onsuccess = () => {
						if (req.result !== 0) {
							fail();
						} else {
							checkDone();
						}
					};
					break;
				}
				case READ_EXISTS: {
					pendingChecks += 1;
					const req = store.count(key);
					req.onsuccess = () => {
						if (req.result === 0) {
							fail();
						} else {
							checkDone();
						}
					};
					break;
				}
				case READ_READ: {
					const idx = readIdx;
					readIdx += 1;
					const expect = read.subarray(readSlices[idx * 2], readSlices[idx * 2 + 1]);
					pendingChecks += 1;
					const req = store.get(key);
					req.onsuccess = () => {
						const value = req.result;
						if (value === undefined || !bytesEqual(toBytes(value), expect)) {
							fail();
						} else {
							checkDone();
						}
					};
					break;
				}
				default:
					break;
			}

			switch (writeFlag) {
				case WRITE_DELETED: {
					store.delete(key);
					break;
				}
				case WRITE_WRITTEN: {
					const idx = writeIdx;
					writeIdx += 1;
					// slice(), not subarray(): the structured clone of a view
					// serializes its entire backing buffer, so storing a
					// subarray would persist a copy of the whole packed
					// commit payload with every entry.
					const value = written.slice(writtenSlices[idx * 2], writtenSlices[idx * 2 + 1]);
					store.put(value, key);
					break;
				}
				default:
					break;
			}
		}

		const applied = await new Promise((resolve, reject) => {
			tx.oncomplete = () => resolve(0);
			tx.onabort = () =>
				conflict ? resolve(1) : reject(tx.error ?? new Error("transaction aborted"));
			// Swallow per-request error events (an abort after a failed
			// validation errors every later queued request) without
			// cancelling their default action, so a genuine request failure
			// still aborts the transaction and surfaces via onabort.
			tx.onerror = () => {};
			if (pendingChecks === 0 && tx.commit) {
				tx.commit();
			}
		});
		return applied;
	}
}

export class Db {
	constructor(db) {
		this.db = db;
	}

	begin() {
		return new Tx(this.db);
	}

	close() {
		this.db.close();
	}
}

export function open(name) {
	return new Promise((resolve, reject) => {
		if (typeof indexedDB === "undefined") {
			reject(new Error("IndexedDB api is not supported on this platform"));
			return;
		}

		const req = indexedDB.open(name, 1);

		req.onerror = () => reject(req.error);

		req.onupgradeneeded = () => {
			// Fresh database: create the store in the indxdb-crate-compatible
			// layout described at the top of this file.
			req.result.createObjectStore("kv");
		};

		req.onsuccess = () => {
			const db = req.result;
			if (!db.objectStoreNames.contains("kv")) {
				db.close();
				reject(new Error(`indexedDB database ${name} is missing the "kv" object store`));
			} else {
				resolve(new Db(db));
			}
		};
	});
}

export function to_string(v){
	return v.toString()
}
