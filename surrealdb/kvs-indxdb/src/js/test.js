// Helpers for the wasm-bindgen-test suite.

export function clear(name) {
	return new Promise((resolve, reject) => {
		const req = indexedDB.deleteDatabase(name);
		req.onsuccess = () => resolve();
		req.onblocked = () =>
			reject(new Error(`clearing database ${name} is blocked by an open connection`));
		req.onerror = () => reject(req.error);
	});
}

// Create a database in exactly the layout the old `indxdb`-crate backend
// produced: version 1, an object store "kv" without a keyPath, and values
// put with explicit out-of-line binary keys. Entries are passed as packed
// buffers with an exclusive end-offset per entry.
export function seedOldFormat(name, keys, keyEnds, values, valueEnds) {
	keys = keys.slice();
	keyEnds = keyEnds.slice();
	values = values.slice();
	valueEnds = valueEnds.slice();
	return new Promise((resolve, reject) => {
		const req = indexedDB.open(name, 1);
		req.onerror = () => reject(req.error);
		req.onupgradeneeded = () => {
			req.result.createObjectStore("kv");
		};
		req.onsuccess = () => {
			const db = req.result;
			const tx = db.transaction("kv", "readwrite");
			const store = tx.objectStore("kv");
			let keyStart = 0;
			let valueStart = 0;
			for (let i = 0; i < keyEnds.length; i += 1) {
				const key = keys.subarray(keyStart, keyEnds[i]);
				const value = values.subarray(valueStart, valueEnds[i]);
				keyStart = keyEnds[i];
				valueStart = valueEnds[i];
				store.put(value, key);
			}
			tx.oncomplete = () => {
				db.close();
				resolve();
			};
			tx.onerror = () => reject(tx.error);
		};
	});
}

