use std::ops::Range;
use std::sync::{Arc, LazyLock};

use anyhow::Result;
use sysinfo::System;

use crate::kvs::{KVKey, KVValue};

/// Detected total system memory in bytes, cached at first access.
/// Falls back to cgroup limits when running inside a container, and
/// uses a conservative 1 GiB default when `/proc` is inaccessible
/// (e.g. systemd `ProcSubset=pid` hardening).
#[cfg_attr(not(any(feature = "kv-rocksdb", feature = "kv-surrealkv")), allow(dead_code))]
pub(crate) static TOTAL_SYSTEM_MEMORY: LazyLock<u64> = LazyLock::new(|| {
	// Load the system attributes
	let mut system = System::new();
	// Refresh the system memory
	system.refresh_memory();
	// Get the total system memory
	let host_memory = system.total_memory();
	// If the total system memory is 0, use a safe default
	if host_memory == 0 {
		return 1024 * 1024 * 1024;
	}
	// Prefer cgroup limits when available (container environments)
	match system.cgroup_limits() {
		// If the limit has been configured, use it
		Some(l) if l.total_memory > 0 => l.total_memory,
		// Otherwise use the host memory
		_ => host_memory,
	}
});

/// Advances a key to the next value,
/// can be used to skip over a certain key.
pub fn advance_key(key: &mut [u8]) {
	for b in key.iter_mut().rev() {
		*b = b.wrapping_add(1);
		if *b != 0 {
			break;
		}
	}
}

pub fn to_prefix_range<K: KVKey>(key: K) -> Result<Range<Vec<u8>>> {
	let start = key.encode_key()?;
	let mut end = start.clone();
	end.push(0xff);
	Ok(Range {
		start,
		end,
	})
}

/// Takes an iterator of byte slices and deserializes the byte slices to the
/// expected type, returning an error if any of the values fail to serialize.
pub fn deserialize_cache<'a, I, T>(iter: I) -> Result<Arc<[T]>>
where
	T: KVValue,
	I: Iterator<Item = &'a [u8]>,
{
	let mut buf = Vec::new();
	for slice in iter {
		buf.push(T::kv_decode_value(slice.to_vec())?)
	}
	Ok(Arc::from(buf))
}
