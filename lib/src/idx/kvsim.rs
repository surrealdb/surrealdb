use crate::kvs::{Key, Val};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

pub(super) struct KVSimulator {
	network_latency: Option<Duration>,
	network_transport_per_byte_ns: u64,
	kv: HashMap<Key, Val>,
	bytes_write: usize,
	bytes_read: usize,
	get_count: usize,
	set_count: usize,
}

pub(super) const DEFAULT_NETWORK_LATENCY: usize = 10;
pub(super) const DEFAULT_NETWORK_TRANSPORT_PER_BYTE: usize = 10;

impl Default for KVSimulator {
	fn default() -> Self {
		Self::new(Some(DEFAULT_NETWORK_LATENCY), DEFAULT_NETWORK_TRANSPORT_PER_BYTE)
	}
}

impl KVSimulator {
	pub(super) fn new(
		network_latency_ms: Option<usize>,
		network_transport_per_byte_ns: usize,
	) -> Self {
		Self {
			network_latency: network_latency_ms.map(|t| Duration::from_micros(t as u64)),
			network_transport_per_byte_ns: network_transport_per_byte_ns as u64,
			kv: Default::default(),
			bytes_write: 0,
			bytes_read: 0,
			get_count: 0,
			set_count: 0,
		}
	}

	fn network_transport(&self, bytes: usize) {
		if self.network_transport_per_byte_ns > 0 {
			sleep(Duration::from_nanos((bytes as u64) * self.network_transport_per_byte_ns));
		}
	}

	fn network_latency(&self) {
		if let Some(d) = self.network_latency {
			sleep(d);
		}
	}

	pub(super) fn get_with_size<V: DeserializeOwned>(&mut self, key: &Key) -> Option<(usize, V)> {
		self.network_latency();
		if let Some(val) = self.kv.get(key) {
			self.get_count += 1;
			let bytes = key.len() + val.len();
			self.bytes_read += bytes;
			self.network_transport(bytes);
			Some((
				val.len(),
				bincode::deserialize(val).unwrap_or_else(|e| panic!("Corrupted Index: {:?}", e)),
			))
		} else {
			None
		}
	}

	pub(super) fn get<V: DeserializeOwned>(&mut self, key: &Key) -> Option<V> {
		self.get_with_size(key).map(|(_, value)| value)
	}

	pub(super) fn set<V: Serialize>(&mut self, key: Key, value: &V) -> usize {
		self.network_latency();
		let val = bincode::serialize(value).unwrap();
		self.set_count += 1;
		let bytes = key.len() + val.len();
		self.bytes_write += bytes;
		self.network_transport(bytes);
		let size = val.len();
		self.kv.insert(key, val);
		size
	}

	pub(super) fn print_stats(&self) {
		println!("get count: {}", self.get_count);
		println!("set count: {}", self.set_count);
		println!("bytes read: {}", self.bytes_read);
		println!("bytes write: {}", self.bytes_write);
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::bkeys::{BKeys, FstKeys, TrieKeys};
	use crate::idx::kvsim::KVSimulator;
	use crate::kvs::Key;

	#[test]
	fn test_kv_sim_string() {
		let mut kv = KVSimulator::default();
		let k: Key = "Foo".to_string().into();
		let v = "Bar".to_string();
		kv.set(k.clone(), &v);
		assert_eq!(kv.get_with_size(&k), Some((11, v)));
		assert_eq!(kv.get(&k), Some(11));
		kv.print_stats();
	}

	#[test]
	fn test_kv_sim_trie() {
		let mut kv = KVSimulator::default();
		let k: Key = "Foo".to_string().into();
		let mut keys = TrieKeys::default();
		keys.insert(k.clone(), 9);
		kv.set(k.clone(), &keys);
		let (_, keys) = kv.get_with_size::<TrieKeys>(&k).unwrap();
		assert_eq!(keys.get(&k), Some(9));
	}

	#[test]
	fn test_kv_sim_fst() {
		let mut kv = KVSimulator::default();
		let k: Key = "Foo".to_string().into();
		let mut keys = FstKeys::default();
		keys.insert(k.clone(), 9);
		keys.compile();
		kv.set(k.clone(), &keys);
		let (_, keys) = kv.get_with_size::<FstKeys>(&k).unwrap();
		assert_eq!(keys.get(&k), Some(9));
	}
}
