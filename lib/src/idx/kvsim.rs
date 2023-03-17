use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

pub(super) struct KVSimulator {
	network_latency: Duration,
	network_transport_per_byte_ns: u64,
	kv: HashMap<Vec<u8>, Vec<u8>>,
	bytes_write: usize,
	bytes_read: usize,
	get_count: usize,
	set_count: usize,
}

pub(super) const DEFAULT_NETWORK_LATENCY: usize = 10;
pub(super) const DEFAULT_NETWORK_TRANSPORT_PER_BYTE: usize = 10;

impl Default for KVSimulator {
	fn default() -> Self {
		Self::new(DEFAULT_NETWORK_LATENCY, DEFAULT_NETWORK_TRANSPORT_PER_BYTE)
	}
}

impl KVSimulator {
	fn new(network_latency_ms: usize, network_transport_per_byte_ns: usize) -> Self {
		Self {
			network_latency: Duration::from_micros(network_latency_ms as u64),
			network_transport_per_byte_ns: network_transport_per_byte_ns as u64,
			kv: Default::default(),
			bytes_write: 0,
			bytes_read: 0,
			get_count: 0,
			set_count: 0,
		}
	}

	fn network_transport(&self, bytes: usize) {
		sleep(Duration::from_nanos((bytes as u64) * self.network_transport_per_byte_ns));
	}

	pub(super) fn get<V: DeserializeOwned>(&mut self, key: &[u8]) -> Option<V> {
		sleep(self.network_latency);
		if let Some(vec) = self.kv.get(key) {
			self.get_count += 1;
			let bytes = key.len() + vec.len();
			self.bytes_read += bytes;
			self.network_transport(bytes);
			return Some(serde_json::from_slice(vec).unwrap());
		}
		None
	}

	pub(super) fn set<V: Serialize>(&mut self, key: Vec<u8>, value: &V) {
		sleep(self.network_latency);
		let val = serde_json::to_vec(value).unwrap();
		self.set_count += 1;
		let bytes = key.len() + val.len();
		self.bytes_write += bytes;
		self.network_transport(bytes);
		self.kv.insert(key, val);
	}

	pub(super) fn print_stats(&self) {
		println!("get count: {}", self.get_count);
		println!("set count: {}", self.set_count);
		println!("bytes read: {}", self.bytes_read);
		println!("bytes write: {}", self.bytes_write);
	}
}
