use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

#[derive(Default)]
pub(super) struct KVSimulator {
	kv: HashMap<Vec<u8>, Vec<u8>>,
}

const NETWORK_LATENCY: Duration = Duration::from_micros(10);
const NANO_SECS_PER_BYTE_TRANSPORT: u64 = 10;

impl KVSimulator {
	fn network_transport(bytes: usize) {
		sleep(Duration::from_nanos((bytes as u64) * NANO_SECS_PER_BYTE_TRANSPORT));
	}

	pub(super) fn get<V: DeserializeOwned>(&self, key: &[u8]) -> Option<V> {
		sleep(NETWORK_LATENCY);
		let key = key.as_ref();
		if let Some(vec) = self.kv.get(key) {
			Self::network_transport(key.len() + vec.len());
			return Some(serde_json::from_slice(vec).unwrap());
		}
		None
	}

	pub(super) fn set<V: Serialize>(&mut self, key: Vec<u8>, value: &V) {
		sleep(NETWORK_LATENCY);
		let val = serde_json::to_vec(value).unwrap();
		Self::network_transport(key.len() + val.len());
		self.kv.insert(key, val);
	}

	pub(super) fn len(&self) -> usize {
		self.kv.len()
	}
}
