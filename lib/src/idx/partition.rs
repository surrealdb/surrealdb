use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Included, Unbounded};

pub(super) const _MAX_PARTITION_SIZE: usize = 5 * 1024 * 1024; // 5MB
pub(super) const _HALF_PARTITION_SIZE: usize = _MAX_PARTITION_SIZE / 2;

#[derive(Default, Serialize, Deserialize)]
pub(super) struct PartitionMap {
	partitions: HashMap<u32, PartitionInfo>,
	next_partition_id: u32,
	#[serde(skip)]
	sorted_partitions: BTreeMap<String, u32>,
	#[serde(skip)]
	updated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartitionInfo {
	lowest: String,
	highest: String,
	size: u32,
}

impl PartitionMap {
	pub(super) fn remap(&mut self) {
		self.sorted_partitions.clear();
		for (id, p) in &self.partitions {
			self.sorted_partitions.insert(p.lowest.clone(), *id);
		}
	}
	/// find the lowest matching partition if any
	pub(super) fn find_partition_id(&self, key: &str) -> Option<u32> {
		let r = (Unbounded, Included(key.to_string()));
		if let Some((_, id)) = self.sorted_partitions.range(r).rev().next() {
			Some(*id)
		} else {
			self.sorted_partitions.values().next().map(|id| *id)
		}
	}

	pub(super) fn new_partition_id(&mut self, key: &str) -> u32 {
		let p = PartitionInfo {
			lowest: key.to_string(),
			highest: key.to_string(),
			size: 0,
		};
		let id = self.next_partition_id;
		self.partitions.insert(id, p);
		self.next_partition_id += 1;
		self.remap();
		self.updated = true;
		id
	}

	pub(super) fn extends_partition_bounds(&mut self, partition_id: u32, key: &str) {
		if let Some(p) = self.partitions.get_mut(&partition_id) {
			let mut need_remap = false;
			if key.lt(p.lowest.as_str()) {
				p.lowest = key.to_string();
				need_remap = true;
			}
			if key.gt(p.highest.as_str()) {
				p.highest = key.to_string();
				need_remap = true;
			}
			if need_remap {
				self.updated = true;
				self.remap();
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::partition::{PartitionInfo, PartitionMap};

	fn build_test_map() -> PartitionMap {
		let p1 = PartitionInfo {
			lowest: "S".to_string(),
			highest: "Y".to_string(),
			size: 0,
		};
		let p2 = PartitionInfo {
			lowest: "K".to_string(),
			highest: "K".to_string(),
			size: 0,
		};
		let p3 = PartitionInfo {
			lowest: "B".to_string(),
			highest: "F".to_string(),
			size: 0,
		};

		let mut map = PartitionMap::default();
		map.partitions.insert(0, p1.clone());
		map.partitions.insert(1, p2.clone());
		map.partitions.insert(2, p3.clone());
		map.next_partition_id = 3;

		map.remap();
		map
	}

	#[test]
	fn partition_map_test() {
		let map = build_test_map();

		// Check serialization / deserialization
		let buf = serde_json::to_vec(&map).unwrap();
		let mut map: PartitionMap = serde_json::from_slice(&buf).unwrap();

		assert!(map.sorted_partitions.is_empty());
		assert!(!map.updated);

		// Check remap
		map.remap();
		assert_eq!(map.sorted_partitions.len(), map.partitions.len());

		// Check that the partitions are sorted by lowest property
		let vec: Vec<u32> = map.sorted_partitions.values().map(|id| *id).collect();
		assert_eq!(vec, vec![2, 1, 0]);
	}

	#[test]
	fn partition_check_find_partition() {
		let map = build_test_map();

		assert_eq!(map.find_partition_id("B"), Some(2), "B");
		assert_eq!(map.find_partition_id("E"), Some(2), "E");
		assert_eq!(map.find_partition_id("F"), Some(2), "F");

		assert_eq!(map.find_partition_id("H"), Some(2), "H");

		assert_eq!(map.find_partition_id("K"), Some(1), "K");

		assert_eq!(map.find_partition_id("M"), Some(1), "M");
		assert_eq!(map.find_partition_id("R"), Some(1), "R");

		assert_eq!(map.find_partition_id("S"), Some(0), "S");
		assert_eq!(map.find_partition_id("Y"), Some(0), "Y");

		assert_eq!(map.find_partition_id("Z"), Some(0), "Z");

		assert_eq!(map.find_partition_id("A"), Some(2), "A");

		assert!(!map.updated);
	}

	fn check_bounds(map: &PartitionMap, id: u32, lowest: &str, highest: &str) {
		let p = map.partitions.get(&id).unwrap();
		assert_eq!(p.lowest.as_str(), lowest);
		assert_eq!(p.highest.as_str(), highest);
	}

	#[test]
	fn partition_check_extends_update_bounds() {
		let mut map = build_test_map();

		map.extends_partition_bounds(1, "K");
		assert!(!map.updated);
		check_bounds(&map, 1, "K", "K");

		map.extends_partition_bounds(1, "L");
		assert!(map.updated);
		check_bounds(&map, 1, "K", "L");

		map.updated = false;
		map.extends_partition_bounds(1, "J");
		assert!(map.updated);
		check_bounds(&map, 1, "J", "L");
	}

	#[test]
	fn partition_check_new_partition() {
		let mut map = PartitionMap::default();
		assert!(!map.updated);
		assert_eq!(map.find_partition_id("S"), None);
		let id = map.new_partition_id("S");
		assert_eq!(id, 0);
		assert_eq!(map.next_partition_id, 1);
		assert!(map.updated);

		assert_eq!(map.find_partition_id("A"), Some(0), "A");
		assert_eq!(map.find_partition_id("S"), Some(0), "S");
		assert_eq!(map.find_partition_id("Z"), Some(0), "Z");
	}
}
