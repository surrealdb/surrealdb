#[cfg(test)]
mod tests {
	use crate::kvs::{Key, Val};
	use rand::Rng;
	use std::collections::{BTreeMap, HashSet};
	use std::time::SystemTime;

	const N: usize = 1000;
	const M: usize = 1000;
	const KEY: Key = Key::new();

	fn get_randomized_map(random: u16) -> BTreeMap<Key, Val> {
		let mut map = BTreeMap::<Key, Val>::new();
		if random == 0 {
			map.insert(KEY, Val::new());
		}
		map
	}

	fn get_some_randomized_map(random: u16) -> Option<BTreeMap<Key, Val>> {
		if random == 1 {
			None
		} else {
			Some(get_randomized_map(random))
		}
	}

	fn get_none_randomized_map(random: u16) -> Option<BTreeMap<Key, Val>> {
		if random == 2 {
			Some(get_randomized_map(random))
		} else {
			None
		}
	}

	/// This test justifies the rationale of enclosing the `BTreeMap` in an `Option`.
	/// Mostly, this test evaluates the performance of `Option::is_none` versus `BTreeMap::is_empty`.
	/// The test passes if `is_none` is at least 2 times faster than `is_empty`.
	///
	/// We also check the cost of encapsulating the new map in an Option vs just using the new map.
	/// The test passes if the difference is less than 10%.
	///
	/// This test checks only the time factor benefit. But there is also a space factor benefit
	/// as the `BTreeMap` structure contains several properties.
	///
	/// Note that the `BTreeMap` is also using a similar approach by encapsulating the root of the
	/// map in an Option: `root: Option<Root<K, V>>`.
	///
	#[tokio::test]
	async fn test_transaction_btreemap_vs_option_rationale() {
		let mut rng = rand::thread_rng();
		let rnd: u16 = rng.gen();

		// Evaluating BTreeMap creation + BTreeMap::is_empty()
		let time = SystemTime::now();
		for _ in 0..N {
			let map = get_randomized_map(rnd);
			for _ in 0..M {
				assert!(map.is_empty() || map.contains_key(&KEY));
			}
		}
		let map_time = time.elapsed().unwrap();

		// Evaluating Option creation + Option::is_none()
		let time = SystemTime::now();
		for _ in 0..N {
			let map = get_none_randomized_map(rnd);
			for _ in 0..M {
				match &map {
					None => {
						assert!(true)
					}
					Some(map) => {
						// We consume the map to avoid any optimisation
						assert!(map.is_empty() || map.contains_key(&KEY))
					}
				}
			}
		}
		let option_time = time.elapsed().unwrap();

		// Evaluating Map encapsulated in an Option + Option::is_none()
		let time = SystemTime::now();
		for _ in 0..N {
			let map = get_some_randomized_map(rnd);
			for _ in 0..M {
				match &map {
					None => {
						assert!(true)
					}
					Some(map) => {
						// We consume the map to avoid any optimisation
						assert!(map.is_empty() || map.contains_key(&KEY))
					}
				}
			}
		}
		let option_map_time = time.elapsed().unwrap();

		println!("map: {}", map_time.as_micros());
		println!("option: {}", option_time.as_micros());
		println!("option_map: {}", option_map_time.as_micros());

		// If `rnd` is smaller than 3, then the test is not relevant
		if rnd > 2 {
			assert!(option_time <= map_time);
			assert!(option_map_time <= map_time);
		}
	}

	fn get_randomized_set(random: u16) -> HashSet<Key> {
		let mut set = HashSet::<Key>::new();
		if random == 0 {
			set.insert(KEY);
		}
		set
	}

	fn get_some_randomized_set(random: u16) -> Option<HashSet<Key>> {
		if random == 1 {
			None
		} else {
			Some(get_randomized_set(random))
		}
	}

	fn get_none_randomized_set(random: u16) -> Option<HashSet<Key>> {
		if random == 2 {
			Some(get_randomized_set(random))
		} else {
			None
		}
	}

	/// Derivation of `test_transaction_btreemap_vs_option_rationale` for HashSet.
	#[tokio::test]
	async fn test_transaction_hashset_vs_option_rationale() {
		let mut rng = rand::thread_rng();
		let rnd: u16 = rng.gen();

		// Evaluating HashSet creation + HashSet::is_empty()
		let time = SystemTime::now();
		for _ in 0..N {
			let set = get_randomized_set(rnd);
			for _ in 0..M {
				assert!(set.is_empty() || set.contains(&KEY));
			}
		}
		let set_time = time.elapsed().unwrap();

		// Evaluating Option creation + Option::is_none()
		let time = SystemTime::now();
		for _ in 0..N {
			let set = get_none_randomized_set(rnd);
			for _ in 0..M {
				match &set {
					None => assert!(true),
					Some(set) => assert!(set.is_empty() || set.contains(&KEY)),
				}
			}
		}
		let option_time = time.elapsed().unwrap();

		// Evaluating Map encapsulated in an Option + Option::is_none()
		let time = SystemTime::now();
		for _ in 0..N {
			let set = get_some_randomized_set(rnd);
			for _ in 0..M {
				match &set {
					None => assert!(true),
					Some(set) => assert!(set.is_empty() || set.contains(&KEY)),
				}
			}
		}
		let option_set_time = time.elapsed().unwrap();

		println!("set: {}", set_time.as_micros());
		println!("option: {}", option_time.as_micros());
		println!("option_set: {}", option_set_time.as_micros());

		// If `rnd` is smaller than 3, then the test is not relevant
		if rnd > 2 {
			assert!(option_time <= set_time);
			assert!(option_set_time <= set_time);
		}
	}
}
