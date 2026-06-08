use std::collections::{BTreeMap, BTreeSet};

use storekey::{Writer, decode_borrow, encode_vec};

use crate::{VecMap, VecSet};

#[test]
fn vec_map_btree_storekey_bytes_decode_to_vecmap() {
	let pairs = [("a", "1"), ("b", "2"), ("c", "3")];
	let bt: BTreeMap<_, _> =
		pairs.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
	let bytes = encode_vec(&bt).unwrap();
	let vm: VecMap<String, String> =
		pairs.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
	let decoded: VecMap<String, String> = decode_borrow(&bytes).unwrap();
	assert_eq!(decoded, vm);
}

#[test]
fn vec_set_btree_storekey_bytes_decode_to_vecset() {
	let vals = [1u32, 2, 3, 4];
	let bt: BTreeSet<_> = vals.iter().copied().collect();
	let bytes = encode_vec(&bt).unwrap();
	let vs: VecSet<_> = vals.iter().copied().collect();
	let decoded: VecSet<u32> = decode_borrow(&bytes).unwrap();
	assert_eq!(decoded, vs);
}

#[test]
fn vec_map_storekey_matches_btree_map() {
	let pairs = [("a", 1u32), ("b", 2), ("c", 3)];
	let bt: BTreeMap<_, _> = pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
	let vm: VecMap<_, _> = pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
	assert_eq!(encode_vec(&bt).unwrap(), encode_vec(&vm).unwrap());
}

#[test]
fn vec_set_storekey_matches_btree_set() {
	let vals = [1u32, 2, 3, 4];
	let bt: BTreeSet<_> = vals.into_iter().collect();
	let vs: VecSet<_> = vals.into_iter().collect();
	assert_eq!(encode_vec(&bt).unwrap(), encode_vec(&vs).unwrap());
}

#[test]
fn vec_map_roundtrip_borrow_decode() {
	let vm: VecMap<String, u32> = [("x".into(), 10), ("y".into(), 20)].into_iter().collect();
	let bytes = encode_vec(&vm).unwrap();
	let decoded: VecMap<String, u32> = decode_borrow(&bytes).unwrap();
	assert_eq!(decoded, vm);
}

#[test]
fn vec_set_roundtrip_borrow_decode() {
	let vs: VecSet<u32> = [5u32, 1, 9].into_iter().collect();
	let bytes = encode_vec(&vs).unwrap();
	let decoded: VecSet<u32> = decode_borrow(&bytes).unwrap();
	assert_eq!(decoded, vs);
}

#[test]
fn vec_map_merge_sorted_prefer_rhs() {
	let a: VecMap<&str, i32> =
		VecMap::from_sorted_vec_unchecked(vec![("a", 1), ("c", 3), ("e", 5)]);
	let b: VecMap<&str, i32> =
		VecMap::from_sorted_vec_unchecked(vec![("b", 2), ("c", 30), ("d", 4)]);
	let m = VecMap::merge_sorted_prefer_rhs(a, b);
	let expected: VecMap<&str, i32> =
		VecMap::from_sorted_vec_unchecked(vec![("a", 1), ("b", 2), ("c", 30), ("d", 4), ("e", 5)]);
	assert_eq!(m, expected);
}

#[test]
fn vec_map_push_matches_collect_when_monotonic() {
	let mut m = VecMap::new();
	m.push("a", 1u32);
	m.push("b", 2);
	m.push("c", 3);
	let collected: VecMap<_, _> = [("a", 1), ("b", 2), ("c", 3)].into_iter().collect();
	assert_eq!(m, collected);
}

#[test]
fn vec_map_from_iter_duplicate_keys_last_wins() {
	let collected: VecMap<_, _> = vec![("a", 1u32), ("b", 2), ("a", 3)].into_iter().collect();
	let mut manual = VecMap::new();
	manual.insert("a", 1);
	manual.insert("b", 2);
	manual.insert("a", 3);
	assert_eq!(collected, manual);
	assert_eq!(collected.get("a"), Some(&3));
}

#[test]
fn vec_set_push_matches_collect_when_monotonic() {
	let mut s = VecSet::new();
	s.push(1u32);
	s.push(2);
	s.push(3);
	let collected: VecSet<_> = [1u32, 2, 3].into_iter().collect();
	assert_eq!(s, collected);
}

/// Encodes a `VecMap`-shaped storekey stream with the given key order (must match `Encode`).
fn storekey_map_bytes_u32(pairs: &[(u32, u32)]) -> Vec<u8> {
	let mut buf = Vec::new();
	let mut w = Writer::new(&mut buf);
	for &(k, v) in pairs {
		w.mark_terminator();
		w.write_u32(k).unwrap();
		w.write_u32(v).unwrap();
	}
	w.write_terminator().unwrap();
	buf
}

/// Encodes a `VecSet`-shaped storekey stream with the given element order (must match `Encode`).
fn storekey_set_bytes_u32(values: &[u32]) -> Vec<u8> {
	let mut buf = Vec::new();
	let mut w = Writer::new(&mut buf);
	for &x in values {
		w.mark_terminator();
		w.write_u32(x).unwrap();
	}
	w.write_terminator().unwrap();
	buf
}

#[test]
fn vec_map_storekey_decode_rejects_descending_keys() {
	let bytes = storekey_map_bytes_u32(&[(2, 0), (1, 0)]);
	assert!(matches!(
		decode_borrow::<VecMap<u32, u32>>(&bytes),
		Err(storekey::DecodeError::InvalidFormat)
	));
}

#[test]
fn vec_map_storekey_decode_rejects_duplicate_keys() {
	let bytes = storekey_map_bytes_u32(&[(1, 0), (1, 1)]);
	assert!(matches!(
		decode_borrow::<VecMap<u32, u32>>(&bytes),
		Err(storekey::DecodeError::InvalidFormat)
	));
}

#[test]
fn vec_set_storekey_decode_rejects_descending_elements() {
	let bytes = storekey_set_bytes_u32(&[2, 1]);
	assert!(matches!(
		decode_borrow::<VecSet<u32>>(&bytes),
		Err(storekey::DecodeError::InvalidFormat)
	));
}

#[test]
fn vec_set_storekey_decode_rejects_duplicate_elements() {
	let bytes = storekey_set_bytes_u32(&[1, 1]);
	assert!(matches!(
		decode_borrow::<VecSet<u32>>(&bytes),
		Err(storekey::DecodeError::InvalidFormat)
	));
}
