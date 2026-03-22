use std::collections::{BTreeSet, BinaryHeap, HashSet, LinkedList, VecDeque};

use surrealdb_types::{Array, Kind, Number, SurrealValue, Value};

// ──────────────────────────────────────────────────────────────
//  BTreeSet<T> — SurrealValue
// ──────────────────────────────────────────────────────────────

mod btreeset {
	use super::*;

	#[test]
	fn kind_of() {
		assert!(matches!(BTreeSet::<i64>::kind_of(), Kind::Array(_, _)));
	}

	#[test]
	fn empty_roundtrip() {
		let set = BTreeSet::<i64>::new();
		let value = set.clone().into_value();
		assert!(matches!(value, Value::Array(_)));
		assert!(BTreeSet::<i64>::is_value(&value));
		let recovered = BTreeSet::<i64>::from_value(value).unwrap();
		assert_eq!(recovered, set);
	}

	#[test]
	fn with_integers_roundtrip() {
		let set: BTreeSet<i64> = [3, 1, 4, 1, 5, 9].into_iter().collect();
		let value = set.clone().into_value();
		assert!(matches!(value, Value::Array(_)));
		let recovered = BTreeSet::<i64>::from_value(value).unwrap();
		assert_eq!(recovered, set);
	}

	#[test]
	fn with_strings_roundtrip() {
		let set: BTreeSet<String> =
			["hello".to_string(), "world".to_string(), "hello".to_string()].into_iter().collect();
		let value = set.clone().into_value();
		let recovered = BTreeSet::<String>::from_value(value).unwrap();
		assert_eq!(recovered, set);
		assert_eq!(recovered.len(), 2);
	}

	#[test]
	fn is_value_accepts_valid_array() {
		let value = Value::Array(Array::from(vec![1_i64, 2, 3]));
		assert!(BTreeSet::<i64>::is_value(&value));
	}

	#[test]
	fn is_value_rejects_wrong_element_type() {
		let value = Value::Array(Array::from(vec!["a".to_string(), "b".to_string()]));
		assert!(!BTreeSet::<i64>::is_value(&value));
	}

	#[test]
	fn is_value_rejects_non_array() {
		assert!(!BTreeSet::<i64>::is_value(&Value::Bool(true)));
		assert!(!BTreeSet::<i64>::is_value(&Value::Number(Number::Int(1))));
		assert!(!BTreeSet::<i64>::is_value(&Value::String("test".into())));
		assert!(!BTreeSet::<i64>::is_value(&Value::None));
		assert!(!BTreeSet::<i64>::is_value(&Value::Null));
	}

	#[test]
	fn from_value_wrong_type_errors() {
		assert!(BTreeSet::<i64>::from_value(Value::Bool(true)).is_err());
		assert!(BTreeSet::<i64>::from_value(Value::String("test".into())).is_err());
		assert!(BTreeSet::<i64>::from_value(Value::None).is_err());
	}

	#[test]
	fn from_value_wrong_element_type_errors() {
		let value = Value::Array(Array::from(vec!["not_a_number".to_string()]));
		assert!(BTreeSet::<i64>::from_value(value).is_err());
	}

	#[test]
	fn deduplicates_from_array_with_duplicates() {
		let value = Value::Array(Array::from(vec![1_i64, 2, 2, 3, 3, 3]));
		let recovered = BTreeSet::<i64>::from_value(value).unwrap();
		assert_eq!(recovered.len(), 3);
		assert!(recovered.contains(&1));
		assert!(recovered.contains(&2));
		assert!(recovered.contains(&3));
	}

	#[test]
	fn into_value_produces_sorted_array() {
		let set: BTreeSet<i64> = [5, 3, 1, 4, 2].into_iter().collect();
		let value = set.into_value();
		let Value::Array(arr) = value else {
			panic!("expected Array");
		};
		let values: Vec<i64> = arr.into_iter().map(|v| i64::from_value(v).unwrap()).collect();
		assert_eq!(values, vec![1, 2, 3, 4, 5]);
	}

	#[test]
	fn nested_option_elements() {
		let set: BTreeSet<i64> = [10, 20].into_iter().collect();
		let opt = Some(set.clone());
		let value = opt.into_value();
		let recovered = Option::<BTreeSet<i64>>::from_value(value).unwrap();
		assert_eq!(recovered, Some(set));
	}
}

// ──────────────────────────────────────────────────────────────
//  VecDeque<T> — SurrealValue
// ──────────────────────────────────────────────────────────────

mod vecdeque {
	use super::*;

	#[test]
	fn kind_of() {
		assert!(matches!(VecDeque::<i64>::kind_of(), Kind::Array(_, _)));
	}

	#[test]
	fn empty_roundtrip() {
		let deque = VecDeque::<i64>::new();
		let value = deque.clone().into_value();
		assert!(matches!(value, Value::Array(_)));
		assert!(VecDeque::<i64>::is_value(&value));
		let recovered = VecDeque::<i64>::from_value(value).unwrap();
		assert_eq!(recovered, deque);
	}

	#[test]
	fn with_integers_roundtrip() {
		let deque: VecDeque<i64> = [1, 2, 3, 4, 5].into_iter().collect();
		let value = deque.clone().into_value();
		assert!(matches!(value, Value::Array(_)));
		let recovered = VecDeque::<i64>::from_value(value).unwrap();
		assert_eq!(recovered, deque);
	}

	#[test]
	fn with_strings_roundtrip() {
		let deque: VecDeque<String> =
			["alpha".to_string(), "beta".to_string(), "gamma".to_string()].into_iter().collect();
		let value = deque.clone().into_value();
		let recovered = VecDeque::<String>::from_value(value).unwrap();
		assert_eq!(recovered, deque);
	}

	#[test]
	fn preserves_order() {
		let deque: VecDeque<i64> = [10, 20, 30, 40, 50].into_iter().collect();
		let value = deque.into_value();
		let recovered = VecDeque::<i64>::from_value(value).unwrap();
		let items: Vec<i64> = recovered.into_iter().collect();
		assert_eq!(items, vec![10, 20, 30, 40, 50]);
	}

	#[test]
	fn preserves_duplicates() {
		let deque: VecDeque<i64> = [1, 2, 2, 3, 3, 3].into_iter().collect();
		let value = deque.clone().into_value();
		let recovered = VecDeque::<i64>::from_value(value).unwrap();
		assert_eq!(recovered.len(), 6);
		assert_eq!(recovered, deque);
	}

	#[test]
	fn is_value_accepts_valid_array() {
		let value = Value::Array(Array::from(vec![1_i64, 2, 3]));
		assert!(VecDeque::<i64>::is_value(&value));
	}

	#[test]
	fn is_value_rejects_wrong_element_type() {
		let value = Value::Array(Array::from(vec!["a".to_string(), "b".to_string()]));
		assert!(!VecDeque::<i64>::is_value(&value));
	}

	#[test]
	fn is_value_rejects_non_array() {
		assert!(!VecDeque::<i64>::is_value(&Value::Bool(true)));
		assert!(!VecDeque::<i64>::is_value(&Value::Number(Number::Int(1))));
		assert!(!VecDeque::<i64>::is_value(&Value::String("test".into())));
		assert!(!VecDeque::<i64>::is_value(&Value::None));
		assert!(!VecDeque::<i64>::is_value(&Value::Null));
	}

	#[test]
	fn from_value_wrong_type_errors() {
		assert!(VecDeque::<i64>::from_value(Value::Bool(true)).is_err());
		assert!(VecDeque::<i64>::from_value(Value::String("test".into())).is_err());
		assert!(VecDeque::<i64>::from_value(Value::None).is_err());
	}

	#[test]
	fn from_value_wrong_element_type_errors() {
		let value = Value::Array(Array::from(vec!["not_a_number".to_string()]));
		assert!(VecDeque::<i64>::from_value(value).is_err());
	}

	#[test]
	fn nested_option_elements() {
		let deque: VecDeque<i64> = [10, 20].into_iter().collect();
		let opt = Some(deque.clone());
		let value = opt.into_value();
		let recovered = Option::<VecDeque<i64>>::from_value(value).unwrap();
		assert_eq!(recovered, Some(deque));
	}

	#[test]
	fn with_nested_vecs() {
		let deque: VecDeque<Vec<i64>> = [vec![1, 2], vec![3, 4], vec![5]].into_iter().collect();
		let value = deque.clone().into_value();
		let recovered = VecDeque::<Vec<i64>>::from_value(value).unwrap();
		assert_eq!(recovered, deque);
	}
}

// ──────────────────────────────────────────────────────────────
//  BinaryHeap<T> — SurrealValue
// ──────────────────────────────────────────────────────────────

mod binaryheap {
	use super::*;

	#[test]
	fn kind_of() {
		assert!(matches!(BinaryHeap::<i64>::kind_of(), Kind::Array(_, _)));
	}

	#[test]
	fn empty_roundtrip() {
		let heap = BinaryHeap::<i64>::new();
		let value = heap.into_value();
		assert!(matches!(value, Value::Array(_)));
		assert!(BinaryHeap::<i64>::is_value(&value));
		let recovered = BinaryHeap::<i64>::from_value(value).unwrap();
		assert_eq!(recovered.len(), 0);
	}

	#[test]
	fn with_integers_roundtrip() {
		let heap: BinaryHeap<i64> = [3, 1, 4, 1, 5].into_iter().collect();
		let original_sorted: Vec<i64> = heap.clone().into_sorted_vec();
		let value = heap.into_value();
		assert!(matches!(value, Value::Array(_)));
		let recovered = BinaryHeap::<i64>::from_value(value).unwrap();
		assert_eq!(recovered.into_sorted_vec(), original_sorted);
	}

	#[test]
	fn with_strings_roundtrip() {
		let heap: BinaryHeap<String> =
			["charlie".to_string(), "alpha".to_string(), "bravo".to_string()].into_iter().collect();
		let original_sorted = heap.clone().into_sorted_vec();
		let value = heap.into_value();
		let recovered = BinaryHeap::<String>::from_value(value).unwrap();
		assert_eq!(recovered.into_sorted_vec(), original_sorted);
	}

	#[test]
	fn preserves_all_elements_including_duplicates() {
		let heap: BinaryHeap<i64> = [1, 2, 2, 3, 3, 3].into_iter().collect();
		let value = heap.into_value();
		let recovered = BinaryHeap::<i64>::from_value(value).unwrap();
		assert_eq!(recovered.len(), 6);
		assert_eq!(recovered.into_sorted_vec(), vec![1, 2, 2, 3, 3, 3]);
	}

	#[test]
	fn is_value_accepts_valid_array() {
		let value = Value::Array(Array::from(vec![1_i64, 2, 3]));
		assert!(BinaryHeap::<i64>::is_value(&value));
	}

	#[test]
	fn is_value_rejects_wrong_element_type() {
		let value = Value::Array(Array::from(vec!["a".to_string(), "b".to_string()]));
		assert!(!BinaryHeap::<i64>::is_value(&value));
	}

	#[test]
	fn is_value_rejects_non_array() {
		assert!(!BinaryHeap::<i64>::is_value(&Value::Bool(true)));
		assert!(!BinaryHeap::<i64>::is_value(&Value::Number(Number::Int(1))));
		assert!(!BinaryHeap::<i64>::is_value(&Value::String("test".into())));
		assert!(!BinaryHeap::<i64>::is_value(&Value::None));
		assert!(!BinaryHeap::<i64>::is_value(&Value::Null));
	}

	#[test]
	fn from_value_wrong_type_errors() {
		assert!(BinaryHeap::<i64>::from_value(Value::Bool(true)).is_err());
		assert!(BinaryHeap::<i64>::from_value(Value::String("test".into())).is_err());
		assert!(BinaryHeap::<i64>::from_value(Value::None).is_err());
	}

	#[test]
	fn from_value_wrong_element_type_errors() {
		let value = Value::Array(Array::from(vec!["not_a_number".to_string()]));
		assert!(BinaryHeap::<i64>::from_value(value).is_err());
	}

	#[test]
	fn nested_option_elements() {
		let heap: BinaryHeap<i64> = [10, 20].into_iter().collect();
		let sorted = heap.clone().into_sorted_vec();
		let opt = Some(heap);
		let value = opt.into_value();
		let recovered = Option::<BinaryHeap<i64>>::from_value(value).unwrap().unwrap();
		assert_eq!(recovered.into_sorted_vec(), sorted);
	}
}

// ──────────────────────────────────────────────────────────────
//  From<Collection<T>> for Array
// ──────────────────────────────────────────────────────────────

mod array_from_collections {
	use super::*;

	#[test]
	fn from_hashset_empty() {
		let set = HashSet::<i64>::new();
		let arr = Array::from(set);
		assert!(arr.is_empty());
	}

	#[test]
	fn from_hashset_with_data() {
		let set: HashSet<i64> = [1, 2, 3].into_iter().collect();
		let arr = Array::from(set);
		assert_eq!(arr.len(), 3);
		let mut values: Vec<i64> = arr.into_iter().map(|v| i64::from_value(v).unwrap()).collect();
		values.sort();
		assert_eq!(values, vec![1, 2, 3]);
	}

	#[test]
	fn from_hashset_with_strings() {
		let set: HashSet<String> =
			["a".to_string(), "b".to_string(), "c".to_string()].into_iter().collect();
		let arr = Array::from(set);
		assert_eq!(arr.len(), 3);
	}

	#[test]
	fn from_btreeset_empty() {
		let set = BTreeSet::<i64>::new();
		let arr = Array::from(set);
		assert!(arr.is_empty());
	}

	#[test]
	fn from_btreeset_with_data() {
		let set: BTreeSet<i64> = [3, 1, 2].into_iter().collect();
		let arr = Array::from(set);
		assert_eq!(arr.len(), 3);
		let values: Vec<i64> = arr.into_iter().map(|v| i64::from_value(v).unwrap()).collect();
		assert_eq!(values, vec![1, 2, 3]);
	}

	#[test]
	fn from_btreeset_preserves_sorted_order() {
		let set: BTreeSet<String> =
			["cherry".to_string(), "apple".to_string(), "banana".to_string()].into_iter().collect();
		let arr = Array::from(set);
		let values: Vec<String> = arr.into_iter().map(|v| String::from_value(v).unwrap()).collect();
		assert_eq!(values, vec!["apple", "banana", "cherry"]);
	}

	#[test]
	fn from_vecdeque_empty() {
		let deque = VecDeque::<i64>::new();
		let arr = Array::from(deque);
		assert!(arr.is_empty());
	}

	#[test]
	fn from_vecdeque_with_data() {
		let deque: VecDeque<i64> = [10, 20, 30].into_iter().collect();
		let arr = Array::from(deque);
		assert_eq!(arr.len(), 3);
		let values: Vec<i64> = arr.into_iter().map(|v| i64::from_value(v).unwrap()).collect();
		assert_eq!(values, vec![10, 20, 30]);
	}

	#[test]
	fn from_vecdeque_preserves_order() {
		let deque: VecDeque<String> =
			["first".to_string(), "second".to_string(), "third".to_string()].into_iter().collect();
		let arr = Array::from(deque);
		let values: Vec<String> = arr.into_iter().map(|v| String::from_value(v).unwrap()).collect();
		assert_eq!(values, vec!["first", "second", "third"]);
	}

	#[test]
	fn from_linkedlist_empty() {
		let list = LinkedList::<i64>::new();
		let arr = Array::from(list);
		assert!(arr.is_empty());
	}

	#[test]
	fn from_linkedlist_with_data() {
		let list: LinkedList<i64> = [100, 200, 300].into_iter().collect();
		let arr = Array::from(list);
		assert_eq!(arr.len(), 3);
		let values: Vec<i64> = arr.into_iter().map(|v| i64::from_value(v).unwrap()).collect();
		assert_eq!(values, vec![100, 200, 300]);
	}

	#[test]
	fn from_linkedlist_preserves_order() {
		let list: LinkedList<String> =
			["x".to_string(), "y".to_string(), "z".to_string()].into_iter().collect();
		let arr = Array::from(list);
		let values: Vec<String> = arr.into_iter().map(|v| String::from_value(v).unwrap()).collect();
		assert_eq!(values, vec!["x", "y", "z"]);
	}

	#[test]
	fn from_binaryheap_empty() {
		let heap = BinaryHeap::<i64>::new();
		let arr = Array::from(heap);
		assert!(arr.is_empty());
	}

	#[test]
	fn from_binaryheap_with_data() {
		let heap: BinaryHeap<i64> = [5, 3, 1, 4, 2].into_iter().collect();
		let arr = Array::from(heap);
		assert_eq!(arr.len(), 5);
		let mut values: Vec<i64> = arr.into_iter().map(|v| i64::from_value(v).unwrap()).collect();
		values.sort();
		assert_eq!(values, vec![1, 2, 3, 4, 5]);
	}

	#[test]
	fn from_binaryheap_with_duplicates() {
		let heap: BinaryHeap<i64> = [1, 1, 2, 2, 3].into_iter().collect();
		let arr = Array::from(heap);
		assert_eq!(arr.len(), 5);
	}
}

// ──────────────────────────────────────────────────────────────
//  Cross-collection compatibility — verify that values produced
//  by one collection type can be consumed by another, since they
//  all share the Value::Array representation.
// ──────────────────────────────────────────────────────────────

mod cross_collection_compat {
	use super::*;

	#[test]
	fn vec_to_vecdeque() {
		let vec = vec![1_i64, 2, 3];
		let value = vec.into_value();
		let deque = VecDeque::<i64>::from_value(value).unwrap();
		let items: Vec<i64> = deque.into_iter().collect();
		assert_eq!(items, vec![1, 2, 3]);
	}

	#[test]
	fn vec_to_linkedlist() {
		let vec = vec![1_i64, 2, 3];
		let value = vec.into_value();
		let list = LinkedList::<i64>::from_value(value).unwrap();
		let items: Vec<i64> = list.into_iter().collect();
		assert_eq!(items, vec![1, 2, 3]);
	}

	#[test]
	fn vec_to_hashset() {
		let vec = vec![1_i64, 2, 3];
		let value = vec.into_value();
		let set = HashSet::<i64>::from_value(value).unwrap();
		assert_eq!(set.len(), 3);
		assert!(set.contains(&1));
		assert!(set.contains(&2));
		assert!(set.contains(&3));
	}

	#[test]
	fn vec_to_btreeset() {
		let vec = vec![3_i64, 1, 2];
		let value = vec.into_value();
		let set = BTreeSet::<i64>::from_value(value).unwrap();
		assert_eq!(set.len(), 3);
		let items: Vec<i64> = set.into_iter().collect();
		assert_eq!(items, vec![1, 2, 3]);
	}

	#[test]
	fn vec_to_binaryheap() {
		let vec = vec![3_i64, 1, 2];
		let value = vec.into_value();
		let heap = BinaryHeap::<i64>::from_value(value).unwrap();
		assert_eq!(heap.into_sorted_vec(), vec![1, 2, 3]);
	}

	#[test]
	fn btreeset_to_vec() {
		let set: BTreeSet<i64> = [3, 1, 2].into_iter().collect();
		let value = set.into_value();
		let vec = Vec::<i64>::from_value(value).unwrap();
		assert_eq!(vec, vec![1, 2, 3]);
	}

	#[test]
	fn hashset_to_btreeset() {
		let set: HashSet<i64> = [1, 2, 3].into_iter().collect();
		let value = set.into_value();
		let btree = BTreeSet::<i64>::from_value(value).unwrap();
		assert_eq!(btree.len(), 3);
	}

	#[test]
	fn vecdeque_to_vec() {
		let deque: VecDeque<i64> = [10, 20, 30].into_iter().collect();
		let value = deque.into_value();
		let vec = Vec::<i64>::from_value(value).unwrap();
		assert_eq!(vec, vec![10, 20, 30]);
	}

	#[test]
	fn linkedlist_to_vecdeque() {
		let list: LinkedList<i64> = [4, 5, 6].into_iter().collect();
		let value = list.into_value();
		let deque = VecDeque::<i64>::from_value(value).unwrap();
		let items: Vec<i64> = deque.into_iter().collect();
		assert_eq!(items, vec![4, 5, 6]);
	}

	#[test]
	fn binaryheap_to_vec() {
		let heap: BinaryHeap<i64> = [5, 3, 1].into_iter().collect();
		let value = heap.into_value();
		let mut vec = Vec::<i64>::from_value(value).unwrap();
		vec.sort();
		assert_eq!(vec, vec![1, 3, 5]);
	}
}

// ──────────────────────────────────────────────────────────────
//  Pre-existing collection types — regression tests to ensure
//  the existing LinkedList and HashSet impls still work after
//  our changes.
// ──────────────────────────────────────────────────────────────

mod existing_collections_regression {
	use super::*;

	#[test]
	fn linkedlist_empty_roundtrip() {
		let list = LinkedList::<i64>::new();
		let value = list.clone().into_value();
		assert!(matches!(value, Value::Array(_)));
		assert!(LinkedList::<i64>::is_value(&value));
		let recovered = LinkedList::<i64>::from_value(value).unwrap();
		assert_eq!(recovered, list);
	}

	#[test]
	fn linkedlist_with_data_roundtrip() {
		let list: LinkedList<i64> = [1, 2, 3, 4, 5].into_iter().collect();
		let value = list.clone().into_value();
		let recovered = LinkedList::<i64>::from_value(value).unwrap();
		assert_eq!(recovered, list);
	}

	#[test]
	fn linkedlist_preserves_order() {
		let list: LinkedList<i64> = [50, 40, 30, 20, 10].into_iter().collect();
		let value = list.into_value();
		let recovered = LinkedList::<i64>::from_value(value).unwrap();
		let items: Vec<i64> = recovered.into_iter().collect();
		assert_eq!(items, vec![50, 40, 30, 20, 10]);
	}

	#[test]
	fn linkedlist_with_strings() {
		let list: LinkedList<String> = ["one".to_string(), "two".to_string()].into_iter().collect();
		let value = list.clone().into_value();
		let recovered = LinkedList::<String>::from_value(value).unwrap();
		assert_eq!(recovered, list);
	}

	#[test]
	fn linkedlist_kind_of() {
		assert!(matches!(LinkedList::<i64>::kind_of(), Kind::Array(_, _)));
	}

	#[test]
	fn linkedlist_from_value_wrong_type_errors() {
		assert!(LinkedList::<i64>::from_value(Value::Bool(true)).is_err());
	}

	#[test]
	fn linkedlist_from_value_wrong_element_errors() {
		let value = Value::Array(Array::from(vec!["not_int".to_string()]));
		assert!(LinkedList::<i64>::from_value(value).is_err());
	}

	#[test]
	fn hashset_empty_roundtrip() {
		let set = HashSet::<i64>::new();
		let value = set.clone().into_value();
		assert!(matches!(value, Value::Array(_)));
		assert!(HashSet::<i64>::is_value(&value));
		let recovered = HashSet::<i64>::from_value(value).unwrap();
		assert_eq!(recovered, set);
	}

	#[test]
	fn hashset_with_data_roundtrip() {
		let set: HashSet<i64> = [1, 2, 3].into_iter().collect();
		let value = set.clone().into_value();
		let recovered = HashSet::<i64>::from_value(value).unwrap();
		assert_eq!(recovered, set);
	}

	#[test]
	fn hashset_with_strings() {
		let set: HashSet<String> = ["hello".to_string(), "world".to_string()].into_iter().collect();
		let value = set.clone().into_value();
		let recovered = HashSet::<String>::from_value(value).unwrap();
		assert_eq!(recovered, set);
	}

	#[test]
	fn hashset_deduplicates() {
		let value = Value::Array(Array::from(vec![1_i64, 1, 2, 2, 3]));
		let set = HashSet::<i64>::from_value(value).unwrap();
		assert_eq!(set.len(), 3);
	}

	#[test]
	fn hashset_kind_of() {
		assert!(matches!(HashSet::<i64>::kind_of(), Kind::Array(_, _)));
	}

	#[test]
	fn hashset_from_value_wrong_type_errors() {
		assert!(HashSet::<i64>::from_value(Value::Bool(true)).is_err());
	}

	#[test]
	fn hashset_from_value_wrong_element_errors() {
		let value = Value::Array(Array::from(vec!["not_int".to_string()]));
		assert!(HashSet::<i64>::from_value(value).is_err());
	}
}

// ──────────────────────────────────────────────────────────────
//  Value::is::<Collection<T>>() generic helper — ensure the
//  blanket is_value / is::<T>() path works for all collections.
// ──────────────────────────────────────────────────────────────

mod value_is_generic {
	use super::*;

	#[test]
	fn is_btreeset() {
		let value = Value::Array(Array::from(vec![1_i64, 2, 3]));
		assert!(value.is::<BTreeSet<i64>>());
		assert!(!Value::Bool(true).is::<BTreeSet<i64>>());
	}

	#[test]
	fn is_vecdeque() {
		let value = Value::Array(Array::from(vec![1_i64, 2, 3]));
		assert!(value.is::<VecDeque<i64>>());
		assert!(!Value::Bool(true).is::<VecDeque<i64>>());
	}

	#[test]
	fn is_binaryheap() {
		let value = Value::Array(Array::from(vec![1_i64, 2, 3]));
		assert!(value.is::<BinaryHeap<i64>>());
		assert!(!Value::Bool(true).is::<BinaryHeap<i64>>());
	}

	#[test]
	fn is_linkedlist() {
		let value = Value::Array(Array::from(vec![1_i64, 2, 3]));
		assert!(value.is::<LinkedList<i64>>());
		assert!(!Value::Bool(true).is::<LinkedList<i64>>());
	}

	#[test]
	fn is_hashset() {
		let value = Value::Array(Array::from(vec![1_i64, 2, 3]));
		assert!(value.is::<HashSet<i64>>());
		assert!(!Value::Bool(true).is::<HashSet<i64>>());
	}

	#[test]
	fn rejects_mismatched_element_types() {
		let value = Value::Array(Array::from(vec!["a".to_string()]));
		assert!(!value.is::<BTreeSet<i64>>());
		assert!(!value.is::<VecDeque<i64>>());
		assert!(!value.is::<BinaryHeap<i64>>());
		assert!(!value.is::<LinkedList<i64>>());
		assert!(!value.is::<HashSet<i64>>());
	}
}
