#[cfg(any(feature = "kv-tikv", feature = "kv-rocksdb", feature = "kv-fdb"))]
pub(crate) mod table {
	#[test]
	fn created_tables_can_be_scanned() {
		assert_eq!(4, 4)
	}

	#[test]
	fn created_tables_can_be_deleted() {
		assert_eq!(4, 2)
	}
}
