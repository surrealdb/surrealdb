/// Enables `strict` server mode
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-sled",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
#[derive(Debug)]
pub struct Strict;
