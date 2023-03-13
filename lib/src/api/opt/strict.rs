/// Enables `strict` server mode
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
	feature = "kv-redis",
))]
#[derive(Debug)]
pub struct Strict;
