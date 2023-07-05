/// Enables `strict` server mode
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-speedb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
#[derive(Debug)]
pub struct Strict;
