//! The index and graph-adjacency engines' configurable resource limits.
//!
//! A knob lives here when this layer is the lowest that reads it: layers
//! above only thread the values down, so this layer owns each knob's
//! invariant.
//!
//! `file_allowlist` belongs here for the same reason. The allowlist is only
//! ever *enforced* by `crate::file::check_is_path_allowed`, and its only
//! callers are the two analyzer mapper loaders —
//! `idx::trees::store::mapper`, which caches a mapper per
//! `DEFINE ANALYZER … mapper('<path>')` path, and `idx::ft::analyzer::mapper`,
//! which reads the term file itself. Every other site in the engine merely
//! threads the value down to one of those two, so idx is the lowest layer that
//! reads it and therefore the owner of its invariant: an empty allowlist denies
//! every path, so file access must be configured explicitly.

use std::path::PathBuf;

use surrealdb_cnf as cnf;

/// Configuration of the index engines.
#[derive(Clone, Debug)]
pub struct IdxConfig {
	/// Batch size used when allocating sequence-based document IDs for a table's
	/// shared doc-ID space (used by full-text, HNSW and DiskANN indexes). Larger
	/// batches reduce coordination on the distributed sequence at the cost of
	/// larger gaps when a node is lost before exhausting its current batch.
	/// (default: 1000)
	pub table_doc_ids_batch_size: u32,
	/// The maximum total size of the HNSW ANN cache (default: 256 MiB)
	pub hnsw_cache_size: u64,
	/// The maximum total size of the DiskANN ANN cache (default: 256 MiB)
	pub diskann_cache_size: u64,
	/// Specifies a list of paths in which files can be accessed (default: empty)
	pub file_allowlist: Vec<PathBuf>,
	/// How many unfolded per-edge adjacency keys a graph traversal tolerates
	/// on one (vertex, direction) scope before observing it for a background
	/// fold into packed blocks. Folding is always on at this built-in
	/// threshold: the value is deliberately not read from the config map, so
	/// every node against a shared datastore folds identically. `0` disables
	/// folding entirely — nothing is observed, queued or folded — and exists
	/// for code constructing the config directly (tests needing a quiescent
	/// adjacency layer). (default: 64)
	pub graph_fold_threshold: usize,
	/// How many adjacency keys one fold pass consumes per transaction.
	/// Bounds the fold's transaction size on high-degree vertices; a scope
	/// with more keys drains over several passes. (default: 1024)
	pub graph_fold_batch: usize,
}

impl Default for IdxConfig {
	fn default() -> Self {
		Self {
			table_doc_ids_batch_size: 1000,
			hnsw_cache_size: 256 * 1024 * 1024,
			diskann_cache_size: 256 * 1024 * 1024,
			file_allowlist: Vec::new(),
			graph_fold_threshold: 64,
			graph_fold_batch: 1024,
		}
	}
}

impl cnf::Config for IdxConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("table_doc_ids_batch_size", &mut self.table_doc_ids_batch_size)
			.parse_key("hnsw_cache_size", &mut self.hnsw_cache_size)
			.parse_key("diskann_cache_size", &mut self.diskann_cache_size)
			.parse_key("graph_fold_batch", &mut self.graph_fold_batch)
			.parse_key_with("file_allowlist", &mut self.file_allowlist, |x| {
				// FIXME: We really shouldn't be doing random, faillable, IO when reading
				// configuration values. But no way to fix it without restructuring the
				// datastore entirely.
				Some(cnf::extract_allowed_paths(x, true, "file"))
			});
	}
}
