//! The index engines' configurable resource limits.
//!
//! These knobs govern the resources only index code allocates: the batch in
//! which a table's shared doc-ID space is drawn from its distributed sequence,
//! and the size of the two ANN vector caches (HNSW and DiskANN). Nothing above
//! this layer allocates against them, so this layer owns them.
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
}

impl Default for IdxConfig {
	fn default() -> Self {
		Self {
			table_doc_ids_batch_size: 1000,
			hnsw_cache_size: 256 * 1024 * 1024,
			diskann_cache_size: 256 * 1024 * 1024,
			file_allowlist: Vec::new(),
		}
	}
}

impl cnf::Config for IdxConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("table_doc_ids_batch_size", &mut self.table_doc_ids_batch_size)
			.parse_key("hnsw_cache_size", &mut self.hnsw_cache_size)
			.parse_key("diskann_cache_size", &mut self.diskann_cache_size)
			.parse_key_with("file_allowlist", &mut self.file_allowlist, |x| {
				// FIXME: We really shouldn't be doing random, faillable, IO when reading
				// configuration values. But no way to fix it without restructuring the
				// datastore entirely.
				Some(cnf::extract_allowed_paths(x, true, "file"))
			});
	}
}
