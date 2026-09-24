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

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::str::FromStr;

use surrealdb_cnf as cnf;

/// How many tasks one graph-construction batch may run on.
///
/// A property of the machine rather than of any index, so it is configuration
/// and never part of an index definition: the graph a batch builds does not
/// depend on it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BuildParallelism {
	/// As many tasks as the machine reports available parallelism.
	#[default]
	Auto,
	/// No spawned tasks: the batch runs on the task applying it.
	Serial,
	/// Exactly this many tasks.
	Fixed(NonZeroUsize),
}

impl BuildParallelism {
	/// The number of tasks a batch runs on; at least one.
	pub fn tasks(self) -> usize {
		match self {
			Self::Auto => std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
			Self::Serial => 1,
			Self::Fixed(n) => n.get(),
		}
	}
}

impl FromStr for BuildParallelism {
	type Err = ();

	/// Accepts `auto`, `serial`, or a task count; `0` means `auto`.
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let s = s.trim();
		if s.eq_ignore_ascii_case("auto") {
			return Ok(Self::Auto);
		}
		if s.eq_ignore_ascii_case("serial") {
			return Ok(Self::Serial);
		}
		let n: usize = s.parse().map_err(|_| ())?;
		Ok(NonZeroUsize::new(n).map_or(Self::Auto, Self::Fixed))
	}
}

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
	/// How many tasks one HNSW compaction batch builds its graph changes on:
	/// `auto`, `serial` or a task count. Batches that remove vectors always
	/// apply serially. (default: auto)
	pub hnsw_build_parallelism: BuildParallelism,
	/// The maximum total size of the DiskANN ANN cache (default: 256 MiB)
	pub diskann_cache_size: u64,
	/// Specifies a list of paths in which files can be accessed (default: empty)
	pub file_allowlist: Vec<PathBuf>,
	/// Directory holding the morphological dictionaries `TOKENIZERS
	/// SEGMENT(<language>)` reads, one subdirectory per language (`chinese`,
	/// `japanese`, `korean`). Unset means every language uses the dictionary
	/// compiled into the binary, which is present only for the `cjk-embed-*`
	/// features enabled at build time. Not subject to `file_allowlist`: the
	/// subdirectory names are this engine's own, so the files this key can
	/// make the engine open are fixed at compile time, whereas the allowlist
	/// bounds paths a statement supplies. (default: none)
	pub segment_dictionary_path: Option<PathBuf>,
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
	/// Whether folds emit numeric-encoded blocks, compressing each entry's
	/// record ids into table-level doc-IDs. Existing plain blocks migrate
	/// on their next fold; readers handle both encodings either way.
	/// (default: false)
	pub graph_numeric_ids: bool,
	/// The maximum total size of the doc-ID → record-key cache that
	/// resolves numeric adjacency entries (default: 64 MiB)
	pub graph_resolve_cache_size: u64,
	/// The maximum encoded size, in bytes, of one edge's inline property
	/// payload. A payload past the cap is written spilled — the values live
	/// only in the edge record and filtered traversals fall back to it.
	/// Read by the document pipeline when writing edges, not by an index
	/// engine; it lives here because the payload rides the adjacency
	/// values the graph engine owns. (default: 64)
	pub graph_inline_props_cap: usize,
}

impl Default for IdxConfig {
	fn default() -> Self {
		Self {
			table_doc_ids_batch_size: 1000,
			hnsw_cache_size: 256 * 1024 * 1024,
			hnsw_build_parallelism: BuildParallelism::Auto,
			diskann_cache_size: 256 * 1024 * 1024,
			file_allowlist: Vec::new(),
			segment_dictionary_path: None,
			graph_fold_threshold: 64,
			graph_fold_batch: 1024,
			graph_numeric_ids: false,
			graph_resolve_cache_size: 64 * 1024 * 1024,
			graph_inline_props_cap: 64,
		}
	}
}

impl cnf::Config for IdxConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("table_doc_ids_batch_size", &mut self.table_doc_ids_batch_size)
			.parse_key("hnsw_cache_size", &mut self.hnsw_cache_size)
			.parse_key("hnsw_build_parallelism", &mut self.hnsw_build_parallelism)
			.parse_key("diskann_cache_size", &mut self.diskann_cache_size)
			.parse_key("graph_fold_batch", &mut self.graph_fold_batch)
			.parse_key("graph_numeric_ids", &mut self.graph_numeric_ids)
			.parse_key("graph_resolve_cache_size", &mut self.graph_resolve_cache_size)
			.parse_key("graph_inline_props_cap", &mut self.graph_inline_props_cap)
			.parse_key_option("segment_dictionary_path", &mut self.segment_dictionary_path)
			.parse_key_with("file_allowlist", &mut self.file_allowlist, |x| {
				// FIXME: We really shouldn't be doing random, faillable, IO when reading
				// configuration values. But no way to fix it without restructuring the
				// datastore entirely.
				Some(cnf::extract_allowed_paths(x, true, "file"))
			});
	}
}
