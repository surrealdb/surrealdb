//! The limits and batch sizes that bound query execution.
//!
//! These knobs cap what a single execution may hold in memory (operator
//! buffers, scan batches, sort and join build sets), how deep it may recurse
//! (computation depth, idiom recursion), and how many rows a fan-out operator
//! may emit. Each one turns an otherwise unbounded consumption of memory or
//! call stack into a query that fails with an error, so raising one widens the
//! set of accepted queries at the cost of the safety margin protecting the
//! node.
//!
//! The executor owns them because it is the lowest layer that reads each one.
//! The legacy evaluator and the `dbs` result collector read several of the same
//! limits so both engines enforce one budget; those reads are downward from
//! here and leave ownership untouched.

use surrealdb_cnf as cnf;

/// Default number of records a scan operator buffers before yielding a batch
/// downstream. Overridable per deployment via the `scan_batch_size` key.
const DEFAULT_SCAN_BATCH_SIZE: usize = 1000;

/// Default row count below which a batch is evaluated one row at a time.
///
/// This is the highest of the thresholds the fan-out sites used before they
/// shared one, not a measured optimum. Overridable per deployment via the
/// `fan_out_row_threshold` key.
const DEFAULT_FAN_OUT_ROW_THRESHOLD: usize = 4;

/// Limits and batch sizes governing query execution.
#[derive(Clone, Debug)]
pub(crate) struct ExecConfig {
	/// Specifies how deep recursive computation will go before erroring (default:
	/// 120)
	pub max_computation_depth: u32,
	/// The maximum recursive idiom path depth allowed (default: 256)
	///
	/// Fixed at that default: no configuration key writes this field, so a
	/// deployment cannot move it.
	pub idiom_recursion_limit: u32,
	/// The number of batches each operator buffers ahead of downstream demand.
	/// Set to 0 to disable operator-level pipeline buffering.
	/// (default: 2)
	pub operator_buffer_size: usize,
	/// Default batch size for scan operators that collect records before
	/// yielding downstream (table/index/record-id/graph/reference scans).
	/// Memory-constrained deployments can reduce this to lower per-pipeline
	/// in-flight memory at the cost of slightly more per-batch dispatch
	/// overhead. Per-batch unit is values, not bytes. (default: 1000)
	pub scan_batch_size: usize,
	/// The maximum size of the priority queue triggering usage of the priority
	/// queue for the result collector.
	pub max_order_limit_priority_queue_size: u32,
	/// Whether eligible `ORDER BY … LIMIT` table scans may skip record decode
	/// for rows that cannot beat the current top-K threshold (default: true)
	pub topk_threshold_pushdown_enabled: bool,
	/// Maximum number of build-side rows a GQL `MATCH` hash join (and the
	/// whole-row `Distinct` dedup that rides the same budget) may hold in memory
	/// before failing the query (default: 1,000,000). Bounds the in-memory
	/// build/seen set for GQL v2 binding-table execution; spill to disk is a
	/// future change (matching the `Aggregate` stance). Errors that trip this
	/// guard name the env knob (`SURREAL_GQL_MAX_JOIN_BUILD_ROWS`).
	pub gql_max_join_build_rows: usize,
	/// Maximum number of paths a single GQL `MATCH` `PathExpand` (variable-length
	/// / quantified edge traversal) may have live on its DFS stack plus already
	/// emitted, per source row, before failing the query (default: 1,000,000).
	/// Bounds the worst-case path explosion of a quantified pattern over a dense
	/// or cyclic graph; edge-uniqueness-within-path guarantees termination but the
	/// number of distinct paths can still be very large. Errors that trip this
	/// guard name the env knob (`SURREAL_GQL_MAX_PATH_ROWS`).
	pub gql_max_path_rows: usize,
	/// Maximum number of rows a single GQL `MATCH` fan-out operator (`HashJoin` —
	/// including the `Cross` cartesian product — and single-hop `Expand`) may
	/// emit, cumulatively across all batches, before failing the query (default:
	/// 1,000,000). Unlike `gql_max_join_build_rows` (which bounds the in-memory
	/// build/seen set), this bounds the *output* product: a cross join of a
	/// bounded build side against a streaming probe side, or a high-fan-out
	/// expand, can emit unboundedly many rows while holding only a small build
	/// set. Errors that trip this guard name the env knob
	/// (`SURREAL_GQL_MAX_OUTPUT_ROWS`).
	pub gql_max_output_rows: usize,
	/// The number of result records which will trigger on-disk sorting (default:
	/// 50,000)
	pub external_sorting_buffer_limit: usize,
	/// Used to limit allocation for builtin functions. Default: 2^20 (1 MiB),
	/// can be as large as 28 (2^28, 256 MiB)
	pub generation_allocation_limit: usize,
	/// Rows below which the expression layer evaluates a batch one row at a
	/// time instead of overlapping them (default: 4). Set it above any batch
	/// size to disable overlapping entirely.
	///
	/// Only a read-only evaluation is ever overlapped, whatever this is set to;
	/// see [`crate::exec::fan_out`].
	///
	/// What overlapping is worth depends on what the rows do. Rows that only
	/// dereference a record link gain nothing, because they share a transaction
	/// and every backend guards it, so they queue on the guard instead of
	/// overlapping their reads. Rows that each run an operator plan gain a great
	/// deal, because those plans buffer through their own tasks and so reach
	/// more than one core. `benches/fan_out.rs` measures both settings against
	/// each other per shape and per backend; #860 tracks acting on it.
	pub fan_out_row_threshold: usize,
}

impl Default for ExecConfig {
	fn default() -> Self {
		Self {
			max_computation_depth: 120,
			idiom_recursion_limit: 256,
			operator_buffer_size: 2,
			scan_batch_size: DEFAULT_SCAN_BATCH_SIZE,
			max_order_limit_priority_queue_size: 1000,
			topk_threshold_pushdown_enabled: true,
			gql_max_join_build_rows: 1_000_000,
			gql_max_path_rows: 1_000_000,
			gql_max_output_rows: 1_000_000,
			external_sorting_buffer_limit: 50_000,
			generation_allocation_limit: 2 << 20,
			fan_out_row_threshold: DEFAULT_FAN_OUT_ROW_THRESHOLD,
		}
	}
}

impl cnf::Config for ExecConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("max_computation_depth", &mut self.max_computation_depth)
			.parse_key("operator_buffer_size", &mut self.operator_buffer_size)
			.parse_key("scan_batch_size", &mut self.scan_batch_size)
			.parse_key(
				"max_order_limit_priority_queue_size",
				&mut self.max_order_limit_priority_queue_size,
			)
			.parse_key("topk_threshold_pushdown_enabled", &mut self.topk_threshold_pushdown_enabled)
			.parse_key("gql_max_join_build_rows", &mut self.gql_max_join_build_rows)
			.parse_key("gql_max_path_rows", &mut self.gql_max_path_rows)
			.parse_key("gql_max_output_rows", &mut self.gql_max_output_rows)
			.parse_key("external_sorting_buffer_limit", &mut self.external_sorting_buffer_limit)
			.parse_key("fan_out_row_threshold", &mut self.fan_out_row_threshold)
			.parse_key_with(
				"generation_allocation_limit",
				&mut self.generation_allocation_limit,
				|x| x.parse::<usize>().ok().map(|x| 2 << x.min(28)),
			);
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_cnf::{Config as _, ConfigMap};

	use super::*;

	/// The TopK threshold pushdown kill switch must default on and parse off
	/// from the config map (`SURREAL_TOPK_THRESHOLD_PUSHDOWN_ENABLED=false`).
	/// Disabling it routes planning through the same code path as "no ORDER
	/// BY opportunity" (TopKPushdownRequest::NotApplicable), which the
	/// topk_pushdown language tests cover.
	#[test]
	fn topk_threshold_pushdown_kill_switch_parses() {
		let mut config = ExecConfig::default();
		assert!(config.topk_threshold_pushdown_enabled, "feature defaults on");
		let map = ConfigMap::empty().with_key_value("topk_threshold_pushdown_enabled", "false");
		config.parse(&map);
		assert!(!config.topk_threshold_pushdown_enabled, "config map disables the feature");
	}

	/// The fan-out threshold defaults to 4 and parses from the config map, which
	/// is what lets `benches/fan_out.rs` run the same query with overlapping on
	/// and off.
	#[test]
	fn fan_out_row_threshold_parses() {
		let mut config = ExecConfig::default();
		assert_eq!(config.fan_out_row_threshold, 4);
		let map = ConfigMap::empty().with_key_value("fan_out_row_threshold", "1000000");
		config.parse(&map);
		assert_eq!(config.fan_out_row_threshold, 1_000_000);
	}

	/// The GQL v2 MATCH resource limits default to 1M and parse from the config
	/// map under the same keys `ConfigMap::from_env` derives from
	/// `SURREAL_GQL_MAX_*`, so the env vars keep working and embedded callers can
	/// set them programmatically.
	#[test]
	fn gql_match_limits_parse_from_config_map() {
		let mut config = ExecConfig::default();
		assert_eq!(config.gql_max_join_build_rows, 1_000_000);
		assert_eq!(config.gql_max_path_rows, 1_000_000);
		assert_eq!(config.gql_max_output_rows, 1_000_000);

		let map = ConfigMap::empty()
			.with_key_value("gql_max_join_build_rows", "5")
			.with_key_value("gql_max_path_rows", "7")
			.with_key_value("gql_max_output_rows", "9");
		config.parse(&map);
		assert_eq!(config.gql_max_join_build_rows, 5);
		assert_eq!(config.gql_max_path_rows, 7);
		assert_eq!(config.gql_max_output_rows, 9);
	}
}
