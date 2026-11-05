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
//! The `dbs` result collector and the builtin functions read several of the same
//! limits so everything enforces one budget; those reads are downward from
//! here and leave ownership untouched.

use common::str::ParseBytes;
use surrealdb_cnf as cnf;

/// Default number of records a scan operator buffers before yielding a batch
/// downstream. Overridable per deployment via the `scan_batch_size` key.
const DEFAULT_SCAN_BATCH_SIZE: usize = 1000;

/// Default approximate decoded size, in bytes, a byte-sized scan batch
/// targets. Overridable per deployment via the `scan_batch_bytes` key.
pub(crate) const DEFAULT_SCAN_BATCH_BYTES: usize = 2 << 20;

/// Default row count below which a batch is evaluated one row at a time.
///
/// This is the highest of the fan-out thresholds in use. It is not a measured
/// optimum. Overridable per deployment via the
/// `fan_out_row_threshold` key.
const DEFAULT_FAN_OUT_ROW_THRESHOLD: usize = 4;

/// Default entries a pipeline's coordinator reads before it may split its
/// remaining input into morsels, and the fewest rows a batch of decoded rows
/// holds when it goes to a worker. Overridable per deployment via the
/// `morsel_split_rows` key.
const DEFAULT_MORSEL_SPLIT_ROWS: usize = 256;

/// Default most rows in a million a range's `WHERE` may pass for the range to be cut into
/// morsels. Overridable per deployment via the `morsel_split_pass_ppm` key.
const DEFAULT_MORSEL_SPLIT_PASS_PPM: u32 = 50_000;

/// Default bytes a morsel over a key range aims at. Overridable per deployment
/// via the `morsel_target_bytes` key.
const DEFAULT_MORSEL_TARGET_BYTES: usize = 1 << 20;

/// Default output bytes a morsel that is not at the front of its pipeline holds
/// before it parks. Overridable per deployment via the `morsel_window_bytes`
/// key.
const DEFAULT_MORSEL_WINDOW_BYTES: usize = 1 << 20;

/// Default places of the node's worker pool morsel workers never take, so that
/// short statements keep a core. Overridable per deployment via the
/// `morsel_oltp_reserve` key.
const DEFAULT_MORSEL_OLTP_RESERVE: usize = 1;

/// Default quanta a worker holds a place of the node's worker pool while another
/// statement waits for one. Overridable per deployment via the
/// `morsel_place_quantum` key.
const DEFAULT_MORSEL_PLACE_QUANTUM: u32 = 8;

/// Default units of work a running program does between two yields to the
/// host's scheduler. Overridable per deployment via the `vm_quantum` key.
const DEFAULT_VM_QUANTUM: u32 = 4096;

/// Default node memory limit, in bytes, for the queries' breakers when
/// neither `memory_limit` nor `SURREAL_MEMORY_THRESHOLD` sets one: 2 GiB.
const DEFAULT_MEMORY_LIMIT: usize = 2 << 30;

/// Default bytes every query lease is guaranteed beyond its spill headroom.
const DEFAULT_QUERY_MEMORY_MINIMUM: usize = 4 << 20;

/// Default size of a spilled run's parts, in bytes.
const DEFAULT_SPILL_PART_SIZE: usize = 1 << 20;

/// Default number of runs a merge reads at once.
const DEFAULT_SPILL_MERGE_FAN_IN: usize = 8;

/// Default bytes a breaker reserves from its query's lease at a time.
const DEFAULT_MEMORY_RESERVE_CHUNK: usize = 256 << 10;
const DEFAULT_MEMORY_WAIT_MS: u64 = 1000;

/// Default bytes of rows a streamed write statement holds back until its
/// commit: no bound, so a retried conflict never sends a failed attempt's rows.
/// A host with a reply or memory bound sets the `stream_stage_bytes` key.
const DEFAULT_STREAM_STAGE_BYTES: usize = usize::MAX;

/// Default number of compiled units of each kind a node keeps. Overridable
/// per deployment via the `compiled_unit_cache_size` key.
const DEFAULT_COMPILED_UNIT_CACHE_SIZE: usize = 1024;

/// Default number of top-level statements a node keeps compiled for later
/// requests. Overridable per deployment via the `statement_cache_size` key.
const DEFAULT_STATEMENT_CACHE_SIZE: usize = 1024;

/// Default number of function bodies compiled for a call's argument values a
/// node keeps. Overridable per deployment via the
/// `specialised_body_cache_size` key.
const DEFAULT_SPECIALISED_BODY_CACHE_SIZE: usize = 256;

/// Default size, in bytes of their encoding, of the largest argument values
/// a node keeps a function body compiled for. Overridable per deployment via
/// the `specialised_body_max_arg_bytes` key.
const DEFAULT_SPECIALISED_BODY_MAX_ARG_BYTES: usize = 4096;

/// Default number of distinct argument values one function's body is
/// compiled for before its calls run a body that reads them when it runs.
/// Overridable per deployment via the `specialised_body_values` key.
const DEFAULT_SPECIALISED_BODY_VALUES: usize = 32;

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
	/// Default batch size for scan operators that collect records before yielding
	/// downstream (table, index, record-id, graph and reference scans). Lowering it
	/// reduces per-pipeline in-flight memory on memory-constrained deployments, at the
	/// cost of more per-batch dispatch overhead. The unit is values, not bytes.
	/// Permission-checked and `FullEdge` graph scans pipeline their record resolves,
	/// holding up to two id batches plus one resolved value batch in flight, so their
	/// worst-case per-pipeline memory is about twice this knob plus one materialized
	/// batch (dominant in `FullEdge` mode). The knob still scales that bound linearly.
	/// (default: 1000)
	pub scan_batch_size: usize,
	/// Approximate decoded size, in bytes, of a batch from a scan that sizes
	/// its batches by bytes as well as rows (bitmap resolves, index scans). Every operator
	/// above a scan buffers whole batches, and the allocator keeps each worker
	/// thread's freed batch memory, so a query's resident memory tracks this
	/// budget times the threads that handle its batches. Lower it to bound
	/// memory on small nodes; raise it to trade memory for fewer, larger
	/// batches. Accepts a byte count or a size such as `512KiB`; zero and
	/// unparseable values are ignored. (default: 2 MiB)
	pub scan_batch_bytes: usize,
	/// The largest `ORDER BY ... LIMIT` size served by a bounded priority queue in
	/// the result collector (default: 1000).
	pub max_order_limit_priority_queue_size: u32,
	/// Whether eligible `ORDER BY … LIMIT` table scans may skip record decode
	/// for rows that cannot beat the current top-K threshold (default: true)
	pub topk_threshold_pushdown_enabled: bool,
	/// Maximum number of build-side rows a GQL `MATCH` hash join (and the
	/// whole-row `Distinct` dedup that shares the budget) may hold in memory
	/// before failing the query (default: 1,000,000). Bounds the in-memory
	/// build/seen set for GQL v2 binding-table execution. Rows are not spilled to
	/// disk; a query over the limit fails. Errors that trip this
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
	/// each other per shape and per backend.
	pub fan_out_row_threshold: usize,
	/// The entries a pipeline's coordinator reads before it may split what is
	/// left of its input into morsels, and the fewest rows a batch of decoded
	/// rows holds when it is handed to a worker (default: 256). Below it
	/// everything runs on the statement's task: a worker costs a task spawn,
	/// which a small morsel does not repay. `usize::MAX` never splits.
	pub morsel_split_rows: usize,
	/// Whether a coordinator cuts a key range into morsels only where the
	/// transaction gives workers a read view to read them through (default:
	/// true). Where it gives none, morsels only run one after another on the
	/// statement's task, so a cut costs and gains nothing; `false` cuts the
	/// range anyway, which runs the merge under the same load on every backend.
	pub morsel_needs_view: bool,
	/// The most rows in a million of a range that its pipeline's filters may pass for the range
	/// to be cut into morsels (default: 50 000, one in twenty). A scan most of whose rows pass is
	/// bound by reading them, which morsels do not speed up, and starting them costs latency; a
	/// selective scan is bound by judging rows, which workers share. The share is measured from
	/// the entries the coordinator has read, or known before the scan opens; a scan whose share
	/// is not known is not cut. `1000000` cuts whatever passes.
	pub morsel_split_pass_ppm: u32,
	/// The bytes a morsel over a key range aims at (default: 1 MiB). `1` makes
	/// every entry a morsel of its own.
	pub morsel_target_bytes: usize,
	/// The most places of the node's worker pool one statement's morsels may
	/// hold at once (default: unbounded). `0` runs every morsel on the
	/// statement's own task.
	pub morsel_places: usize,
	/// The width of the loopback host places a datastore installs when the host supplies none
	/// (default: 0, none): every morsel handed to them, its unit and its frames, is encoded and
	/// run by a second engine of the process, as a host's leaf runs it. A conformance setting for
	/// the host-place contract.
	pub morsel_loopback_places: usize,
	/// The places of the node's worker pool that morsel workers never take
	/// (default: 1), so that short statements keep a core.
	pub morsel_oltp_reserve: usize,
	/// The output bytes a morsel that is not at the front of its pipeline holds
	/// before it parks and hands its place back (default: 1 MiB).
	pub morsel_window_bytes: usize,
	/// Whether every running morsel donates its remainder at each chunk boundary where
	/// its range can be split, instead of only when a worker is idle (default: false).
	/// Setting it forces the most donation a run can see, so the merge's ordering and
	/// failure rules are exercised under that load.
	pub morsel_steal_always: bool,
	/// The quanta (chunk boundaries of the morsel a worker runs) a worker holds a place of the
	/// node's worker pool while another statement waits for one, before it donates what is left of
	/// its morsel and returns the place (default: 8).
	pub morsel_place_quantum: u32,
	/// Whether `EXPLAIN ANALYZE` renders the morsel counters of the statement (default: false).
	/// Off, its text is the same at every width.
	pub morsel_explain_analyze: bool,
	/// How many compiled units of each kind — function bodies, closure
	/// bodies, catalog units — a node keeps for later plans to link, least
	/// recently used evicted first (default: 1024); `0` keeps none and
	/// compiles each with the plan that needs it (`crate::exec::vm::units`).
	pub compiled_unit_cache_size: usize,
	/// How many top-level statements a node keeps compiled for later requests,
	/// least recently used evicted first (default: 1024): a request's
	/// read-only statement that reads its variables only as the key of a
	/// record of a constant table is served the compilation of the same
	/// statement text under the same namespace, database, principal class and
	/// capabilities, which reads those variables when it runs
	/// (`crate::exec::vm::serve::kept_request_statement`); `0` keeps none and
	/// compiles each statement with the request that carries it.
	pub statement_cache_size: usize,
	/// The units of work a running program does between two yields to the
	/// host's scheduler (default: 4096). A unit is a row, a loop iteration
	/// or a host step, and an instruction that walks an array spends one per
	/// element (`crate::exec::vm::fuel`). The deadline and the cancellation
	/// flag are checked once per quantum, so it also bounds how long a
	/// statement runs past either. Zero is read as one.
	pub vm_quantum: u32,
	/// How many function bodies compiled for a call's argument values a node
	/// keeps, least recently used evicted first (default: 256): a body that
	/// reads an argument where only a value answers is compiled for the values
	/// each call brings (`crate::exec::vm::call_unit`); `0` keeps none.
	pub specialised_body_cache_size: usize,
	/// The size, in bytes of their encoding, of the largest argument values a
	/// node keeps a function body compiled for (default: 4096). A body
	/// compiled for larger values holds them, so it is compiled for its call
	/// alone and not kept: the cache holds at most
	/// `specialised_body_cache_size` bodies, each folding at most this much.
	pub specialised_body_max_arg_bytes: usize,
	/// How many distinct argument values one function is compiled for. Past this count,
	/// its calls run a body that reads the arguments at run time (default: 32).
	pub specialised_body_values: usize,
	/// The bytes the node's queries may hold in their breakers — sorts,
	/// groups, distincts, join builds — before they spill
	/// (`crate::exec::memory`). `0`, the default, takes half of
	/// `SURREAL_MEMORY_THRESHOLD` where that is set, and 2 GiB otherwise. A
	/// host that supplies its own governor sizes it itself.
	pub memory_limit: usize,
	/// The bytes every query that holds a breaker is guaranteed, beyond what
	/// spilling takes (default: 4 MiB). No lowered grant takes them back.
	pub query_memory_minimum: usize,
	/// The size, in bytes, of the parts a spilled run is written and read in
	/// (default: 1 MiB). A query holds one to write a run and one per run a
	/// merge reads, as part of its guaranteed minimum.
	pub spill_part_size: usize,
	/// The runs a merge reads at once (default: 8, at least 2); a merge of
	/// more takes several passes.
	pub spill_merge_fan_in: usize,
	/// The bytes a breaker reserves from its query's lease at a time
	/// (default: 256 KiB).
	pub memory_reserve_chunk: usize,
	/// The longest, in milliseconds, a query waits in all for other queries to
	/// give memory back before it proceeds past the node limit (default: 1000).
	/// `0` never waits.
	pub memory_wait: u64,
	/// The bytes a single query may hold that it cannot spill — values it has
	/// gathered or built, and its transaction's buffers — before it fails with
	/// the memory threshold error (default: `0`, the node's `memory_limit`). It
	/// bounds the query's own state alone: no other query's use counts toward
	/// it.
	pub query_memory_max: usize,
	/// The bytes of rows, as estimated, that a streamed write statement
	/// running in a transaction of its own holds back from its consumer until
	/// its commit. Held rows let a statement that loses a
	/// retryable commit conflict run again without the consumer seeing the
	/// failed attempt's rows. Past the bound the held rows are sent and the
	/// rest stream as they are produced, so the consumer can stop the
	/// statement before it commits; a statement whose rows have reached its
	/// consumer is not run again. A host that stops a statement once its reply
	/// passes a bound sets this at or below that bound. Accepts a byte count or
	/// a size such as `16KiB`; `0` holds nothing back. By default nothing bounds
	/// it: every row of the statement is held back until its commit.
	pub stream_stage_bytes: usize,
}

impl ExecConfig {
	/// The node memory limit `memory_limit` names, with its default resolved.
	pub(crate) fn memory_limit(&self) -> usize {
		match self.memory_limit {
			0 => match *cnf::MEMORY_THRESHOLD {
				0 => DEFAULT_MEMORY_LIMIT,
				threshold => threshold / 2,
			},
			limit => limit,
		}
	}

	/// The per-query ceiling on bytes a query cannot spill that
	/// `query_memory_max` names, with its default resolved.
	pub(crate) fn query_memory_max(&self) -> usize {
		match self.query_memory_max {
			0 => self.memory_limit(),
			max => max,
		}
	}
}

impl Default for ExecConfig {
	fn default() -> Self {
		Self {
			max_computation_depth: 120,
			idiom_recursion_limit: 256,
			operator_buffer_size: 2,
			scan_batch_size: DEFAULT_SCAN_BATCH_SIZE,
			scan_batch_bytes: DEFAULT_SCAN_BATCH_BYTES,
			max_order_limit_priority_queue_size: 1000,
			topk_threshold_pushdown_enabled: true,
			gql_max_join_build_rows: 1_000_000,
			gql_max_path_rows: 1_000_000,
			gql_max_output_rows: 1_000_000,
			external_sorting_buffer_limit: 50_000,
			generation_allocation_limit: 2 << 20,
			fan_out_row_threshold: DEFAULT_FAN_OUT_ROW_THRESHOLD,
			morsel_split_rows: DEFAULT_MORSEL_SPLIT_ROWS,
			morsel_needs_view: true,
			morsel_split_pass_ppm: DEFAULT_MORSEL_SPLIT_PASS_PPM,
			morsel_target_bytes: DEFAULT_MORSEL_TARGET_BYTES,
			morsel_places: usize::MAX,
			morsel_loopback_places: 0,
			morsel_oltp_reserve: DEFAULT_MORSEL_OLTP_RESERVE,
			morsel_window_bytes: DEFAULT_MORSEL_WINDOW_BYTES,
			morsel_steal_always: false,
			morsel_place_quantum: DEFAULT_MORSEL_PLACE_QUANTUM,
			morsel_explain_analyze: false,
			compiled_unit_cache_size: DEFAULT_COMPILED_UNIT_CACHE_SIZE,
			vm_quantum: DEFAULT_VM_QUANTUM,
			statement_cache_size: DEFAULT_STATEMENT_CACHE_SIZE,
			specialised_body_cache_size: DEFAULT_SPECIALISED_BODY_CACHE_SIZE,
			specialised_body_max_arg_bytes: DEFAULT_SPECIALISED_BODY_MAX_ARG_BYTES,
			specialised_body_values: DEFAULT_SPECIALISED_BODY_VALUES,
			memory_limit: 0,
			query_memory_minimum: DEFAULT_QUERY_MEMORY_MINIMUM,
			spill_part_size: DEFAULT_SPILL_PART_SIZE,
			spill_merge_fan_in: DEFAULT_SPILL_MERGE_FAN_IN,
			memory_reserve_chunk: DEFAULT_MEMORY_RESERVE_CHUNK,
			memory_wait: DEFAULT_MEMORY_WAIT_MS,
			query_memory_max: 0,
			stream_stage_bytes: DEFAULT_STREAM_STAGE_BYTES,
		}
	}
}

impl cnf::Config for ExecConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("max_computation_depth", &mut self.max_computation_depth)
			.parse_key("operator_buffer_size", &mut self.operator_buffer_size)
			.parse_key("scan_batch_size", &mut self.scan_batch_size)
			.parse_key_with("scan_batch_bytes", &mut self.scan_batch_bytes, |x| {
				x.parse_bytes::<usize>().ok().filter(|&n| n > 0)
			})
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
			.parse_key("morsel_split_rows", &mut self.morsel_split_rows)
			.parse_key_bool("morsel_needs_view", &mut self.morsel_needs_view)
			.parse_key("morsel_split_pass_ppm", &mut self.morsel_split_pass_ppm)
			.parse_key_with("morsel_target_bytes", &mut self.morsel_target_bytes, |x| {
				x.parse_bytes::<usize>().ok().filter(|&n| n > 0)
			})
			.parse_key("morsel_places", &mut self.morsel_places)
			.parse_key("morsel_loopback_places", &mut self.morsel_loopback_places)
			.parse_key("morsel_oltp_reserve", &mut self.morsel_oltp_reserve)
			.parse_key_with("morsel_window_bytes", &mut self.morsel_window_bytes, |x| {
				x.parse_bytes::<usize>().ok().filter(|&n| n > 0)
			})
			.parse_key_bool("morsel_steal_always", &mut self.morsel_steal_always)
			.parse_key("morsel_place_quantum", &mut self.morsel_place_quantum)
			.parse_key_bool("morsel_explain_analyze", &mut self.morsel_explain_analyze)
			.parse_key("compiled_unit_cache_size", &mut self.compiled_unit_cache_size)
			.parse_key("vm_quantum", &mut self.vm_quantum)
			.parse_key("statement_cache_size", &mut self.statement_cache_size)
			.parse_key("specialised_body_cache_size", &mut self.specialised_body_cache_size)
			.parse_key("specialised_body_max_arg_bytes", &mut self.specialised_body_max_arg_bytes)
			.parse_key("specialised_body_values", &mut self.specialised_body_values)
			.parse_key_with("memory_limit", &mut self.memory_limit, |x| {
				x.parse_bytes::<usize>().ok()
			})
			.parse_key_with("query_memory_minimum", &mut self.query_memory_minimum, |x| {
				x.parse_bytes::<usize>().ok()
			})
			.parse_key_with("spill_part_size", &mut self.spill_part_size, |x| {
				x.parse_bytes::<usize>().ok().filter(|&n| n > 0)
			})
			.parse_key("spill_merge_fan_in", &mut self.spill_merge_fan_in)
			.parse_key_with("memory_reserve_chunk", &mut self.memory_reserve_chunk, |x| {
				x.parse_bytes::<usize>().ok().filter(|&n| n > 0)
			})
			.parse_key("memory_wait", &mut self.memory_wait)
			.parse_key_with("query_memory_max", &mut self.query_memory_max, |x| {
				x.parse_bytes::<usize>().ok()
			})
			.parse_key_with("stream_stage_bytes", &mut self.stream_stage_bytes, |x| {
				x.parse_bytes::<usize>().ok()
			})
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

	/// The bound on a streamed write's held-back rows defaults to none,
	/// parses a byte count or a size with a suffix, and accepts zero.
	#[test]
	fn stream_stage_bytes_parses() {
		let mut config = ExecConfig::default();
		assert_eq!(config.stream_stage_bytes, usize::MAX, "no bound by default");
		config.parse(&ConfigMap::empty().with_key_value("stream_stage_bytes", "16KiB"));
		assert_eq!(config.stream_stage_bytes, 16 << 10);
		config.parse(&ConfigMap::empty().with_key_value("stream_stage_bytes", "0"));
		assert_eq!(config.stream_stage_bytes, 0, "zero holds nothing back");
	}

	/// The scan batch byte budget defaults to 2 MiB, parses a byte count or a
	/// size with a suffix, and ignores zero.
	#[test]
	fn scan_batch_bytes_parses() {
		let mut config = ExecConfig::default();
		assert_eq!(config.scan_batch_bytes, 2 << 20);
		config.parse(&ConfigMap::empty().with_key_value("scan_batch_bytes", "512KiB"));
		assert_eq!(config.scan_batch_bytes, 512 << 10);
		config.parse(&ConfigMap::empty().with_key_value("scan_batch_bytes", "0"));
		assert_eq!(config.scan_batch_bytes, 512 << 10, "zero is ignored");
	}

	/// The per-query ceiling defaults to the node's memory limit, and parses a
	/// byte count or a size with a suffix.
	#[test]
	fn query_memory_max_defaults_to_the_memory_limit() {
		let mut config = ExecConfig::default();
		assert_eq!(config.query_memory_max(), config.memory_limit());
		config.parse(&ConfigMap::empty().with_key_value("memory_limit", "8MiB"));
		assert_eq!(config.query_memory_max(), 8 << 20);
		config.parse(&ConfigMap::empty().with_key_value("query_memory_max", "1MiB"));
		assert_eq!(config.query_memory_max(), 1 << 20);
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
