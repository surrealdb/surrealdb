//! `HashJoin` — equi-join (and cartesian product) of two binding-row streams.
//!
//! `HashJoin` combines the binding tables produced by two MATCH sub-plans —
//! either two comma-separated patterns inside one `MATCH`, or two sequential
//! `MATCH` clauses — that share zero or more *node* variables (the planner makes
//! repeated node variables join keys, see `doc/gql/V2_DESIGN.md` §6). Each
//! input row is a `Value::Object` keyed by binding name (the binding-row
//! convention, §3); the output row is the union of a build row and a probe row.
//!
//! ## Join key
//!
//! The join is keyed on `<binding>.id` for each shared binding name in [`keys`],
//! in order. A binding row holds a full record object, so `<binding>.id` is its
//! `id` field (a `Value::RecordId`); a bare `Value::RecordId` slot is tolerated
//! defensively. The key is the `Vec<Value>` of those id values, one per key
//! binding. **No fetch happens here** — keys are read straight from the
//! already-bound objects (§5). Because the shared keys are equal by construction
//! on every emitted match, the merged row keeps the probe side's copy of each
//! shared binding (they are identical, so the choice is immaterial).
//!
//! [`keys`]: HashJoin::keys
//!
//! ## NULL / NONE keys (3VL, §0 "Joins & null")
//!
//! A `Value::Null` or `Value::None` in *any* key slot — or a missing /
//! non-extractable key binding — means the row has no join key (SQL 3VL: null
//! never equals anything, so it can never equi-join):
//!
//! - **Build side**: a null-keyed row is excluded from the build table entirely.
//! - **Probe side, [`Inner`]**: a null-keyed row is dropped (it can match nothing).
//! - **Probe side, [`Left`]**: a null-keyed row passes through null-filled (its [`null_template`]
//!   bindings set to `Value::Null`), exactly like a probe row whose key found no build match.
//!
//! [`Inner`]: JoinType::Inner
//! [`Left`]: JoinType::Left
//! [`null_template`]: HashJoin::null_template
//!
//! ## Join types
//!
//! - [`Inner`](JoinType::Inner): emit the union of every probe row with each build row sharing its
//!   key.
//! - [`Left`](JoinType::Left): like `Inner`, but a probe row with no matching build row (including
//!   a null-keyed probe row) is still emitted once, with the build-introduced bindings
//!   ([`null_template`]) set to `Value::Null`. PR-C wires this in for `OPTIONAL MATCH`; the
//!   operator supports it now.
//! - [`Cross`](JoinType::Cross): `keys` is empty; emit the full cartesian product of build × probe
//!   rows (the no-shared-variable case).
//!
//! ## Residual (`ON`) predicate
//!
//! An optional [`residual`](HashJoin::residual) predicate (the SQL `ON`
//! condition) is evaluated against the merged (build ∪ probe) row and
//! participates in the match decision: a merged row that fails it is not a
//! match. For [`Left`] this is load-bearing — a probe row with no
//! residual-passing build row is null-filled, **not** dropped — so a correlated
//! `OPTIONAL MATCH` predicate spanning the optional body and the accumulator can
//! gate the left-outer match instead of becoming a post-join filter (which would
//! wrongly drop the null-filled rows). Mirrors `NestedLoopJoin.cond` in
//! surrealdb/surrealdb#7024.
//!
//! ## Build budget & output bound
//!
//! The build side is materialised fully in memory (the probe side streams). The
//! number of build rows is bounded by `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` (the
//! shared GQL in-memory-build budget, default 1M); exceeding it fails the query
//! with an error that names the knob. Spill to disk is a future change, matching
//! the `Aggregate` / `Distinct` stance. Separately, the cumulative number of
//! rows *emitted* (a `Cross` product, or a high-fan-out equi-join, can emit far
//! more rows than either side holds) is bounded by `SURREAL_GQL_MAX_OUTPUT_ROWS`;
//! the probe loop also polls cancellation between batches so a runaway join is
//! interruptible.
//!
//! ## Read-after-write ordering (interleaved GQL mutations)
//!
//! When one side carries a GQL write stage upstream (a `MATCH … SET/DELETE/INSERT
//! … MATCH/OPTIONAL …` program), the *other* side must read the post-write state,
//! and the writing side must drain before the reading side opens its scan cursors
//! (the read-only buffering opens them eagerly at construction time). The normal
//! build-then-probe order already lands a mutating BUILD's writes before the probe
//! reads (so the probe is merely constructed lazily, after the build drains); a
//! mutating PROBE (the `OPTIONAL` left-join shape) is instead PRE-DRAINED — which,
//! since the mutation operators are pipeline breakers, executes its writes — and
//! its rows buffered (bounded by the same build budget) before the build is
//! constructed and the buffered rows replayed. See `execute`.

use std::collections::BTreeMap;
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use futures::StreamExt;

use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream,
	monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::{Object, Value};

/// The kind of join `HashJoin` performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
	/// Emit only probe rows that match at least one build row.
	Inner,
	/// Emit every probe row; unmatched probe rows are null-filled with the
	/// build-introduced bindings (`null_template`). Used by `OPTIONAL MATCH`.
	Left,
	/// Cartesian product (`keys` is empty): every build row × every probe row.
	Cross,
}

impl JoinType {
	/// EXPLAIN rendering of the join type.
	fn label(self) -> &'static str {
		match self {
			JoinType::Inner => "Inner",
			JoinType::Left => "Left",
			JoinType::Cross => "Cross",
		}
	}
}

/// Equi-join / cartesian product of two binding-row streams.
///
/// See the module docs for the key extraction, the null-key rules per join
/// type, and the build budget.
#[derive(Debug, Clone)]
pub struct HashJoin {
	/// Child producing the build side; materialised fully in memory.
	pub(crate) build: Arc<dyn ExecOperator>,
	/// Child producing the probe side; streamed.
	pub(crate) probe: Arc<dyn ExecOperator>,
	/// Shared binding names to equi-join on (`<binding>.id` per side). Empty
	/// for a `Cross` join.
	pub(crate) keys: Vec<String>,
	/// The kind of join.
	pub(crate) join_type: JoinType,
	/// Build-introduced binding names, set to `Value::Null` on a `Left` miss.
	/// Empty for `Inner` / `Cross`.
	pub(crate) null_template: Vec<String>,
	/// Residual join predicate (SQL `ON`), evaluated against the merged
	/// (build ∪ probe) row. A merged row that fails it is not a match. For
	/// `Left`, a probe row with no residual-passing build row is null-filled
	/// (the correlated-`OPTIONAL` match-vs-null-fill decision), NOT dropped.
	/// `None` ⇒ a pure equi-join (the existing behaviour).
	pub(crate) residual: Option<Arc<dyn PhysicalExpr>>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl HashJoin {
	/// Create a new `HashJoin` with fresh metrics.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		build: Arc<dyn ExecOperator>,
		probe: Arc<dyn ExecOperator>,
		keys: Vec<String>,
		join_type: JoinType,
		null_template: Vec<String>,
		residual: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			build,
			probe,
			keys,
			join_type,
			null_template,
			residual,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

/// One side of a `HashJoin` (build or probe): the child operator plus its
/// pre-read buffering inputs. `execute` constructs each side's stream through
/// this so it can choose to build a side eagerly (up front, for read-only
/// pipeline parallelism) or lazily inside the stream (after the *other*,
/// mutating side has drained — see the read-after-write ordering notes in
/// `execute`) without juggling parallel `op`/`access`/`card` locals.
struct JoinSide {
	op: Arc<dyn ExecOperator>,
	access: AccessMode,
	card: CardinalityHint,
}

impl JoinSide {
	fn new(op: &Arc<dyn ExecOperator>) -> Self {
		Self {
			op: Arc::clone(op),
			access: op.access_mode(),
			card: op.cardinality_hint(),
		}
	}

	/// Whether this side carries a GQL write stage upstream.
	fn mutates(&self) -> bool {
		self.access.is_read_write()
	}

	/// Construct and buffer this side's stream (lazy until first polled).
	fn stream(&self, ctx: &ExecutionContext, buffer_size: usize) -> FlowResult<ValueBatchStream> {
		Ok(buffer_stream(self.op.execute(ctx)?, self.access, self.card, buffer_size))
	}
}

impl ExecOperator for HashJoin {
	fn name(&self) -> &'static str {
		"HashJoin"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("type".to_string(), self.join_type.label().to_string())];
		if !self.keys.is_empty() {
			attrs.push(("keys".to_string(), self.keys.join(", ")));
		}
		if !self.null_template.is_empty() {
			attrs.push(("null_template".to_string(), self.null_template.join(", ")));
		}
		if let Some(residual) = self.residual.as_ref() {
			attrs.push(("residual".to_string(), residual.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		let mut level = ContextLevel::Database
			.max(self.build.required_context())
			.max(self.probe.required_context());
		if let Some(residual) = self.residual.as_ref() {
			level = level.max(residual.required_context());
		}
		level
	}

	fn access_mode(&self) -> AccessMode {
		let mut mode = self.build.access_mode().combine(self.probe.access_mode());
		if let Some(residual) = self.residual.as_ref() {
			mode = mode.combine(residual.access_mode());
		}
		mode
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		// A join fans out to an unknown number of rows.
		CardinalityHint::Unbounded
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.build, &self.probe]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		match self.residual.as_ref() {
			Some(residual) => vec![("residual", residual)],
			None => vec![],
		}
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// Read-after-write ordering. The join always drains the build side fully
		// (build phase) before streaming the probe (probe phase). When one side
		// mutates — a GQL write stage upstream of this join
		// (`MATCH … SET/DELETE/INSERT … MATCH/OPTIONAL …`) — the OTHER side must
		// observe those writes, so the WRITING side must drain before the READING
		// side is constructed: the read-only buffering strategy opens scan cursors
		// *eagerly at construction time* (for pipeline parallelism), and that eager
		// read is buried throughout the subtree (every read-only operator buffers
		// its child), so it cannot be defeated by buffering choices at this level
		// alone. The mutation operators are pipeline breakers, so draining the
		// mutating side executes its writes. Three cases:
		//
		//  - build mutates, probe reads (mandatory `MATCH` after a write — `fold_mandatory` puts
		//    the mutating accumulator on the build side): the natural build-then-probe order
		//    already lands the writes first; just DEFER the probe's construction so its cursors
		//    open post-write.
		//  - probe mutates, build reads (an `OPTIONAL` block after a write — `fold_optional`'s
		//    left-join path puts the mutating accumulator on the probe side and the read-only block
		//    on the build side): the build would otherwise drain (read) before the probe's write
		//    runs. So PRE-DRAIN the probe (executing its writes), THEN construct the build
		//    (post-write), then replay the buffered probe rows in the probe phase.
		//  - neither mutates: construct both eagerly (one shared snapshot, so overlapping the reads
		//    is safe and faster).
		let buffer_size = ctx.root().ctx.config.exec.operator_buffer_size;
		let build = JoinSide::new(&self.build);
		let probe = JoinSide::new(&self.probe);
		let probe_first = probe.mutates() && !build.mutates();

		// Eagerly construct a side only when doing so is safe — i.e. it neither
		// reads pre-write data nor is the pre-drained writer. The build is eager
		// unless it must read post-write state (`probe_first`); the probe is eager
		// unless it must read post-write state (`build.mutates()`) or it is the
		// pre-drained writer (`probe_first`). Anything left `None` is constructed
		// lazily inside the stream, after the writing side has drained.
		let eager_build = if probe_first {
			None
		} else {
			Some(build.stream(ctx, buffer_size)?)
		};
		let eager_probe = if build.mutates() || probe_first {
			None
		} else {
			Some(probe.stream(ctx, buffer_size)?)
		};

		let keys = self.keys.clone();
		let join_type = self.join_type;
		let null_template = self.null_template.clone();
		let residual = self.residual.clone();
		let max_rows = ctx.root().ctx.config.exec.gql_max_join_build_rows;
		let max_output_rows = ctx.root().ctx.config.exec.gql_max_output_rows;
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// ---- Pre-drain phase (`probe_first` only): drain the mutating probe so
			// its writes land before the reading build side is constructed. The
			// probe is a pipeline breaker, so draining it executes all of its
			// writes; its rows are buffered and replayed in the probe phase below.
			// (Re-executing the probe instead would re-run the mutation, so the rows
			// must be materialised from this single drain — bounded by the same
			// build-row budget.)
			let prebuffered_probe: Option<Vec<ValueBatch>> = if probe_first {
				let mut probe_stream = probe.stream(&ctx, buffer_size)?;
				let mut batches: Vec<ValueBatch> = Vec::new();
				let mut total: usize = 0;
				while let Some(batch_result) = probe_stream.next().await {
					crate::exec::operators::check_cancelled(&ctx)?;
					let batch = batch_result?;
					total += batch.len();
					if total > max_rows {
						Err(ControlFlow::Err(anyhow::anyhow!(
							crate::exec::Error::InvalidStatement(format!(
								"GQL MATCH join exceeded the maximum of {max_rows} buffered \
								 probe-side rows (configurable via SURREAL_GQL_MAX_JOIN_BUILD_ROWS)"
							),)
						)))?;
					}
					batches.push(batch);
				}
				Some(batches)
			} else {
				None
			};

			// ---- Build phase: drain the build side fully into the build table.
			// Constructed now (after the probe pre-drain when `probe_first`) so a
			// post-write build reads the live transaction state.
			let mut build_stream = match eager_build {
				Some(stream) => stream,
				None => build.stream(&ctx, buffer_size)?,
			};
			let mut table = BuildTable::new();
			while let Some(batch_result) = build_stream.next().await {
				// Draining the whole build side can run long without yielding;
				// poll cancellation so a client disconnect / timeout interrupts.
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				for row in batch.into_values() {
					if let Some(key) = join_key(&row, &keys) {
						table.insert(key, row, max_rows)?
					}
				}
			}

			// ---- Probe phase: stream the probe side, emitting matches.
			// Either replay the pre-drained (post-write) probe rows, or construct
			// the probe now — deferred past the build phase when `build_mutates` so
			// its scans read the live, post-write transaction state.
			let mut probe_stream: ValueBatchStream = match prebuffered_probe {
				Some(batches) => Box::pin(futures::stream::iter(batches.into_iter().map(Ok))),
				None => match eager_probe {
					Some(stream) => stream,
					None => probe.stream(&ctx, buffer_size)?,
				},
			};
			// `base_eval` evaluates the residual (ON) predicate against each merged
			// row; `emitted` bounds the cumulative output (a Cross product or a
			// high-fan-out equi-join can emit far more rows than either side holds).
			let base_eval = EvalContext::from_exec_ctx(&ctx);
			let mut emitted: usize = 0;
			while let Some(batch_result) = probe_stream.next().await {
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				let mut out: Vec<Value> = Vec::new();
				for row in batch.into_values() {
					match join_type {
						JoinType::Cross => {
							// Cartesian product: pair every probe row with every
							// build row that passes the residual.
							for build_row in table.all_rows() {
								let merged = merge_rows(build_row, &row);
								if residual_passes(residual.as_deref(), &base_eval, &merged).await?
								{
									emit(&mut out, &mut emitted, max_output_rows, merged)?;
								}
							}
						}
						JoinType::Inner => {
							// A null-keyed probe row matches nothing and is dropped;
							// otherwise emit the union with each residual-passing
							// build row sharing the key.
							if let Some(key) = join_key(&row, &keys)
								&& let Some(matches) = table.get(&key)
							{
								for build_row in matches {
									let merged = merge_rows(build_row, &row);
									if residual_passes(residual.as_deref(), &base_eval, &merged)
										.await?
									{
										emit(&mut out, &mut emitted, max_output_rows, merged)?;
									}
								}
							}
						}
						JoinType::Left => {
							// A build row counts as a match only if the merged row
							// passes the residual; a probe row with no passing build
							// row (including a null-keyed one) is null-filled — NOT
							// dropped — which is the correlated-OPTIONAL match-vs-null
							// -fill decision (a post-join Filter would drop it).
							let mut matched = false;
							if let Some(key) = join_key(&row, &keys)
								&& let Some(matches) = table.get(&key)
							{
								for build_row in matches {
									let merged = merge_rows(build_row, &row);
									if residual_passes(residual.as_deref(), &base_eval, &merged)
										.await?
									{
										emit(&mut out, &mut emitted, max_output_rows, merged)?;
										matched = true;
									}
								}
							}
							if !matched {
								emit(
									&mut out,
									&mut emitted,
									max_output_rows,
									null_filled(&row, &null_template),
								)?;
							}
						}
					}
				}
				if !out.is_empty() {
					yielder.emit(ValueBatch::new(out)).await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "HashJoin", &self.metrics))
	}
}

/// A join key: the ordered `<binding>.id` values for the shared bindings.
type JoinKey = Vec<Value>;

/// Extract the join key for `row`: the `id` of each binding named in `keys`.
///
/// Returns `None` if any key slot is `Value::Null` / `Value::None`, missing, or
/// not a record (i.e. has no usable id) — such a row never equi-joins (3VL).
/// For a `Cross` join `keys` is empty; the planner uses [`HashJoin::all_rows`]
/// directly and never calls this with an empty `keys`, but an empty `keys`
/// here would yield `Some(vec![])` (every row shares the one empty key), which
/// is also correct.
fn join_key(row: &Value, keys: &[String]) -> Option<JoinKey> {
	let Value::Object(obj) = row else {
		return None;
	};
	let mut key = Vec::with_capacity(keys.len());
	for name in keys {
		let id = binding_id(obj.get(name))?;
		key.push(id);
	}
	Some(key)
}

/// The id `Value` of a binding slot, or `None` when it cannot equi-join.
///
/// - a full record object ⇒ its `id` field (normally a `Value::RecordId`);
/// - a bare `Value::RecordId` ⇒ itself (defensive: bindings normally hold full objects);
/// - `Value::Null` / `Value::None`, a missing binding, or any other value ⇒ `None` (no join key —
///   excluded per the 3VL rules).
fn binding_id(slot: Option<&Value>) -> Option<Value> {
	match slot {
		Some(Value::Object(node)) => match node.get("id") {
			Some(Value::Null) | Some(Value::None) | None => None,
			Some(id) => Some(id.clone()),
		},
		Some(Value::RecordId(rid)) => Some(Value::RecordId(rid.clone())),
		// Null / None / missing / any other value ⇒ no join key.
		_ => None,
	}
}

/// Merge a build row and a probe row into a single binding row.
///
/// The probe row's entries take precedence on the shared keys (which are equal
/// by construction, so the choice is immaterial); build-only bindings are
/// carried over. Both rows are objects in practice; a non-object row defaults
/// to empty so the merge never panics.
fn merge_rows(build_row: &Value, probe_row: &Value) -> Value {
	let mut merged = match build_row {
		Value::Object(o) => o.clone(),
		_ => Object::default(),
	};
	if let Value::Object(probe) = probe_row {
		for (k, v) in probe.iter() {
			merged.insert(k.clone(), v.clone());
		}
	}
	Value::Object(merged)
}

/// Build a null-filled output for a `Left` miss: the probe row plus every
/// `null_template` binding set to `Value::Null`.
fn null_filled(probe_row: &Value, null_template: &[String]) -> Value {
	let mut row = match probe_row {
		Value::Object(o) => o.clone(),
		_ => Object::default(),
	};
	for name in null_template {
		row.insert(name.clone(), Value::Null);
	}
	Value::Object(row)
}

/// Evaluate the residual (`ON`) predicate against a merged row. `None` ⇒ always
/// passes (a pure equi-join). Mirrors `Expand`'s per-candidate predicate eval
/// (`graph/expand.rs`) and `Filter`.
async fn residual_passes(
	residual: Option<&dyn PhysicalExpr>,
	base_eval: &EvalContext<'_>,
	merged: &Value,
) -> FlowResult<bool> {
	match residual {
		None => Ok(true),
		Some(predicate) => Ok(predicate.evaluate(base_eval.with_value(merged)).await?.is_truthy()),
	}
}

/// Push `row` to the output batch, bumping the cumulative emitted-row counter and
/// failing when it exceeds the `SURREAL_GQL_MAX_OUTPUT_ROWS` ceiling. Counting at
/// the push (not per batch) is required: a single probe row crossed against a
/// large build side already fans out unboundedly, so the guard must trip inside
/// the inner loop.
fn emit(out: &mut Vec<Value>, emitted: &mut usize, max_rows: usize, row: Value) -> FlowResult<()> {
	*emitted += 1;
	if *emitted > max_rows {
		return Err(crate::exec::operators::gql_output_rows_exceeded(max_rows));
	}
	out.push(row);
	Ok(())
}

/// Build table keyed by `JoinKey: Ord`, with the rows sharing a key accumulating
/// in that key's `Vec` in insertion order. The stored row count is bounded by the
/// configured build-row budget.
///
/// **Ordered, not hashed.** A probe row joins a build row when their keys compare
/// equal, and `Value: Hash` does not agree with `Value: PartialEq` for numbers —
/// `0.1f == 0.1dec` while the two hash apart (see
/// [`Value::hash_agrees_with_eq`]). Hash-bucketing the keys would drop such a
/// match, reachable here through a compound record id holding a non-integral
/// number (`t:[0.1f]` against `t:[0.1dec]`). Ordering also makes iteration
/// deterministic, which is what [`Self::all_rows`] needs.
struct BuildTable {
	keyed: BTreeMap<JoinKey, Vec<Value>>,
	rows: usize,
}

impl BuildTable {
	fn new() -> Self {
		Self {
			keyed: BTreeMap::new(),
			rows: 0,
		}
	}

	/// Insert a build `row` under `key`, failing when the total stored row count
	/// would exceed `max_rows`.
	fn insert(&mut self, key: JoinKey, row: Value, max_rows: usize) -> Result<(), ControlFlow> {
		if self.rows >= max_rows {
			return Err(ControlFlow::Err(anyhow::anyhow!(crate::exec::Error::InvalidStatement(
				format!(
					"GQL MATCH join exceeded the maximum of {max_rows} build-side rows \
					 (configurable via SURREAL_GQL_MAX_JOIN_BUILD_ROWS)"
				),
			))));
		}
		self.keyed.entry(key).or_default().push(row);
		self.rows += 1;
		Ok(())
	}

	/// The build rows stored under `key`, if any.
	fn get(&self, key: &JoinKey) -> Option<&[Value]> {
		self.keyed.get(key).map(|rows| rows.as_slice())
	}

	/// Every stored build row, used by `Cross`, in an order that is deterministic
	/// across processes: by key, then insertion order within a key. `Cross` has no
	/// join keys at all, so every row shares the one empty key and this is plain
	/// insertion order.
	fn all_rows(&self) -> impl Iterator<Item = &Value> {
		self.keyed.values().flatten()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{ValuesOperator, collect, root_ctx};
	use crate::val::{RecordId, RecordIdKey, TableName};

	fn rid(table: &str, key: &str) -> RecordId {
		RecordId {
			table: TableName::new(table.to_string()),
			key: RecordIdKey::from(key.to_string()),
		}
	}

	/// A binding-row object: each `(binding, id)` pair becomes a full node
	/// object `{ id: table:key }` under `binding`.
	fn node_row(pairs: &[(&str, RecordId)]) -> Value {
		let mut row = Object::default();
		for (binding, id) in pairs {
			let mut node = Object::default();
			node.insert("id".to_string(), Value::RecordId(id.clone()));
			row.insert(binding.to_string(), Value::Object(node));
		}
		Value::Object(row)
	}

	/// A binding-row object with explicit slot values (for null/none cases).
	fn raw_row(pairs: &[(&str, Value)]) -> Value {
		let mut row = Object::default();
		for (binding, value) in pairs {
			row.insert(binding.to_string(), value.clone());
		}
		Value::Object(row)
	}

	/// Read `row[binding].id` back out as a `RecordId` for assertions.
	fn row_id(row: &Value, binding: &str) -> Option<RecordId> {
		let Value::Object(o) = row else {
			return None;
		};
		match o.get(binding) {
			Some(Value::Object(node)) => match node.get("id") {
				Some(Value::RecordId(rid)) => Some(rid.clone()),
				_ => None,
			},
			_ => None,
		}
	}

	fn join(
		build: Vec<Value>,
		probe: Vec<Value>,
		keys: &[&str],
		join_type: JoinType,
		null_template: &[&str],
	) -> HashJoin {
		join_with_residual(build, probe, keys, join_type, null_template, None)
	}

	fn join_with_residual(
		build: Vec<Value>,
		probe: Vec<Value>,
		keys: &[&str],
		join_type: JoinType,
		null_template: &[&str],
		residual: Option<Arc<dyn PhysicalExpr>>,
	) -> HashJoin {
		HashJoin::new(
			ValuesOperator::new(build),
			ValuesOperator::new(probe),
			keys.iter().map(|s| s.to_string()).collect(),
			join_type,
			null_template.iter().map(|s| s.to_string()).collect(),
			residual,
		)
	}

	/// A constant-`value` residual predicate, for exercising the ON-gate.
	fn const_residual(value: bool) -> Arc<dyn PhysicalExpr> {
		Arc::new(crate::exec::physical_expr::Literal(Value::Bool(value))) as Arc<dyn PhysicalExpr>
	}

	#[tokio::test]
	async fn inner_join_matches_on_shared_binding() {
		// build:  (a, b=v1), (a, b=v2)   probe: (c, b=v1)
		// shared key b ⇒ probe(c,b=v1) joins only the build row with b=v1.
		let build = vec![
			node_row(&[("a", rid("person", "alice")), ("b", rid("person", "x"))]),
			node_row(&[("a", rid("person", "amy")), ("b", rid("person", "y"))]),
		];
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", rid("person", "x"))])];
		let op: Arc<dyn ExecOperator> = Arc::new(join(build, probe, &["b"], JoinType::Inner, &[]));
		let out = collect(&op, &root_ctx()).await;

		assert_eq!(out.len(), 1, "only the b=x build row matches");
		let merged = &out[0];
		assert_eq!(row_id(merged, "a"), Some(rid("person", "alice")));
		assert_eq!(row_id(merged, "b"), Some(rid("person", "x")));
		assert_eq!(row_id(merged, "c"), Some(rid("person", "carol")));
	}

	/// A compound record id carrying a non-integral number is the shape that
	/// reaches the `Hash`/`PartialEq` disagreement through a join key: `t:[0.1f]`
	/// and `t:[0.1dec]` are the same id to `=` but hash apart, so a hash-bucketed
	/// build table would never compare them and the join would miss.
	#[tokio::test]
	async fn inner_join_matches_cross_variant_compound_record_id() {
		fn array_id(n: crate::val::Number) -> RecordId {
			RecordId {
				table: TableName::new("t".to_string()),
				key: RecordIdKey::Array(vec![Value::Number(n)].into()),
			}
		}
		let build_id = array_id(crate::val::Number::Decimal(
			<rust_decimal::Decimal as common::decimal::DecimalExt>::from_str_normalized("0.1")
				.unwrap(),
		));
		let probe_id = array_id(crate::val::Number::Float(0.1));
		// Precondition: one id to `=`, two keys to `Hash`.
		assert_eq!(Value::RecordId(build_id.clone()), Value::RecordId(probe_id.clone()));

		let build = vec![node_row(&[("a", rid("person", "alice")), ("b", build_id)])];
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", probe_id)])];
		let op: Arc<dyn ExecOperator> = Arc::new(join(build, probe, &["b"], JoinType::Inner, &[]));
		let out = collect(&op, &root_ctx()).await;

		assert_eq!(out.len(), 1, "equal ids must join: {out:?}");
		assert_eq!(row_id(&out[0], "a"), Some(rid("person", "alice")));
	}

	#[tokio::test]
	async fn inner_join_emits_one_row_per_build_match() {
		// Two build rows share key b=x; one probe row with b=x ⇒ two output rows.
		let build = vec![
			node_row(&[("a", rid("person", "a1")), ("b", rid("person", "x"))]),
			node_row(&[("a", rid("person", "a2")), ("b", rid("person", "x"))]),
		];
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", rid("person", "x"))])];
		let op: Arc<dyn ExecOperator> = Arc::new(join(build, probe, &["b"], JoinType::Inner, &[]));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 2);
	}

	#[tokio::test]
	async fn inner_join_no_match_emits_nothing() {
		let build = vec![node_row(&[("a", rid("person", "alice")), ("b", rid("person", "x"))])];
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", rid("person", "y"))])];
		let op: Arc<dyn ExecOperator> = Arc::new(join(build, probe, &["b"], JoinType::Inner, &[]));
		assert!(collect(&op, &root_ctx()).await.is_empty());
	}

	#[tokio::test]
	async fn left_join_miss_null_fills_template() {
		// Probe row with no build match ⇒ pass through with `a` and `__e0`
		// (the build-introduced bindings) nulled.
		let build = vec![node_row(&[("a", rid("person", "alice")), ("b", rid("person", "x"))])];
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", rid("person", "y"))])];
		let op: Arc<dyn ExecOperator> =
			Arc::new(join(build, probe, &["b"], JoinType::Left, &["a", "__e0"]));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 1);
		let row = &out[0];
		// Probe bindings survive.
		assert_eq!(row_id(row, "c"), Some(rid("person", "carol")));
		assert_eq!(row_id(row, "b"), Some(rid("person", "y")));
		// Template bindings nulled.
		let Value::Object(o) = row else {
			panic!("expected object");
		};
		assert_eq!(o.get("a"), Some(&Value::Null));
		assert_eq!(o.get("__e0"), Some(&Value::Null));
	}

	#[tokio::test]
	async fn left_join_match_does_not_null_fill() {
		let build = vec![node_row(&[("a", rid("person", "alice")), ("b", rid("person", "x"))])];
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", rid("person", "x"))])];
		let op: Arc<dyn ExecOperator> =
			Arc::new(join(build, probe, &["b"], JoinType::Left, &["a"]));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 1);
		// The build value is present (not nulled) on a match.
		assert_eq!(row_id(&out[0], "a"), Some(rid("person", "alice")));
	}

	#[tokio::test]
	async fn cross_join_emits_full_product() {
		// 2 build × 3 probe = 6 rows, no shared keys.
		let build = vec![node_row(&[("a", rid("t", "1"))]), node_row(&[("a", rid("t", "2"))])];
		let probe = vec![
			node_row(&[("b", rid("t", "x"))]),
			node_row(&[("b", rid("t", "y"))]),
			node_row(&[("b", rid("t", "z"))]),
		];
		let op: Arc<dyn ExecOperator> = Arc::new(join(build, probe, &[], JoinType::Cross, &[]));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 6);
		// Every output row carries both an `a` and a `b` binding.
		for row in &out {
			assert!(row_id(row, "a").is_some());
			assert!(row_id(row, "b").is_some());
		}
	}

	#[tokio::test]
	async fn cross_join_output_order_is_deterministic() {
		// `Cross` stores every build row under the one empty key, so `all_rows()`
		// yields them in insertion order and the cartesian product is build-minor
		// within each probe row — deterministic across processes, which a hashed
		// build table could not promise.
		let build = vec![
			node_row(&[("a", rid("t", "1"))]),
			node_row(&[("a", rid("t", "2"))]),
			node_row(&[("a", rid("t", "3"))]),
		];
		let probe = vec![node_row(&[("b", rid("t", "x"))])];
		let op: Arc<dyn ExecOperator> = Arc::new(join(build, probe, &[], JoinType::Cross, &[]));
		let out = collect(&op, &root_ctx()).await;
		let a_ids: Vec<_> = out.iter().filter_map(|r| row_id(r, "a")).collect();
		assert_eq!(a_ids, vec![rid("t", "1"), rid("t", "2"), rid("t", "3")]);
	}

	#[tokio::test]
	async fn left_join_residual_false_null_fills_not_drops() {
		// A residual that fails on the only key match must null-fill the probe
		// row (left-outer preserved), NOT drop it — the correlated-OPTIONAL
		// match-vs-null-fill decision. A post-join Filter would drop it.
		let build = vec![node_row(&[("a", rid("person", "alice")), ("b", rid("person", "x"))])];
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", rid("person", "x"))])];
		let op: Arc<dyn ExecOperator> = Arc::new(join_with_residual(
			build,
			probe,
			&["b"],
			JoinType::Left,
			&["a"],
			Some(const_residual(false)),
		));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 1, "the probe row survives, null-filled");
		assert_eq!(row_id(&out[0], "c"), Some(rid("person", "carol")));
		let Value::Object(o) = &out[0] else {
			panic!("expected object");
		};
		assert_eq!(o.get("a"), Some(&Value::Null), "build binding nulled on residual miss");
	}

	#[tokio::test]
	async fn left_join_residual_true_keeps_match() {
		// A residual that passes keeps the matched (non-null-filled) row.
		let build = vec![node_row(&[("a", rid("person", "alice")), ("b", rid("person", "x"))])];
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", rid("person", "x"))])];
		let op: Arc<dyn ExecOperator> = Arc::new(join_with_residual(
			build,
			probe,
			&["b"],
			JoinType::Left,
			&["a"],
			Some(const_residual(true)),
		));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 1);
		assert_eq!(row_id(&out[0], "a"), Some(rid("person", "alice")), "match kept, not nulled");
	}

	#[tokio::test]
	async fn inner_join_residual_false_drops_match() {
		// On an Inner join a failing residual drops the row (no null-fill).
		let build = vec![node_row(&[("a", rid("person", "alice")), ("b", rid("person", "x"))])];
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", rid("person", "x"))])];
		let op: Arc<dyn ExecOperator> = Arc::new(join_with_residual(
			build,
			probe,
			&["b"],
			JoinType::Inner,
			&[],
			Some(const_residual(false)),
		));
		assert!(collect(&op, &root_ctx()).await.is_empty());
	}

	#[tokio::test]
	async fn null_key_excluded_from_build_inner() {
		// One build row has b=NULL (no key) and is excluded; the other matches.
		let build = vec![
			raw_row(&[("a", Value::RecordId(rid("person", "anon"))), ("b", Value::Null)]),
			node_row(&[("a", rid("person", "alice")), ("b", rid("person", "x"))]),
		];
		let probe = vec![
			node_row(&[("c", rid("person", "carol")), ("b", rid("person", "x"))]),
			// A probe row with b=NULL never joins under Inner.
			raw_row(&[("c", Value::RecordId(rid("person", "dave"))), ("b", Value::Null)]),
		];
		let op: Arc<dyn ExecOperator> = Arc::new(join(build, probe, &["b"], JoinType::Inner, &[]));
		let out = collect(&op, &root_ctx()).await;
		// Only carol↔alice survives; both null-keyed rows drop.
		assert_eq!(out.len(), 1);
		assert_eq!(row_id(&out[0], "a"), Some(rid("person", "alice")));
	}

	#[tokio::test]
	async fn null_key_probe_passes_through_under_left() {
		// A null-keyed probe row is excluded from joining but, under Left,
		// passes through null-filled.
		let build = vec![node_row(&[("a", rid("person", "alice")), ("b", rid("person", "x"))])];
		let probe =
			vec![raw_row(&[("c", Value::RecordId(rid("person", "dave"))), ("b", Value::Null)])];
		let op: Arc<dyn ExecOperator> =
			Arc::new(join(build, probe, &["b"], JoinType::Left, &["a"]));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 1);
		let Value::Object(o) = &out[0] else {
			panic!("expected object");
		};
		// The probe binding `c` (a bare record id here) survives unchanged...
		assert_eq!(o.get("c"), Some(&Value::RecordId(rid("person", "dave"))));
		// ...and the build-introduced binding `a` is null-filled.
		assert_eq!(o.get("a"), Some(&Value::Null));
	}

	#[tokio::test]
	async fn empty_build_inner_yields_nothing_left_passes_through() {
		// Inner with empty build ⇒ nothing.
		let probe = vec![node_row(&[("c", rid("person", "carol")), ("b", rid("person", "x"))])];
		let inner: Arc<dyn ExecOperator> =
			Arc::new(join(Vec::new(), probe.clone(), &["b"], JoinType::Inner, &[]));
		assert!(collect(&inner, &root_ctx()).await.is_empty());

		// Left with empty build ⇒ every probe row null-filled.
		let left: Arc<dyn ExecOperator> =
			Arc::new(join(Vec::new(), probe, &["b"], JoinType::Left, &["a"]));
		let out = collect(&left, &root_ctx()).await;
		assert_eq!(out.len(), 1);
		let Value::Object(o) = &out[0] else {
			panic!("expected object");
		};
		assert_eq!(o.get("a"), Some(&Value::Null));
	}

	#[tokio::test]
	async fn empty_keys_left_is_uncorrelated_outer_join() {
		// The uncorrelated OPTIONAL shape (no shared variable): a `Left` join with
		// EMPTY keys pairs the one empty key on every side. A non-empty build ⇒ each
		// probe row crosses with every build row; an empty build ⇒ each probe row
		// passes through null-filled. (Cross short-circuits via `all_rows`, but an
		// empty-keys Left still routes through `join_key`/`get`, so this pins it.)
		let build = vec![node_row(&[("b", rid("t", "x"))]), node_row(&[("b", rid("t", "y"))])];
		let probe = vec![node_row(&[("a", rid("t", "1"))]), node_row(&[("a", rid("t", "2"))])];
		let op: Arc<dyn ExecOperator> =
			Arc::new(join(build, probe.clone(), &[], JoinType::Left, &["b"]));
		let out = collect(&op, &root_ctx()).await;
		// 2 probe × 2 build = 4 matched rows (no null-fill, the build matched).
		assert_eq!(out.len(), 4);
		for row in &out {
			assert!(row_id(row, "a").is_some());
			assert!(row_id(row, "b").is_some());
		}

		// Empty build ⇒ every probe row null-filled (the accumulator is preserved).
		let empty: Arc<dyn ExecOperator> =
			Arc::new(join(Vec::new(), probe, &[], JoinType::Left, &["b"]));
		let out = collect(&empty, &root_ctx()).await;
		assert_eq!(out.len(), 2);
		for row in &out {
			let Value::Object(o) = row else {
				panic!("expected object");
			};
			assert_eq!(o.get("b"), Some(&Value::Null));
		}
	}

	#[tokio::test]
	async fn multi_key_join_uses_all_key_bindings() {
		// Two-column key (a, b). Only the probe row matching on BOTH joins.
		let build =
			vec![node_row(&[("a", rid("t", "1")), ("b", rid("t", "2")), ("x", rid("t", "build"))])];
		let probe = vec![
			// matches on both a and b
			node_row(&[("a", rid("t", "1")), ("b", rid("t", "2")), ("y", rid("t", "p1"))]),
			// matches a but not b
			node_row(&[("a", rid("t", "1")), ("b", rid("t", "9")), ("y", rid("t", "p2"))]),
		];
		let op: Arc<dyn ExecOperator> =
			Arc::new(join(build, probe, &["a", "b"], JoinType::Inner, &[]));
		let out = collect(&op, &root_ctx()).await;
		assert_eq!(out.len(), 1);
		assert_eq!(row_id(&out[0], "y"), Some(rid("t", "p1")));
		assert_eq!(row_id(&out[0], "x"), Some(rid("t", "build")));
	}

	#[test]
	fn build_table_guard_names_the_knob() {
		let mut table = BuildTable::new();
		// First insert under a budget of 1 succeeds.
		assert!(table.insert(vec![Value::from(1)], Value::from(10), 1).is_ok());
		// A second distinct row past the budget errors and names the knob.
		let err = table.insert(vec![Value::from(2)], Value::from(20), 1).unwrap_err();
		let msg = match err {
			ControlFlow::Err(e) => e.to_string(),
			other => panic!("expected error, got {other:?}"),
		};
		assert!(
			msg.contains("SURREAL_GQL_MAX_JOIN_BUILD_ROWS"),
			"guard error must name the knob, got: {msg}"
		);
	}

	#[test]
	fn build_table_budget_counts_rows_not_keys() {
		// Two rows sharing one key still count as two rows against the budget.
		let mut table = BuildTable::new();
		assert!(table.insert(vec![Value::from(1)], Value::from(10), 2).is_ok());
		assert!(table.insert(vec![Value::from(1)], Value::from(11), 2).is_ok());
		let err = table.insert(vec![Value::from(1)], Value::from(12), 2).unwrap_err();
		assert!(matches!(err, ControlFlow::Err(_)));
		// The shared key holds both rows.
		assert_eq!(table.get(&vec![Value::from(1)]).map(|r| r.len()), Some(2));
	}

	#[test]
	fn binding_id_extraction_rules() {
		// Full object ⇒ its id.
		let node = node_row(&[("a", rid("t", "1"))]);
		let Value::Object(o) = &node else {
			panic!();
		};
		assert_eq!(binding_id(o.get("a")), Some(Value::RecordId(rid("t", "1"))));
		// Bare record id ⇒ itself.
		assert_eq!(
			binding_id(Some(&Value::RecordId(rid("t", "2")))),
			Some(Value::RecordId(rid("t", "2")))
		);
		// Null / None / missing / object-without-id / non-record ⇒ None.
		assert_eq!(binding_id(Some(&Value::Null)), None);
		assert_eq!(binding_id(Some(&Value::None)), None);
		assert_eq!(binding_id(None), None);
		assert_eq!(binding_id(Some(&Value::Bool(true))), None);
		let mut no_id = Object::default();
		no_id.insert("name".to_string(), Value::from("x"));
		assert_eq!(binding_id(Some(&Value::Object(no_id))), None);
		// Object whose id is itself Null ⇒ None.
		let mut null_id = Object::default();
		null_id.insert("id".to_string(), Value::Null);
		assert_eq!(binding_id(Some(&Value::Object(null_id))), None);
	}

	#[test]
	fn join_key_returns_none_on_any_null_slot() {
		let row = raw_row(&[("a", Value::RecordId(rid("t", "1"))), ("b", Value::Null)]);
		assert_eq!(join_key(&row, &["a".to_string(), "b".to_string()]), None);
	}

	#[test]
	fn name_and_attrs() {
		let op = join(Vec::new(), Vec::new(), &["b"], JoinType::Inner, &[]);
		assert_eq!(op.name(), "HashJoin");
		let attrs = op.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "type" && v == "Inner"));
		assert!(attrs.iter().any(|(k, v)| k == "keys" && v == "b"));

		let left = join(Vec::new(), Vec::new(), &["b"], JoinType::Left, &["a", "k"]);
		let left_attrs = left.attrs();
		assert!(left_attrs.iter().any(|(k, v)| k == "null_template" && v == "a, k"));

		let cross = join(Vec::new(), Vec::new(), &[], JoinType::Cross, &[]);
		let cross_attrs = cross.attrs();
		assert!(cross_attrs.iter().any(|(k, v)| k == "type" && v == "Cross"));
		// No keys / null_template rendered when empty.
		assert!(!cross_attrs.iter().any(|(k, _)| k == "keys"));
	}

	#[test]
	fn children_are_build_then_probe() {
		let op = join(Vec::new(), Vec::new(), &["b"], JoinType::Inner, &[]);
		assert_eq!(op.children().len(), 2);
	}
}
