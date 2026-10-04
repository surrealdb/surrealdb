//! `HashJoin` — an equi-join of two row streams, the right side built into a
//! table in memory and the left side probed against it.
//!
//! The operator serves two row conventions, chosen by [`JoinShape`]:
//!
//! - **Binding rows** ([`JoinShape::Bindings`]): the binding tables produced by two GQL `MATCH`
//!   sub-plans — two comma-separated patterns inside one `MATCH`, or two sequential `MATCH` clauses
//!   — that share zero or more *node* variables (the planner makes repeated node variables join
//!   keys, see `doc/gql/V2_DESIGN.md` §6). Each input row is a `Value::Object` keyed by binding
//!   name (§3); the output row is the union of a build row and a probe row.
//! - **Document rows** ([`JoinShape::Documents`]): the rows of a SurrealQL pipeline, joined by
//!   arbitrary key expressions. The output row is the probe row with the matched build row added
//!   under one top-level key, the join's *alias*; a semi or anti join emits the probe row as it is.
//!
//! ## Join key — binding rows
//!
//! The join is keyed on `<binding>.id` for each shared binding name in
//! [`keys`], in order. A binding row holds a full record object, so
//! `<binding>.id` is its `id` field (a `Value::RecordId`); a bare
//! `Value::RecordId` slot is tolerated defensively. The key is the `Vec<Value>`
//! of those id values, one per key binding. **No fetch happens here** — keys
//! are read straight from the already-bound objects (§5). Because the shared
//! keys are equal by construction on every emitted match, the merged row keeps
//! the probe side's copy of each shared binding (they are identical, so the
//! choice is immaterial).
//!
//! [`keys`]: JoinShape::Bindings::keys
//!
//! ### NULL / NONE keys (3VL, §0 "Joins & null")
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
//! [`null_template`]: JoinShape::Bindings::null_template
//!
//! ## Join key — document rows
//!
//! Each side's key is a list of expressions, one per key column: the probe
//! expressions are evaluated against the probe row, the build expressions
//! against the build row seen through the alias (`{ alias: row }`), so the
//! same expression reads the same field on both sides of the join. Two keys
//! match when they are equal by SurrealQL's `=`, which is what the `IN` and
//! `CONTAINS` these joins replace apply element by element: `NULL` matches
//! `NULL`, `NONE` matches `NONE`, and a string matches a regex it satisfies.
//! No key is excluded for being `NULL` or `NONE`.
//!
//! ## Join types
//!
//! - [`Inner`](JoinType::Inner): emit the union of every probe row with each build row sharing its
//!   key.
//! - [`Left`](JoinType::Left): like `Inner`, but a probe row with no matching build row (including
//!   a null-keyed probe row) is still emitted once, with the build-introduced bindings
//!   ([`null_template`]) — or the alias, for document rows — set to `Value::Null`. Used by GQL
//!   `OPTIONAL MATCH`.
//! - [`Semi`](JoinType::Semi): emit each probe row with at least one matching build row, once and
//!   unchanged. The form of `x IN (SELECT VALUE k FROM t …)`.
//! - [`Anti`](JoinType::Anti): emit each probe row with no matching build row, unchanged. The form
//!   of `x NOT IN (SELECT VALUE k FROM t …)`.
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
//! surrealdb/surrealdb#7024. For a semi or anti join the residual decides which
//! candidate build rows count as matches.
//!
//! ## Evaluation frame
//!
//! A document join lifted out of a `PERMISSIONS FOR select` predicate carries a
//! [`JoinFrame`]: its keys and residual are evaluated, and its build side is
//! run, in the permission frame — reads inside them are not themselves
//! permission-checked and run under the definer's recorded authority — exactly
//! as the predicate they came from would be by `PermissionFilter`. Inside an
//! enclosing permission frame (`skip_fetch_perms` set on the context's root) a
//! semi or anti join in a frame passes every probe row through, as
//! `PermissionFilter` does: enforcement is bypassed there. For a checked caller
//! such a join hides its keys, residual and build plan from `EXPLAIN`, since
//! they are the table definer's.
//!
//! ## Build budget & output bound
//!
//! The build side is materialised fully in memory (the probe side streams). For
//! binding rows, the number of build rows is bounded by
//! `SURREAL_GQL_MAX_JOIN_BUILD_ROWS` (the shared GQL in-memory-build budget,
//! default 1M); exceeding it fails the query with an error that names the knob.
//! Spill to disk is a future change, matching the `Aggregate` / `Distinct`
//! stance. Separately, the cumulative number of rows *emitted* (a `Cross`
//! product, or a high-fan-out equi-join, can emit far more rows than either
//! side holds) is bounded by `SURREAL_GQL_MAX_OUTPUT_ROWS`; the probe loop also
//! polls cancellation between batches so a runaway join is interruptible. A
//! document join has no budget: the scalar subquery it replaces materialises
//! the same rows for every probe row.
//!
//! ## Build and probe order
//!
//! When neither side mutates, the probe side is pulled first: its first
//! non-empty batch is awaited before the build side is constructed, and a
//! probe that yields no row ends the join with the build side never
//! constructed and never polled. A right side lifted from an `IN` subquery is
//! therefore evaluated only when a left row reaches the join, as the per-row
//! subquery it replaces is evaluated only for a row the other conjuncts
//! admitted — a right filter that would raise on a stored value raises on
//! neither engine when no row reaches it. Once a probe row exists the build
//! side is drained fully into the build table, then the probe streams.
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

use super::JoinFrame;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, OutputOrdering, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::{Object, Value};

/// The kind of join `HashJoin` performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
	/// Emit only probe rows that match at least one build row.
	Inner,
	/// Emit every probe row; unmatched probe rows are null-filled with the
	/// build-introduced bindings (`null_template`) or the alias. Used by
	/// `OPTIONAL MATCH`.
	Left,
	/// Emit each probe row with at least one match, unchanged.
	Semi,
	/// Emit each probe row with no match, unchanged.
	Anti,
	/// Cartesian product (`keys` is empty): every build row × every probe row.
	Cross,
}

impl JoinType {
	/// EXPLAIN rendering of the join type.
	pub(crate) fn label(self) -> &'static str {
		match self {
			JoinType::Inner => "Inner",
			JoinType::Left => "Left",
			JoinType::Semi => "Semi",
			JoinType::Anti => "Anti",
			JoinType::Cross => "Cross",
		}
	}
}

/// How the two sides' rows are keyed and merged; see the module docs.
#[derive(Debug, Clone)]
pub(crate) enum JoinShape {
	/// Binding rows, keyed on `<binding>.id`.
	Bindings {
		/// Shared binding names to equi-join on (`<binding>.id` per side).
		/// Empty for a `Cross` join.
		keys: Vec<String>,
		/// Build-introduced binding names, set to `Value::Null` on a `Left`
		/// miss. Empty for `Inner` / `Cross`.
		null_template: Vec<String>,
	},
	/// Document rows, keyed by expressions; the matched build row lands on
	/// the probe row under `alias`.
	Documents {
		/// Evaluated against the probe row, one per key column.
		probe_keys: Vec<Arc<dyn PhysicalExpr>>,
		/// Evaluated against `{ alias: build row }`, one per key column.
		build_keys: Vec<Arc<dyn PhysicalExpr>>,
		alias: String,
	},
}

/// Equi-join / cartesian product of two row streams.
///
/// See the module docs for the key extraction, the null-key rules per join
/// type, and the build budget.
#[derive(Debug, Clone)]
pub struct HashJoin {
	/// Child producing the build side; materialised fully in memory.
	pub(crate) build: Arc<dyn ExecOperator>,
	/// Child producing the probe side; streamed.
	pub(crate) probe: Arc<dyn ExecOperator>,
	pub(crate) shape: JoinShape,
	/// The kind of join.
	pub(crate) join_type: JoinType,
	/// Residual join predicate (SQL `ON`), evaluated against the merged
	/// (build ∪ probe) row. A merged row that fails it is not a match. For
	/// `Left`, a probe row with no residual-passing build row is null-filled
	/// (the correlated-`OPTIONAL` match-vs-null-fill decision), NOT dropped.
	/// `None` ⇒ a pure equi-join.
	pub(crate) residual: Option<Arc<dyn PhysicalExpr>>,
	/// The permission frame a document join lifted from a permission
	/// predicate evaluates in; `None` evaluates as the caller.
	pub(crate) frame: Option<JoinFrame>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl HashJoin {
	/// A join of binding rows, keyed on the shared bindings `keys`.
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
			shape: JoinShape::Bindings {
				keys,
				null_template,
			},
			join_type,
			residual,
			frame: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// A join of document rows: each probe row keyed by `probe_keys`, each
	/// build row by `build_keys` read through `alias`, the matched build row
	/// added to the probe row under `alias`.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn documents(
		build: Arc<dyn ExecOperator>,
		probe: Arc<dyn ExecOperator>,
		probe_keys: Vec<Arc<dyn PhysicalExpr>>,
		build_keys: Vec<Arc<dyn PhysicalExpr>>,
		join_type: JoinType,
		alias: String,
		residual: Option<Arc<dyn PhysicalExpr>>,
		frame: Option<JoinFrame>,
	) -> Self {
		Self {
			build,
			probe,
			shape: JoinShape::Documents {
				probe_keys,
				build_keys,
				alias,
			},
			join_type,
			residual,
			frame,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Whether the join's expressions and build plan are the table
	/// definer's and the plan is compiled for a caller they are enforced on.
	fn redacted(&self) -> bool {
		self.frame.as_ref().is_some_and(|frame| frame.checked_caller)
	}

	/// Every expression the join evaluates, with its role.
	fn own_expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		let mut out = Vec::new();
		if let JoinShape::Documents {
			probe_keys,
			build_keys,
			..
		} = &self.shape
		{
			out.extend(probe_keys.iter().map(|key| ("probe_key", key)));
			out.extend(build_keys.iter().map(|key| ("build_key", key)));
		}
		if let Some(residual) = self.residual.as_ref() {
			out.push(("residual", residual));
		}
		out
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
		match &self.shape {
			JoinShape::Bindings {
				keys,
				null_template,
			} => {
				if !keys.is_empty() {
					attrs.push(("keys".to_string(), keys.join(", ")));
				}
				if !null_template.is_empty() {
					attrs.push(("null_template".to_string(), null_template.join(", ")));
				}
			}
			JoinShape::Documents {
				probe_keys,
				build_keys,
				alias,
			} => {
				attrs.push(("alias".to_string(), alias.clone()));
				if self.redacted() {
					attrs.push(("on".to_string(), "<permission>".to_string()));
				} else if !probe_keys.is_empty() {
					let on = probe_keys
						.iter()
						.zip(build_keys)
						.map(|(probe, build)| format!("{} = {}", probe.to_sql(), build.to_sql()))
						.collect::<Vec<_>>()
						.join(" AND ");
					attrs.push(("on".to_string(), on));
				}
			}
		}
		if let Some(residual) = self.residual.as_ref() {
			let residual = if self.redacted() {
				"<permission>".to_string()
			} else {
				residual.to_sql()
			};
			attrs.push(("residual".to_string(), residual));
		}
		if let Some(frame) = &self.frame {
			attrs.push(("frame".to_string(), "permission".to_string()));
			if frame.auth.is_some() {
				attrs.push(("auth".to_string(), "limited".to_string()));
			}
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		let mut level = ContextLevel::Database
			.max(self.build.required_context())
			.max(self.probe.required_context());
		for (_, expr) in self.own_expressions() {
			level = level.max(expr.required_context());
		}
		level
	}

	fn access_mode(&self) -> AccessMode {
		let mut mode = self.build.access_mode().combine(self.probe.access_mode());
		for (_, expr) in self.own_expressions() {
			mode = mode.combine(expr.access_mode());
		}
		mode
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		match self.join_type {
			// A semi or anti join emits a subset of the probe rows.
			JoinType::Semi | JoinType::Anti => self.probe.cardinality_hint(),
			// Any other join fans out to an unknown number of rows.
			JoinType::Inner | JoinType::Left | JoinType::Cross => CardinalityHint::Unbounded,
		}
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		// Binding rows list the build side first. Document rows list the
		// probe side first, as the logical join lists its left input, so a
		// plan reads top to bottom as the pipeline's source and then what it
		// is joined against, the order `LookupJoin` uses.
		match self.shape {
			JoinShape::Bindings {
				..
			} => vec![&self.build, &self.probe],
			JoinShape::Documents {
				..
			} => vec![&self.probe, &self.build],
		}
	}

	fn explain_children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		// The build plan is the definer's; a checked caller is shown the
		// probe side only.
		if self.redacted() {
			vec![&self.probe]
		} else {
			self.children()
		}
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn hides_metrics_beneath(&self) -> bool {
		// The probe side's rows are the records before the permission judged
		// them; a checked caller is shown the counts of what reaches it.
		self.redacted()
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		// The expressions are the definer's; a checked caller is shown none
		// of them.
		if self.redacted() {
			return Vec::new();
		}
		self.own_expressions()
	}

	fn output_ordering(&self) -> OutputOrdering {
		// Probe rows are emitted in probe order, the matches of one row
		// together, so the probe side's ordering holds on the output.
		self.probe.output_ordering()
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
		//  - neither mutates: construct the probe eagerly and the build only once the probe has
		//    yielded a row (the module docs, "Build and probe order"): an empty probe leaves the
		//    build side unbuilt.
		let buffer_size = ctx.root().ctx.config.exec.operator_buffer_size;
		let build = JoinSide::new(&self.build);
		let probe = JoinSide::new(&self.probe);
		let probe_first = probe.mutates() && !build.mutates();

		// A framed semi or anti join inside an enclosing permission frame
		// enforces nothing; see the module docs.
		if self.frame.is_some()
			&& ctx.root().skip_fetch_perms
			&& matches!(self.join_type, JoinType::Semi | JoinType::Anti)
		{
			return Ok(monitor_stream(probe.stream(ctx, buffer_size)?, "HashJoin", &self.metrics));
		}

		// The keys, residual and build side evaluate in the join's frame; the
		// probe side is the enclosing pipeline's and runs in its context.
		let frame_ctx = match &self.frame {
			Some(frame) => frame.context(ctx),
			None => ctx.clone(),
		};

		// Eagerly construct a side only when doing so is safe — i.e. it neither
		// reads pre-write data nor is the pre-drained writer. The build is eager
		// only when it mutates (its writes land before the probe is constructed);
		// otherwise it waits for the probe's first row (`peek_probe`) or for the
		// probe's pre-drain (`probe_first`). The probe is eager unless it must
		// read post-write state (`build.mutates()`) or it is the pre-drained
		// writer (`probe_first`). Anything left `None` is constructed lazily
		// inside the stream, after the writing side has drained.
		let eager_build = if build.mutates() && !probe_first {
			Some(build.stream(&frame_ctx, buffer_size)?)
		} else {
			None
		};
		let eager_probe = if build.mutates() || probe_first {
			None
		} else {
			Some(probe.stream(ctx, buffer_size)?)
		};

		let shape = self.shape.clone();
		let join_type = self.join_type;
		let residual = self.residual.clone();
		// The GQL budgets bound binding-row joins; a document join has none.
		let (max_rows, max_output_rows) = match shape {
			JoinShape::Bindings {
				..
			} => (
				ctx.root().ctx.config.exec.gql_max_join_build_rows,
				ctx.root().ctx.config.exec.gql_max_output_rows,
			),
			JoinShape::Documents {
				..
			} => (usize::MAX, usize::MAX),
		};
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

			// ---- Peek phase (neither side mutates): the probe's first non-empty
			// batch is awaited before the build side exists. A probe that yields
			// no row ends the join here, the build side never constructed and
			// never polled; the batch found is replayed ahead of the rest of the
			// probe in the probe phase.
			let peeked_probe: Option<(ValueBatchStream, ValueBatch)> = match eager_probe {
				Some(mut probe_stream) => {
					let first = loop {
						match probe_stream.next().await {
							None => return Ok(()),
							Some(batch_result) => {
								crate::exec::operators::check_cancelled(&ctx)?;
								let batch = batch_result?;
								if !batch.is_empty() {
									break batch;
								}
							}
						}
					};
					Some((probe_stream, first))
				}
				None => None,
			};

			// ---- Build phase: drain the build side fully into the build table.
			// Constructed now (after the probe pre-drain when `probe_first`, after
			// the probe's first row otherwise) so a post-write build reads the
			// live transaction state and an empty probe builds nothing.
			let mut build_stream = match eager_build {
				Some(stream) => stream,
				None => build.stream(&frame_ctx, buffer_size)?,
			};
			let frame_eval = EvalContext::from_exec_ctx(&frame_ctx);
			let mut table = BuildTable::new();
			while let Some(batch_result) = build_stream.next().await {
				// Draining the whole build side can run long without yielding;
				// poll cancellation so a client disconnect / timeout interrupts.
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				for row in batch.into_values() {
					if let Some(key) = shape.build_key(&row, &frame_eval).await? {
						table.insert(key, row, max_rows)?
					}
				}
			}

			// ---- Probe phase: stream the probe side, emitting matches.
			// Either replay the pre-drained (post-write) probe rows, continue the
			// peeked probe behind its first batch, or construct the probe now —
			// deferred past the build phase when `build_mutates` so its scans
			// read the live, post-write transaction state.
			let mut probe_stream: ValueBatchStream = match (prebuffered_probe, peeked_probe) {
				(Some(batches), _) => Box::pin(futures::stream::iter(batches.into_iter().map(Ok))),
				(None, Some((rest, first))) => {
					Box::pin(futures::stream::once(std::future::ready(Ok(first))).chain(rest))
				}
				(None, None) => probe.stream(&ctx, buffer_size)?,
			};
			// `emitted` bounds the cumulative output (a Cross product or a
			// high-fan-out equi-join can emit far more rows than either side
			// holds).
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
								if residual_passes(
									residual.as_deref(),
									&frame_eval,
									&shape,
									build_row,
									&row,
								)
								.await?
								{
									let merged = shape.merge(build_row, &row);
									emit(&mut out, &mut emitted, max_output_rows, merged)?;
								}
							}
						}
						JoinType::Inner => {
							// A probe row without a key matches nothing and is
							// dropped; otherwise emit the union with each
							// residual-passing build row sharing the key.
							if let Some(key) = shape.probe_key(&row, &frame_eval).await? {
								for build_row in table.matches(&key) {
									if residual_passes(
										residual.as_deref(),
										&frame_eval,
										&shape,
										build_row,
										&row,
									)
									.await?
									{
										let merged = shape.merge(build_row, &row);
										emit(&mut out, &mut emitted, max_output_rows, merged)?;
									}
								}
							}
						}
						JoinType::Left => {
							// A build row counts as a match only if the merged row
							// passes the residual; a probe row with no passing build
							// row (including a keyless one) is null-filled — NOT
							// dropped — which is the correlated-OPTIONAL match-vs-null
							// -fill decision (a post-join Filter would drop it).
							let mut matched = false;
							if let Some(key) = shape.probe_key(&row, &frame_eval).await? {
								for build_row in table.matches(&key) {
									if residual_passes(
										residual.as_deref(),
										&frame_eval,
										&shape,
										build_row,
										&row,
									)
									.await?
									{
										let merged = shape.merge(build_row, &row);
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
									shape.null_filled(&row),
								)?;
							}
						}
						JoinType::Semi | JoinType::Anti => {
							// The probe row itself is emitted, once, when it has a
							// residual-passing match (semi) or none (anti).
							let mut matched = false;
							if let Some(key) = shape.probe_key(&row, &frame_eval).await? {
								for build_row in table.matches(&key) {
									if residual_passes(
										residual.as_deref(),
										&frame_eval,
										&shape,
										build_row,
										&row,
									)
									.await?
									{
										matched = true;
										break;
									}
								}
							}
							if matched == (join_type == JoinType::Semi) {
								emit(&mut out, &mut emitted, max_output_rows, row)?;
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

impl JoinShape {
	/// The key of a build row, or `None` for a row that can match nothing.
	async fn build_key(&self, row: &Value, eval: &EvalContext<'_>) -> FlowResult<Option<JoinKey>> {
		match self {
			JoinShape::Bindings {
				keys,
				..
			} => Ok(join_key(row, keys)),
			JoinShape::Documents {
				build_keys,
				alias,
				..
			} => {
				// The build expressions read the row through the alias, as the
				// residual and everything above the join do.
				let mut seen = Object::default();
				seen.insert(alias.clone(), row.clone());
				let seen = Value::Object(seen);
				let keys = evaluate_keys(build_keys, eval, &seen).await?;
				if keys.iter().any(|k| matches!(k, Value::Null | Value::None)) {
					Ok(None)
				} else {
					Ok(Some(keys))
				}
			}
		}
	}

	/// The key of a probe row, or `None` for a row that can match nothing.
	async fn probe_key(&self, row: &Value, eval: &EvalContext<'_>) -> FlowResult<Option<JoinKey>> {
		match self {
			JoinShape::Bindings {
				keys,
				..
			} => Ok(join_key(row, keys)),
			JoinShape::Documents {
				probe_keys,
				..
			} => {
				let keys = evaluate_keys(probe_keys, eval, row).await?;
				if keys.iter().any(|k| matches!(k, Value::Null | Value::None)) {
					Ok(None)
				} else {
					Ok(Some(keys))
				}
			}
		}
	}

	/// The output row for a build row matching a probe row.
	fn merge(&self, build_row: &Value, probe_row: &Value) -> Value {
		match self {
			JoinShape::Bindings {
				..
			} => merge_rows(build_row, probe_row),
			JoinShape::Documents {
				alias,
				..
			} => with_alias(probe_row, alias, build_row.clone()),
		}
	}

	/// The row the residual predicate reads for a build row and a probe row.
	///
	/// This is [`JoinShape::merge`]'s row wherever the merge can carry the
	/// alias. A probe row that is not an object cannot — a bare value from a
	/// `VALUE` projection or a scalar source has no field to hold it, and is
	/// emitted as it is — but the residual still has to read the build row
	/// through the alias, so it reads an object holding the alias alone.
	/// Every other name answers `NONE` there, as it does off the bare value:
	/// a residual is built from field reads, `$parent.path`, literals and
	/// operators over them (`Side::of` in `logical/rules/decorrelate.rs`),
	/// never from a read of the row itself.
	fn residual_row(&self, build_row: &Value, probe_row: &Value) -> Value {
		match self {
			JoinShape::Documents {
				alias,
				..
			} if !matches!(probe_row, Value::Object(_)) => {
				let mut row = Object::default();
				row.insert(alias.clone(), build_row.clone());
				Value::Object(row)
			}
			_ => self.merge(build_row, probe_row),
		}
	}

	/// The output row for a probe row a `Left` join found no match for.
	fn null_filled(&self, probe_row: &Value) -> Value {
		match self {
			JoinShape::Bindings {
				null_template,
				..
			} => null_filled(probe_row, null_template),
			JoinShape::Documents {
				alias,
				..
			} => with_alias(probe_row, alias, Value::Null),
		}
	}
}

/// A join key: one value per key column.
type JoinKey = Vec<Value>;

/// Evaluate each key expression against `row`, as the row and its document.
async fn evaluate_keys(
	keys: &[Arc<dyn PhysicalExpr>],
	eval: &EvalContext<'_>,
	row: &Value,
) -> FlowResult<JoinKey> {
	let mut out = Vec::with_capacity(keys.len());
	for key in keys {
		out.push(key.evaluate(eval.with_value_and_doc(row)).await?);
	}
	Ok(out)
}

/// The probe row with `value` set under `alias`. A probe row that is not an
/// object — a bare value from a `VALUE` projection — has no field to hold
/// the alias, and is emitted as it is.
fn with_alias(probe_row: &Value, alias: &str, value: Value) -> Value {
	match probe_row {
		Value::Object(row) => {
			let mut row = row.clone();
			row.insert(alias.to_string(), value);
			Value::Object(row)
		}
		other => other.clone(),
	}
}

/// Extract the join key for `row`: the `id` of each binding named in `keys`.
///
/// Returns `None` if any key slot is `Value::Null` / `Value::None`, missing, or
/// not a record (i.e. has no usable id) — such a row never equi-joins (3VL).
/// For a `Cross` join `keys` is empty; the planner uses [`BuildTable::all_rows`]
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

/// Whether a build row and a probe row pass the residual (`ON`) predicate.
/// `None` ⇒ always passes (a pure equi-join), and the row the predicate would
/// read is not built. Mirrors `Expand`'s per-candidate predicate eval
/// (`graph/expand.rs`) and `Filter`.
async fn residual_passes(
	residual: Option<&dyn PhysicalExpr>,
	base_eval: &EvalContext<'_>,
	shape: &JoinShape,
	build_row: &Value,
	probe_row: &Value,
) -> FlowResult<bool> {
	let Some(predicate) = residual else {
		return Ok(true);
	};
	let row = shape.residual_row(build_row, probe_row);
	Ok(predicate.evaluate(base_eval.with_value_and_doc(&row)).await?.is_truthy())
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

/// Whether two keys are equal by SurrealQL's `=`, column by column.
fn keys_equal(left: &[Value], right: &[Value]) -> bool {
	left.len() == right.len() && left.iter().zip(right).all(|(l, r)| l.equal(r))
}

/// Whether a key holds a regex in any column, which `=` matches against
/// strings rather than by identity.
fn key_has_regex(key: &[Value]) -> bool {
	key.iter().any(|v| matches!(v, Value::Regex(_)))
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
///
/// A key holding a regex is kept apart: `=` matches a regex against a string
/// it satisfies, which no ordering expresses, so such keys are compared one
/// by one with [`Value::equal`] — on the build side against every probe key,
/// and a regex probe key against every build row.
struct BuildTable {
	keyed: BTreeMap<JoinKey, Vec<Value>>,
	/// Build rows whose key holds a regex, with their keys.
	regex_keyed: Vec<(JoinKey, Value)>,
	rows: usize,
}

impl BuildTable {
	fn new() -> Self {
		Self {
			keyed: BTreeMap::new(),
			regex_keyed: Vec::new(),
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
		if key_has_regex(&key) {
			self.regex_keyed.push((key, row));
		} else {
			self.keyed.entry(key).or_default().push(row);
		}
		self.rows += 1;
		Ok(())
	}

	/// The build rows stored under `key`, if any, by ordering equality alone.
	fn get(&self, key: &JoinKey) -> Option<&[Value]> {
		self.keyed.get(key).map(|rows| rows.as_slice())
	}

	/// The build rows whose key equals `key` by `=`: those stored under it,
	/// then those a regex on either side matches.
	fn matches<'a>(&'a self, key: &'a JoinKey) -> impl Iterator<Item = &'a Value> + 'a {
		let exact = self.get(key).unwrap_or(&[]).iter();
		let build_regex =
			self.regex_keyed.iter().filter(move |(k, _)| keys_equal(k, key)).map(|(_, row)| row);
		let probe_regex = key_has_regex(key)
			.then(|| {
				self.keyed
					.iter()
					.filter(move |(k, _)| keys_equal(k, key))
					.flat_map(|(_, rows)| rows.iter())
			})
			.into_iter()
			.flatten();
		exact.chain(build_regex).chain(probe_regex)
	}

	/// Every stored build row, used by `Cross`, in an order that is deterministic
	/// across processes: by key, then insertion order within a key. `Cross` has no
	/// join keys at all, so every row shares the one empty key and this is plain
	/// insertion order.
	fn all_rows(&self) -> impl Iterator<Item = &Value> {
		self.keyed.values().flatten().chain(self.regex_keyed.iter().map(|(_, row)| row))
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

	#[test]
	fn a_scalar_probe_row_still_carries_the_alias_into_the_residual() {
		fn read(row: &Value, name: &str) -> Value {
			match row {
				Value::Object(o) => o.get(name).cloned().unwrap_or(Value::None),
				_ => Value::None,
			}
		}
		let shape = JoinShape::Documents {
			probe_keys: vec![],
			build_keys: vec![],
			alias: "_join0".to_string(),
		};
		let build = raw_row(&[("n", Value::from(2))]);

		// An object probe row holds the alias, so the residual reads the
		// merged row itself.
		let probe = raw_row(&[("z", Value::from(1))]);
		let row = shape.residual_row(&build, &probe);
		assert_eq!(row, shape.merge(&build, &probe));
		assert_eq!(read(&row, "z"), Value::from(1));
		assert_eq!(read(&read(&row, "_join0"), "n"), Value::from(2));

		// A bare value has no field to hold the alias and is emitted as it
		// is, but the residual still reads the build row through the alias.
		// Every other name answers `NONE`, as it does off the bare value.
		let probe = Value::from(0);
		assert_eq!(shape.merge(&build, &probe), Value::from(0));
		let row = shape.residual_row(&build, &probe);
		assert_eq!(read(&read(&row, "_join0"), "n"), Value::from(2));
		assert_eq!(read(&row, "z"), Value::None);
	}

	/// A constant-`value` residual predicate, for exercising the ON-gate.
	fn const_residual(value: bool) -> Arc<dyn PhysicalExpr> {
		Arc::new(crate::exec::physical_expr::Literal(Value::Bool(value))) as Arc<dyn PhysicalExpr>
	}

	/// A build side that fails the test if it is ever constructed.
	#[derive(Debug)]
	struct UnreachableSource;

	impl ExecOperator for UnreachableSource {
		fn name(&self) -> &'static str {
			"Unreachable"
		}

		fn required_context(&self) -> ContextLevel {
			ContextLevel::Root
		}

		fn access_mode(&self) -> AccessMode {
			AccessMode::ReadOnly
		}

		fn cardinality_hint(&self) -> CardinalityHint {
			CardinalityHint::Unbounded
		}

		fn output_ordering(&self) -> OutputOrdering {
			OutputOrdering::Unordered
		}

		fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
			panic!("the build side of a join with an empty probe must never be executed")
		}
	}

	#[tokio::test]
	async fn an_empty_probe_never_executes_the_build_side() {
		for join_type in [JoinType::Inner, JoinType::Left, JoinType::Semi, JoinType::Anti] {
			let op: Arc<dyn ExecOperator> = Arc::new(HashJoin::new(
				Arc::new(UnreachableSource),
				ValuesOperator::new(vec![]),
				vec!["b".to_string()],
				join_type,
				vec![],
				None,
			));
			assert!(collect(&op, &root_ctx()).await.is_empty(), "{join_type:?}");
		}
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

	// ---- document rows -------------------------------------------------

	use crate::exec::operators::test_util::{TestDb, physical_expr, val};

	async fn docs(srcs: &[&str]) -> Vec<Value> {
		let mut out = Vec::with_capacity(srcs.len());
		for src in srcs {
			out.push(val(src).await);
		}
		out
	}

	/// A document join of `probe` against `build` on `probe_key = alias.build_key`.
	async fn doc_join(
		ctx: &crate::exec::ExecutionContext,
		build: Vec<Value>,
		probe: Vec<Value>,
		probe_key: &str,
		build_key: &str,
		join_type: JoinType,
		residual: Option<&str>,
	) -> Arc<dyn ExecOperator> {
		let residual = match residual {
			Some(src) => Some(physical_expr(src, ctx).await),
			None => None,
		};
		Arc::new(HashJoin::documents(
			ValuesOperator::new(build),
			ValuesOperator::new(probe),
			vec![physical_expr(probe_key, ctx).await],
			vec![physical_expr(build_key, ctx).await],
			join_type,
			"_join0".to_string(),
			residual,
			None,
		))
	}

	fn ids(rows: &[Value]) -> Vec<Value> {
		rows.iter()
			.map(|row| match row {
				Value::Object(o) => o.get("id").cloned().unwrap_or(Value::None),
				_ => Value::None,
			})
			.collect()
	}

	#[tokio::test]
	async fn a_document_semi_join_keeps_each_probe_row_once_and_unchanged() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let build = docs(&["{ id: 1, k: 'a' }", "{ id: 2, k: 'a' }", "{ id: 3, k: 'b' }"]).await;
		let probe = docs(&["{ id: 10, x: 'a' }", "{ id: 11, x: 'c' }", "{ id: 12, x: 'b' }"]).await;
		let op = doc_join(&ctx, build, probe.clone(), "x", "_join0.k", JoinType::Semi, None).await;
		let out = collect(&op, &ctx).await;
		// Two build rows share 'a'; the probe row is emitted once, as it was.
		assert_eq!(out, vec![probe[0].clone(), probe[2].clone()]);
	}

	#[tokio::test]
	async fn a_document_anti_join_keeps_the_probe_rows_with_no_match() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let build = docs(&["{ k: 'a' }"]).await;
		let probe = docs(&["{ id: 10, x: 'a' }", "{ id: 11, x: 'c' }"]).await;
		let op = doc_join(&ctx, build, probe.clone(), "x", "_join0.k", JoinType::Anti, None).await;
		assert_eq!(collect(&op, &ctx).await, vec![probe[1].clone()]);
	}

	#[tokio::test]
	async fn document_keys_match_none_to_none_and_null_to_null() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		// `IN` compares elements with `=`: NONE equals NONE, NULL equals
		// NULL, and neither equals the other.
		let build = docs(&["{ id: 1, k: NULL }", "{ id: 2 }"]).await;
		let probe = docs(&["{ id: 10, x: NULL }", "{ id: 11 }", "{ id: 12, x: 1 }"]).await;
		let op = doc_join(&ctx, build, probe, "x", "_join0.k", JoinType::Inner, None).await;
		let out = collect(&op, &ctx).await;
		assert_eq!(ids(&out), vec![Value::from(10), Value::from(11)]);
		let Value::Object(first) = &out[0] else {
			panic!("expected an object");
		};
		assert_eq!(first.get("_join0"), Some(&val("{ id: 1, k: NULL }").await));
	}

	#[tokio::test]
	async fn a_regex_key_matches_the_strings_it_accepts() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let build = docs(&["{ id: 1, k: /^a/ }", "{ id: 2, k: 'b' }"]).await;
		let probe =
			docs(&["{ id: 10, x: 'apple' }", "{ id: 11, x: 'b' }", "{ id: 12, x: /^b/ }"]).await;
		let op = doc_join(&ctx, build, probe, "x", "_join0.k", JoinType::Semi, None).await;
		// 'apple' matches the regex build key; 'b' its equal; the regex
		// probe key matches the string build key.
		assert_eq!(
			ids(&collect(&op, &ctx).await),
			vec![Value::from(10), Value::from(11), Value::from(12)]
		);
	}

	#[tokio::test]
	async fn a_document_left_join_adds_the_match_under_the_alias_or_null() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let build = docs(&["{ id: 1, k: 'a', v: 'A' }"]).await;
		let probe = docs(&["{ id: 10, x: 'a' }", "{ id: 11, x: 'z' }"]).await;
		let op = doc_join(&ctx, build, probe, "x", "_join0.k", JoinType::Left, None).await;
		let out = collect(&op, &ctx).await;
		assert_eq!(out[0], val("{ id: 10, x: 'a', _join0: { id: 1, k: 'a', v: 'A' } }").await);
		assert_eq!(out[1], val("{ id: 11, x: 'z', _join0: NULL }").await);
	}

	#[tokio::test]
	async fn a_document_residual_reads_both_rows_through_the_alias() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let build = docs(&["{ k: 'a', n: 1 }", "{ k: 'a', n: 5 }"]).await;
		let probe = docs(&["{ id: 10, x: 'a', m: 3 }", "{ id: 11, x: 'a', m: 9 }"]).await;
		let op =
			doc_join(&ctx, build, probe, "x", "_join0.k", JoinType::Semi, Some("_join0.n > m"))
				.await;
		// Only the first probe row has a build row with n above its m.
		assert_eq!(ids(&collect(&op, &ctx).await), vec![Value::from(10)]);
	}

	#[tokio::test]
	async fn a_document_join_renders_its_keys_and_hides_them_for_a_checked_caller() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let open = HashJoin::documents(
			ValuesOperator::new(Vec::new()),
			ValuesOperator::new(Vec::new()),
			vec![physical_expr("x", &ctx).await],
			vec![physical_expr("_join0.k", &ctx).await],
			JoinType::Semi,
			"_join0".to_string(),
			None,
			None,
		);
		assert!(open.attrs().iter().any(|(k, v)| k == "on" && v == "x = _join0.k"));
		assert_eq!(open.expressions().len(), 2);
		assert_eq!(open.explain_children().len(), 2);
		let framed = HashJoin::documents(
			ValuesOperator::new(Vec::new()),
			ValuesOperator::new(Vec::new()),
			vec![physical_expr("x", &ctx).await],
			vec![physical_expr("_join0.k", &ctx).await],
			JoinType::Semi,
			"_join0".to_string(),
			None,
			Some(JoinFrame::new(None, true)),
		);
		assert!(framed.attrs().iter().any(|(k, v)| k == "on" && v == "<permission>"));
		assert!(framed.attrs().iter().any(|(k, v)| k == "frame" && v == "permission"));
		assert!(framed.expressions().is_empty());
		// Only the probe side is shown; every child is still instrumented.
		assert_eq!(framed.explain_children().len(), 1);
		assert_eq!(framed.children().len(), 2);
	}
}
