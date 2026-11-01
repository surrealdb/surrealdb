//! The `EXPLAIN` tree of the VM, and its one renderer.
//!
//! The lowering of `phys.explain` derives a statement's tree from its physical plan when the
//! query arrives ([`PlanTree`]): plain data, holding every node's name, context, attributes,
//! children and redaction flags, and the bitmap trees an `EXPLAIN ANALYZE` measures
//! ([`MeasureSpec`]). [`render`] turns a tree into the value `EXPLAIN` answers. Plain `EXPLAIN`
//! renders it once, at lowering, with no run. `EXPLAIN ANALYZE` renders the same tree after the
//! statement has run ([`analysed_run`]), overlaying what the run counted ([`RunCounters`]); the VM
//! derives nothing and compiles nothing while it does.
use std::fmt::Write;
use std::sync::Arc;

use surrealdb_strand::TableName;

use super::plain::ValueData;
use crate::exec::FlowResult;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::index::access_path::{BitmapPlan, IndexRef};
use crate::exec::metrics::OperatorMetrics;
use crate::exec::operators::scan::bitmap::{evaluate_fusion, knn_allow_list};
use crate::exec::operators::scan::{BitmapTally, FusedCandidates, PrefilterTier};
use crate::expr::ExplainFormat;
use crate::val::{Array, Object, Value};

/// Number of spaces used per indentation level in text plan output.
const INDENT_WIDTH: usize = 4;

/// The tree `EXPLAIN` renders for a statement, derived from its physical plan when the query
/// arrives.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlanTree {
	pub(crate) root: PlanNode,
	/// The bitmap trees `EXPLAIN ANALYZE` evaluates again after the run, indexed by
	/// [`PlanNode::measure`].
	pub(crate) measures: Vec<MeasureSpec>,
}

assert_plain!(PlanTree);

/// A node of a [`PlanTree`], representing a step of a planned statement.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlanNode {
	pub(crate) name: String,
	pub(crate) context: ContextLevel,
	pub(crate) attrs: Vec<(String, String)>,
	pub(crate) children: Vec<PlanNode>,
	/// Whether every access path beneath renders as `<permission>`. Applies to descendants only.
	pub(crate) redacts_access_paths: bool,
	/// Whether `EXPLAIN ANALYZE` withholds the metrics beneath. Applies to descendants only.
	pub(crate) hides_metrics: bool,
	/// The measured bitmap tree that is this node's one child, as an index of
	/// [`PlanTree::measures`].
	pub(crate) measure: Option<u16>,
}

impl PlanNode {
	pub(crate) fn new(
		name: &str,
		context: ContextLevel,
		attrs: Vec<(String, String)>,
		children: Vec<PlanNode>,
	) -> Self {
		Self {
			name: name.to_owned(),
			context,
			attrs,
			children,
			redacts_access_paths: false,
			hides_metrics: false,
			measure: None,
		}
	}
}

/// What one run of an analysed statement measured, which [`render`] overlays on the tree.
pub(in crate::exec::vm) struct RunCounters<'a> {
	/// The one set of metrics every node reports.
	pub(in crate::exec::vm) metrics: &'a OperatorMetrics,
	/// The outcome of each measured bitmap tree, parallel to [`PlanTree::measures`].
	pub(in crate::exec::vm) measured: &'a [Measured],
	/// Whether volatile counters are left out.
	pub(in crate::exec::vm) redact: bool,
	pub(in crate::exec::vm) total_rows: u64,
}

/// The outcome of evaluating one [`MeasureSpec`] again after the run.
pub(in crate::exec::vm) struct Measured {
	/// The cardinality of each node, shaped as the spec's plan.
	tally: BitmapTally,
	/// For an allow-list: the [`PrefilterTier`] code it chose, and the width of the walk the
	/// allow-list gates (`0` = no walk).
	prefilter: Option<(u8, u32)>,
	/// For a fused plan: whether an overflowing branch made it answer with its fallback.
	fallback: bool,
}

/// What the rendering withholds of the operators beneath a node that
/// enforces a definer's declaration on the caller the plan was compiled for;
/// see [`PlanNode::hides_metrics`] and
/// [`PlanNode::redacts_access_paths`]. Accumulated top-down: once
/// a node withholds something, every operator beneath it — children and the
/// plans embedded in its expressions alike — renders without it.
#[derive(Clone, Copy, Default)]
struct Redaction {
	/// No operator renders its metrics.
	metrics: bool,
	/// Every attribute naming an access path renders as `<permission>`.
	access_paths: bool,
}

/// The attributes that print what an access path reads at or a scan applies:
/// the index and the bound an index scan reads, a record id looked up, a
/// full-text query, the predicate a scan applies per row, and the pinned
/// components, range and range count of a key-range scan. Beneath a
/// permission node whose predicate drove the access path these are the
/// definer's, so a checked caller sees `<permission>` in their place.
const ACCESS_PATH_ATTRS: &[&str] =
	&["index", "access", "record_id", "query", "predicate", "key", "range", "ranges"];

impl Redaction {
	/// The redaction in force beneath `node`.
	fn beneath(self, node: &PlanNode) -> Self {
		Self {
			metrics: self.metrics || node.hides_metrics,
			access_paths: self.access_paths || node.redacts_access_paths,
		}
	}

	/// `attrs` as this redaction renders them.
	fn attrs(self, attrs: Vec<(String, String)>) -> Vec<(String, String)> {
		if !self.access_paths {
			return attrs;
		}
		attrs
			.into_iter()
			.map(|(key, value)| {
				if ACCESS_PATH_ATTRS.contains(&key.as_str()) {
					(key, "<permission>".to_string())
				} else {
					(key, value)
				}
			})
			.collect()
	}
}

// =========================================================================
// Rendering
// =========================================================================

/// The attributes of `node` as rendered: its own, then what the run measured for it.
fn shown_attrs(
	node: &PlanNode,
	redaction: Redaction,
	run: Option<&RunCounters<'_>>,
) -> Vec<(String, String)> {
	let mut attrs = node.attrs.clone();
	if let Some(run) = run
		&& let Some(at) = node.measure
		&& let Some(measured) = run.measured.get(usize::from(at))
	{
		if let Some((code, ef)) = measured.prefilter
			&& let Some(label) = PrefilterTier::label(code)
		{
			attrs.push(("prefilter_tier".to_string(), label.to_string()));
			// The width of a walk the allow-list gates, where the triage chose one.
			if ef > 0 {
				attrs.push(("prefilter_ef".to_string(), ef.to_string()));
			}
		}
		if measured.fallback {
			attrs.push(("union_tier".to_string(), "fallback".to_string()));
		}
	}
	redaction.attrs(attrs)
}

/// The tally of the `at`th child of `node`: the root of the measured bitmap tree when `node`
/// stands over one, else the matching operand of `tally`.
fn child_tally<'a>(
	node: &PlanNode,
	tally: Option<&'a BitmapTally>,
	at: usize,
	run: Option<&'a RunCounters<'_>>,
) -> Option<&'a BitmapTally> {
	match (run, node.measure) {
		(Some(run), Some(measure)) => run.measured.get(usize::from(measure)).map(|m| &m.tally),
		_ => tally.and_then(|t| t.operand(at)),
	}
}

/// The metrics `node` reports under `run`: its tally's, where it has one, else the run's.
fn reported<'a>(tally: Option<&'a BitmapTally>, run: &'a RunCounters<'_>) -> &'a OperatorMetrics {
	tally.map_or(run.metrics, |t| &t.metrics)
}

/// Writes `node`, and every node beneath it, as a text tree.
fn text(
	node: &PlanNode,
	tally: Option<&BitmapTally>,
	out: &mut String,
	prefix: &str,
	redaction: Redaction,
	run: Option<&RunCounters<'_>>,
) {
	let _ = write!(out, "{} [ctx: {}]", node.name, node.context.short_name());
	let attrs = shown_attrs(node, redaction, run);
	if !attrs.is_empty() {
		let _ = write!(out, " [");
		for (i, (key, value)) in attrs.iter().enumerate() {
			if i > 0 {
				let _ = write!(out, ", ");
			}
			let _ = write!(out, "{key}: {value}");
		}
		let _ = write!(out, "]");
	}
	if let Some(run) = run
		&& !redaction.metrics
	{
		let _ = write!(out, " {{{}}}", format_metrics_text(reported(tally, run), run.redact));
	}
	let _ = writeln!(out);
	let beneath = redaction.beneath(node);
	if !node.children.is_empty() {
		let child_prefix = format!("{}{:width$}", prefix, "", width = INDENT_WIDTH);
		for (at, child) in node.children.iter().enumerate() {
			let _ = write!(out, "{}", child_prefix);
			text(child, child_tally(node, tally, at, run), out, &child_prefix, beneath, run);
		}
	}
}

/// `node`, and every node beneath it, as a JSON object.
fn json(
	node: &PlanNode,
	tally: Option<&BitmapTally>,
	redaction: Redaction,
	run: Option<&RunCounters<'_>>,
) -> Object {
	let mut obj = Object::default();
	obj.insert("operator", Value::String(node.name.as_str().into()));
	obj.insert("context", Value::String(node.context.short_name().into()));
	let attrs = shown_attrs(node, redaction, run);
	if !attrs.is_empty() {
		let mut attrs_obj = Object::default();
		for (key, value) in attrs {
			attrs_obj.insert(key, Value::String(value.into()));
		}
		obj.insert("attributes", Value::Object(attrs_obj));
	}
	if let Some(run) = run
		&& !redaction.metrics
	{
		obj.insert("metrics", Value::Object(metrics_json(reported(tally, run), run.redact)));
	}
	if !node.children.is_empty() {
		let beneath = redaction.beneath(node);
		let children: Vec<Value> = node
			.children
			.iter()
			.enumerate()
			.map(|(at, child)| {
				Value::Object(json(child, child_tally(node, tally, at, run), beneath, run))
			})
			.collect();
		obj.insert("children", Value::Array(Array::from(children)));
	}
	obj
}

/// The value `EXPLAIN` answers for `tree` in `format`.
///
/// Without a `run` it is a function of the tree alone. With one, every node reports the run's
/// metrics, a measured bitmap tree its tally, and the rendering ends with the rows the statement
/// yielded and what its morsels did.
pub(in crate::exec::vm) fn render(
	tree: &PlanTree,
	format: ExplainFormat,
	run: Option<&RunCounters<'_>>,
) -> Value {
	// Every operator of a VM program reports one set of metrics, so what its morsels did is
	// written once, beneath the plan.
	let morsels = run.filter(|run| !run.redact).and_then(|run| run.metrics.morsels());
	match format {
		ExplainFormat::Text => {
			let mut out = String::new();
			text(&tree.root, None, &mut out, "", Redaction::default(), run);
			if let Some(run) = run {
				let _ = writeln!(out);
				let _ = write!(out, "Total rows: {}", run.total_rows);
				if let Some(morsels) = morsels {
					let _ = write!(out, "\n{}", morsels.render_text());
				}
			}
			Value::String(out.into())
		}
		ExplainFormat::Json => {
			let mut obj = json(&tree.root, None, Redaction::default(), run);
			if let Some(run) = run {
				obj.insert("total_rows", Value::from(run.total_rows as i64));
				if let Some(morsels) = morsels {
					obj.insert("morsels", Value::Object(morsels.render_object()));
				}
			}
			Value::Object(obj)
		}
	}
}

/// The `metrics` object `EXPLAIN ANALYZE` renders in JSON for `metrics`.
fn metrics_json(metrics: &OperatorMetrics, redact_volatile_explain_attrs: bool) -> Object {
	let mut metrics_obj = Object::default();
	metrics_obj.insert("output_rows", Value::from(metrics.output_rows() as i64));
	// Emitted only when a bitmap branch declined, so its `output_rows: 0`
	// is distinguishable from a genuinely empty candidate set.
	// Deterministic for a given dataset and configuration, so never
	// redacted.
	if metrics.branch_dropped() {
		metrics_obj.insert("dropped", Value::Bool(true));
	}
	if !redact_volatile_explain_attrs {
		metrics_obj.insert("output_batches", Value::from(metrics.output_batches() as i64));
		metrics_obj.insert("elapsed_ns", Value::from(metrics.elapsed_ns() as i64));
		// Volatile (progress/publish-timing dependent); rendered only when
		// non-zero. `edges_scanned` is the graph-traversal selectivity
		// denominator (matched = output_rows).
		let scanned = metrics.edges_scanned();
		if scanned > 0 {
			metrics_obj.insert("edges_scanned", Value::from(scanned as i64));
		}
		let block_hits = metrics.block_hits();
		if block_hits > 0 {
			metrics_obj.insert("block_hits", Value::from(block_hits as i64));
		}
		let delta_hits = metrics.delta_hits();
		if delta_hits > 0 {
			metrics_obj.insert("delta_hits", Value::from(delta_hits as i64));
		}
		let cache_hits = metrics.cache_hits();
		if cache_hits > 0 {
			metrics_obj.insert("cache_hits", Value::from(cache_hits as i64));
		}
		let cache_misses = metrics.cache_misses();
		if cache_misses > 0 {
			metrics_obj.insert("cache_misses", Value::from(cache_misses as i64));
		}
		let props_evals = metrics.props_evals();
		if props_evals > 0 {
			metrics_obj.insert("props_evals", Value::from(props_evals as i64));
		}
		let props_fallbacks = metrics.props_fallbacks();
		if props_fallbacks > 0 {
			metrics_obj.insert("props_fallbacks", Value::from(props_fallbacks as i64));
		}
		let skipped = metrics.skipped_rows();
		if skipped > 0 {
			metrics_obj.insert("skipped_rows", Value::from(skipped as i64));
		}
		// Deliberately rendered even at zero — unlike the counters above,
		// `records_fetched: 0` is itself the claim worth asserting: an ANN
		// search whose predicate was fully served by the allow-list bitmap
		// performed no in-traversal record fetches.
		metrics_obj.insert("records_fetched", Value::from(metrics.records_fetched() as i64));
	}
	metrics_obj
}

/// Format metrics as a human-readable string fragment.
///
/// When `redact_volatile_explain_attrs` is true, elapsed time and batch counts are
/// omitted so the output is deterministic for test assertions.
fn format_metrics_text(metrics: &OperatorMetrics, redact_volatile_explain_attrs: bool) -> String {
	let rows = metrics.output_rows();

	// A bitmap branch that declined (budget overflow or a gate) is flagged so
	// its `rows: 0` is distinguishable from a genuinely empty candidate set.
	// Deterministic for a given dataset and configuration, so never redacted.
	let dropped = if metrics.branch_dropped() {
		", dropped: true"
	} else {
		""
	};

	if redact_volatile_explain_attrs {
		return format!("rows: {}{}", rows, dropped);
	}

	let batches = metrics.output_batches();
	let elapsed = metrics.elapsed_ns();

	// Format elapsed time in the most readable unit
	let elapsed_str = if elapsed >= 1_000_000_000 {
		format!("{:.2}s", elapsed as f64 / 1_000_000_000.0)
	} else if elapsed >= 1_000_000 {
		format!("{:.2}ms", elapsed as f64 / 1_000_000.0)
	} else if elapsed >= 1_000 {
		format!("{:.2}µs", elapsed as f64 / 1_000.0)
	} else {
		format!("{}ns", elapsed)
	};

	// Scan-side counters that depend on how far the scan progressed (volatile
	// like batches/elapsed), rendered only when non-zero. `scanned` is the
	// denominator for graph-traversal filter selectivity (matched = rows);
	// `skipped` is the TopK threshold-pushdown reject count; `fetched` is the
	// count of candidate records evaluated inside an ANN search (zero for
	// predicates the index fully covers).
	let mut extra = String::new();
	let scanned = metrics.edges_scanned();
	if scanned > 0 {
		extra.push_str(&format!(", scanned: {}", scanned));
	}
	// The storage-tier split of `scanned`: entries served from packed
	// adjacency blocks vs per-edge pointer keys.
	let block_hits = metrics.block_hits();
	if block_hits > 0 {
		extra.push_str(&format!(", block_hits: {}", block_hits));
	}
	let delta_hits = metrics.delta_hits();
	if delta_hits > 0 {
		extra.push_str(&format!(", delta_hits: {}", delta_hits));
	}
	let cache_hits = metrics.cache_hits();
	if cache_hits > 0 {
		extra.push_str(&format!(", cache_hits: {}", cache_hits));
	}
	let cache_misses = metrics.cache_misses();
	if cache_misses > 0 {
		extra.push_str(&format!(", cache_misses: {}", cache_misses));
	}
	let props_evals = metrics.props_evals();
	if props_evals > 0 {
		extra.push_str(&format!(", props_evals: {}", props_evals));
	}
	let props_fallbacks = metrics.props_fallbacks();
	if props_fallbacks > 0 {
		extra.push_str(&format!(", props_fallbacks: {}", props_fallbacks));
	}
	let skipped = metrics.skipped_rows();
	if skipped > 0 {
		extra.push_str(&format!(", skipped: {}", skipped));
	}
	let fetched = metrics.records_fetched();
	if fetched > 0 {
		extra.push_str(&format!(", fetched: {}", fetched));
	}
	format!("rows: {}, batches: {}, elapsed: {}{}{}", rows, batches, elapsed_str, extra, dropped)
}

/// What a [`Measured`] bitmap tree is, as a program holds it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MeasureSpec {
	pub(in crate::exec::vm) table: TableName,
	pub(in crate::exec::vm) root: ValueData<BitmapPlan>,
	pub(in crate::exec::vm) kind: MeasureKind,
}

/// What a measured bitmap tree decides for the run.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MeasureKind {
	/// The allow-list a KNN search is restricted to, whose size chose the
	/// strategy of the search.
	Prefilter {
		/// Whether the statement reads at a `VERSION`, which no allow-list
		/// serves.
		versioned: bool,
		/// The index searched, and the `ef` the search was asked for.
		index: IndexRef,
		ef: u32,
		/// Whether the exact tier judges a condition on each member it reads,
		/// apart from the table's permission.
		judges_condition: bool,
		/// Whether the table's permission is judged on each candidate, for a
		/// caller whose reads are checked.
		judges_permission: bool,
	},
	/// The doc-IDs a fused plan resolves, or the overflow that made it answer
	/// with its fallback.
	Fusion {
		/// Whether the plan has no fallback, so no branch may overflow.
		anchored: bool,
	},
}

assert_plain!(MeasureSpec);

impl Measured {
	/// Evaluates `spec` again in `ctx`: the bitmap depends only on the transaction the statement
	/// ran in, so this matches what the run saw.
	pub(in crate::exec::vm) async fn run(
		spec: &MeasureSpec,
		ctx: &ExecutionContext,
	) -> FlowResult<Self> {
		let table = &spec.table;
		let tally = BitmapTally::of(&spec.root);
		let (prefilter, fallback) = match &spec.kind {
			MeasureKind::Prefilter {
				versioned,
				index,
				ef,
				judges_condition,
				judges_permission,
			} => {
				let (chosen, walk_ef) = if *versioned {
					(PrefilterTier::Fallback, None)
				} else {
					match knn_allow_list(ctx, table, &spec.root, *ef, Some(&tally)).await? {
						Some(allowed) => {
							let per_row = *judges_condition
								|| (*judges_permission && !ctx.root().skip_fetch_perms);
							let decision = super::knn::decide_prefilter(
								ctx,
								table,
								index.definition(),
								allowed.len(),
								*ef,
								per_row,
							)
							.await?;
							(decision.tier(), decision.walk_ef())
						}
						None => (PrefilterTier::Fallback, None),
					}
				};
				(Some((chosen as u8, walk_ef.unwrap_or(0))), false)
			}
			MeasureKind::Fusion {
				anchored,
			} => {
				let overflowed = matches!(
					evaluate_fusion(ctx, table, &spec.root, &tally, *anchored).await?,
					FusedCandidates::Overflow
				);
				(None, overflowed)
			}
		};
		Ok(Self {
			tally,
			prefilter,
			fallback,
		})
	}
}

/// Runs `run`, the statement `analysis` explains, in `ctx` and answers the
/// rendering of what it measured: the run's counters, the outcome of each
/// measured bitmap tree, and the rows the statement yielded.
///
/// The run's signals end it as they end a drain of its rows; an error is
/// raised.
pub(in crate::exec::vm) async fn analysed_run<F, Fut>(
	analysis: &crate::exec::vm::program::Analysis,
	ctx: &ExecutionContext,
	run: F,
) -> FlowResult<crate::val::Value>
where
	F: FnOnce(ExecutionContext) -> Fut,
	Fut: std::future::Future<Output = (u64, FlowResult<()>)>,
{
	let metrics = Arc::new(OperatorMetrics::new());
	metrics.enable();
	let frame = ctx.with_vm_metrics(Arc::clone(&metrics));
	let started = web_time::Instant::now();
	let (total, ended) = run(frame.clone()).await;
	let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
	match ended {
		Ok(())
		| Err(
			crate::expr::ControlFlow::Break
			| crate::expr::ControlFlow::Continue
			| crate::expr::ControlFlow::Return(_),
		) => {}
		Err(error @ crate::expr::ControlFlow::Err(_)) => return Err(error),
	}
	match total {
		0 => metrics.record_elapsed(elapsed),
		rows => metrics.record_batch(rows, elapsed),
	}
	let mut measured = Vec::with_capacity(analysis.plan.measures.len());
	for spec in &analysis.plan.measures {
		measured.push(Measured::run(spec, &frame).await?);
	}
	Ok(render(
		&analysis.plan,
		analysis.format,
		Some(&RunCounters {
			metrics: &metrics,
			measured: &measured,
			redact: analysis.redact,
			total_rows: total,
		}),
	))
}
