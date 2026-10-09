//! EXPLAIN and EXPLAIN ANALYZE operators.
//!
//! - [`ExplainPlan`] formats a query plan without executing it (read-only).
//! - EXPLAIN ANALYZE runs as the VM program: `VmAnalysis` (`exec/vm/explain.rs`) drains the
//!   program's output to completion and then renders the display tree through
//!   [`ExplainPlan::render_analyzed`] together with the collected [`OperatorMetrics`].

use std::fmt::Write;
use std::sync::Arc;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, OutputShape, ValueBatch,
	ValueBatchStream,
};
use crate::expr::ExplainFormat;
use crate::val::{Array, Object, Value};

/// Number of spaces used per indentation level in text plan output.
const INDENT_WIDTH: usize = 4;

/// EXPLAIN operator - formats an execution plan as text.
///
/// This operator wraps an inner statement's planned content and returns
/// the formatted execution plan as a string value.
#[derive(Debug)]
pub struct ExplainPlan {
	/// The inner statement's planned content
	pub plan: Arc<dyn ExecOperator>,
	/// The output format (currently only Text is supported)
	pub format: ExplainFormat,
}
impl ExplainPlan {
	/// The value `EXPLAIN` answers for `plan` in `format`: a function of the
	/// plan alone, which nothing it runs changes.
	pub(crate) fn render(plan: &dyn ExecOperator, format: ExplainFormat) -> Value {
		match format {
			ExplainFormat::Text => {
				let mut plan_text = String::new();
				format_execution_plan(plan, &mut plan_text, "", Redaction::default());
				Value::String(plan_text.into())
			}
			ExplainFormat::Json => {
				Value::Object(format_execution_plan_json(plan, Redaction::default()))
			}
		}
	}
}

impl ExecOperator for ExplainPlan {
	fn name(&self) -> &'static str {
		"Explain"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		match self.format {
			ExplainFormat::Text => vec![("format".to_string(), "TEXT".to_string())],
			ExplainFormat::Json => vec![("format".to_string(), "JSON".to_string())],
		}
	}

	fn required_context(&self) -> ContextLevel {
		// EXPLAIN doesn't need database context - it just formats the plan
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		// EXPLAIN is always read-only - it doesn't execute the inner statement
		AccessMode::ReadOnly
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let output = Self::render(self.plan.as_ref(), self.format);
		Ok(Box::pin(futures::stream::once(std::future::ready(Ok(ValueBatch::new(vec![output]))))))
	}

	fn output_shape(&self) -> OutputShape {
		// EXPLAIN returns a single scalar value (text or JSON)
		OutputShape::Scalar
	}
}

// =========================================================================
// Redaction beneath a permission node
// =========================================================================

/// What the rendering withholds of the operators beneath a node that
/// enforces a definer's declaration on the caller the plan was compiled for;
/// see [`ExecOperator::hides_metrics_beneath`] and
/// [`ExecOperator::redacts_access_paths_beneath`]. Accumulated top-down: once
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
/// full-text query, the predicate a scan applies per row. Beneath a
/// permission node whose predicate drove the access path these are the
/// definer's, so a checked caller sees `<permission>` in their place.
const ACCESS_PATH_ATTRS: &[&str] = &["index", "access", "record_id", "query", "predicate"];

impl Redaction {
	/// The redaction in force beneath `plan`.
	fn beneath(self, plan: &dyn ExecOperator) -> Self {
		Self {
			metrics: self.metrics || plan.hides_metrics_beneath(),
			access_paths: self.access_paths || plan.redacts_access_paths_beneath(),
		}
	}

	/// `plan`'s attributes as this redaction renders them.
	fn attrs(self, plan: &dyn ExecOperator) -> Vec<(String, String)> {
		let attrs = plan.attrs();
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
// Text Formatting
// =========================================================================

/// Format an execution plan node as a text tree
fn format_execution_plan(
	plan: &dyn ExecOperator,
	output: &mut String,
	prefix: &str,
	redaction: Redaction,
) {
	// Get operator name and properties
	let name = plan.name();
	let properties = redaction.attrs(plan);
	let beneath = redaction.beneath(plan);

	// Show context level
	let context = plan.required_context();
	let _ = write!(output, "{} [ctx: {}]", name, context.short_name());

	// Show the output partitioning only when the operator produces more than one
	// stream, so a plan built entirely from single-partition operators renders
	// exactly as it did before partitioning existed.
	let partitioning = plan.output_partitioning();
	if !partitioning.is_single() {
		let _ = write!(output, " [partitions: {}]", partitioning.count());
	}

	// Show properties if any
	if !properties.is_empty() {
		let _ = write!(output, " [");
		for (i, (key, value)) in properties.iter().enumerate() {
			if i > 0 {
				let _ = write!(output, ", ");
			}
			let _ = write!(output, "{key}: {value}");
		}
		let _ = write!(output, "]");
	}

	let _ = writeln!(output);

	// Format children with indentation
	let children = plan.explain_children();
	if !children.is_empty() {
		let child_prefix = format!("{}{:width$}", prefix, "", width = INDENT_WIDTH);
		for child in children.iter() {
			let _ = write!(output, "{}", child_prefix);
			format_execution_plan(child.as_ref(), output, &child_prefix, beneath);
		}
	}
}

// =========================================================================
// JSON Formatting
// =========================================================================

/// Format an execution plan node as a JSON object
fn format_execution_plan_json(plan: &dyn ExecOperator, redaction: Redaction) -> Object {
	let mut obj = Object::default();
	let beneath = redaction.beneath(plan);

	obj.insert("operator", Value::String(plan.name().into()));

	obj.insert("context", Value::String(plan.required_context().short_name().into()));

	// Emitted only when the operator produces more than one stream, so a plan of
	// single-partition operators renders the same keys it always did.
	let partitioning = plan.output_partitioning();
	if !partitioning.is_single() {
		obj.insert("partitions", Value::from(partitioning.count() as i64));
	}

	let attrs = redaction.attrs(plan);
	if !attrs.is_empty() {
		let mut attrs_obj = Object::default();
		for (key, value) in attrs {
			attrs_obj.insert(key, Value::String(value.into()));
		}
		obj.insert("attributes", Value::Object(attrs_obj));
	}

	let children = plan.explain_children();
	if !children.is_empty() {
		let children_array: Vec<Value> = children
			.iter()
			.map(|child| Value::Object(format_execution_plan_json(child.as_ref(), beneath)))
			.collect();
		obj.insert("children", Value::Array(Array::from(children_array)));
	}

	obj
}

// =========================================================================
// ANALYZE Formatters (include metrics)
// =========================================================================

/// The text `EXPLAIN ANALYZE` renders of `plan` cut where it writes metrics,
/// for a plan whose operators all report one set of metrics: the rendering
/// is the pieces joined by those metrics ([`render_analyze_text`]). `None`
/// where two operators report different metrics.
pub(crate) fn analyze_text_pieces(plan: &dyn ExecOperator) -> Option<Vec<String>> {
	let mut pieces = Vec::new();
	let mut reported: Option<*const OperatorMetrics> = None;
	let mut shared = true;
	let mut output = String::new();
	format_analyze_plan_by(plan, &mut output, "", Redaction::default(), &mut |metrics, output| {
		let at = std::ptr::from_ref(metrics);
		shared &= *reported.get_or_insert(at) == at;
		pieces.push(std::mem::take(output));
	});
	pieces.push(output);
	shared.then_some(pieces)
}

/// The value `EXPLAIN ANALYZE` answers in text for the plan
/// [`analyze_text_pieces`] cut into `pieces`, whose operators reported
/// `metrics`, after it yielded `total_rows` rows.
pub(crate) fn render_analyze_text(
	pieces: &[String],
	metrics: &OperatorMetrics,
	redact_volatile_explain_attrs: bool,
	total_rows: u64,
) -> Value {
	let reported = format!(" {{{}}}", format_metrics_text(metrics, redact_volatile_explain_attrs));
	let mut plan_text = pieces.join(&reported);
	let _ = writeln!(plan_text);
	let _ = write!(plan_text, "Total rows: {}", total_rows);
	Value::String(plan_text.into())
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
		// performed no in-traversal record fetches (#548).
		metrics_obj.insert("records_fetched", Value::from(metrics.records_fetched() as i64));
	}
	metrics_obj
}

/// The JSON `EXPLAIN ANALYZE` renders for `plan`, every operator of which
/// reports the one set of metrics a run records, with that set's `metrics`
/// objects to be filled when the run ends ([`render_analyze_json`]); `None`
/// where operators report metrics of their own.
pub(crate) fn analyze_json_template(plan: &dyn ExecOperator) -> Option<Object> {
	analyze_text_pieces(plan)?;
	Some(format_analyze_plan_json(plan, false, Redaction::default()))
}

/// The value `EXPLAIN ANALYZE` answers in JSON for the plan rendered as
/// `template` ([`analyze_json_template`]), whose operators reported
/// `metrics`, after it yielded `total_rows` rows.
pub(crate) fn render_analyze_json(
	template: &Object,
	metrics: &OperatorMetrics,
	redact_volatile_explain_attrs: bool,
	total_rows: u64,
) -> Value {
	fn fill(value: &mut Value, metrics: &Object) {
		match value {
			Value::Object(object) => {
				for (key, inner) in object.iter_mut() {
					if key == "metrics" && matches!(inner, Value::Object(_)) {
						*inner = Value::Object(metrics.clone());
					} else {
						fill(inner, metrics);
					}
				}
			}
			Value::Array(items) => {
				for item in items.iter_mut() {
					fill(item, metrics);
				}
			}
			_ => {}
		}
	}
	let metrics = metrics_json(metrics, redact_volatile_explain_attrs);
	let mut plan = Value::Object(template.clone());
	fill(&mut plan, &metrics);
	if let Value::Object(object) = &mut plan {
		object.insert("total_rows", Value::from(total_rows as i64));
	}
	plan
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
	// count of candidate records evaluated inside an ANN search (#548 drives
	// it to zero for index-covered predicates).
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

/// Format an execution plan node as a text tree with metrics.
fn format_analyze_plan(
	plan: &dyn ExecOperator,
	output: &mut String,
	prefix: &str,
	redact_volatile_explain_attrs: bool,
	redaction: Redaction,
) {
	format_analyze_plan_by(plan, output, prefix, redaction, &mut |metrics, output| {
		let _ =
			write!(output, " {{{}}}", format_metrics_text(metrics, redact_volatile_explain_attrs));
	});
}

/// [`format_analyze_plan`], with each operator's metrics written by
/// `write_metrics` where the rendering shows them.
fn format_analyze_plan_by(
	plan: &dyn ExecOperator,
	output: &mut String,
	prefix: &str,
	redaction: Redaction,
	write_metrics: &mut dyn FnMut(&OperatorMetrics, &mut String),
) {
	let name = plan.name();
	let properties = redaction.attrs(plan);
	let beneath = redaction.beneath(plan);

	// Show context level
	let context = plan.required_context();
	let _ = write!(output, "{} [ctx: {}]", name, context.short_name());

	// Show the output partitioning only when the operator produces more than one
	// stream, so a plan built entirely from single-partition operators renders
	// exactly as it did before partitioning existed.
	let partitioning = plan.output_partitioning();
	if !partitioning.is_single() {
		let _ = write!(output, " [partitions: {}]", partitioning.count());
	}

	// Show properties if any
	if !properties.is_empty() {
		let _ = write!(output, " [");
		for (i, (key, value)) in properties.iter().enumerate() {
			if i > 0 {
				let _ = write!(output, ", ");
			}
			let _ = write!(output, "{key}: {value}");
		}
		let _ = write!(output, "]");
	}

	// Show metrics if available, and not withheld beneath a permission node.
	if !redaction.metrics
		&& let Some(metrics) = plan.metrics()
	{
		write_metrics(metrics, output);
	}

	let _ = writeln!(output);

	// Format children with indentation
	let children = plan.explain_children();
	if !children.is_empty() {
		let child_prefix = format!("{}{:width$}", prefix, "", width = INDENT_WIDTH);
		for child in children.iter() {
			let _ = write!(output, "{}", child_prefix);
			format_analyze_plan_by(child.as_ref(), output, &child_prefix, beneath, write_metrics);
		}
	}
}

/// Format an execution plan node as a JSON object with metrics.
fn format_analyze_plan_json(
	plan: &dyn ExecOperator,
	redact_volatile_explain_attrs: bool,
	redaction: Redaction,
) -> Object {
	let mut obj = Object::default();
	let beneath = redaction.beneath(plan);

	obj.insert("operator", Value::String(plan.name().into()));

	obj.insert("context", Value::String(plan.required_context().short_name().into()));

	// Emitted only when the operator produces more than one stream, so a plan of
	// single-partition operators renders the same keys it always did.
	let partitioning = plan.output_partitioning();
	if !partitioning.is_single() {
		obj.insert("partitions", Value::from(partitioning.count() as i64));
	}

	let attrs = redaction.attrs(plan);
	if !attrs.is_empty() {
		let mut attrs_obj = Object::default();
		for (key, value) in attrs {
			attrs_obj.insert(key, Value::String(value.into()));
		}
		obj.insert("attributes", Value::Object(attrs_obj));
	}

	// The metrics, unless withheld beneath a permission node.
	if !redaction.metrics
		&& let Some(metrics) = plan.metrics()
	{
		obj.insert("metrics", Value::Object(metrics_json(metrics, redact_volatile_explain_attrs)));
	}

	// Add children if any
	let children = plan.explain_children();
	if !children.is_empty() {
		let children_array: Vec<Value> = children
			.iter()
			.map(|child| {
				Value::Object(format_analyze_plan_json(
					child.as_ref(),
					redact_volatile_explain_attrs,
					beneath,
				))
			})
			.collect();
		obj.insert("children", Value::Array(Array::from(children_array)));
	}

	obj
}

impl ExplainPlan {
	/// Render a plan's completed counters with the same permission redaction as EXPLAIN.
	pub(crate) fn render_analyzed(
		plan: &dyn ExecOperator,
		format: ExplainFormat,
		redact_volatile_explain_attrs: bool,
		total_rows: u64,
	) -> Value {
		match format {
			ExplainFormat::Text => {
				let mut plan_text = String::new();
				format_analyze_plan(
					plan,
					&mut plan_text,
					"",
					redact_volatile_explain_attrs,
					Redaction::default(),
				);
				let _ = writeln!(plan_text);
				let _ = write!(plan_text, "Total rows: {}", total_rows);
				Value::String(plan_text.into())
			}
			ExplainFormat::Json => {
				let mut plan_json = format_analyze_plan_json(
					plan,
					redact_volatile_explain_attrs,
					Redaction::default(),
				);
				plan_json.insert("total_rows", Value::from(total_rows as i64));
				Value::Object(plan_json)
			}
		}
	}
}
