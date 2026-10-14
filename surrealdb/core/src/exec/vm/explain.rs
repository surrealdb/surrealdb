//! The `EXPLAIN` display tree of the VM.
//!
//! The lowering of `phys.explain` renders a statement's container into inert
//! display nodes ([`ExplainNode`]); no display node executes an expression or
//! a statement. A run under `EXPLAIN ANALYZE` reports its counters through
//! them ([`VmAnalysis`]).
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering};

use futures::StreamExt;
use surrealdb_strand::TableName;

use super::plain::ValueData;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::index::access_path::{BitmapPlan, IndexRef};
use crate::exec::metrics::OperatorMetrics;
use crate::exec::operators::scan::bitmap::evaluate_fusion;
use crate::exec::operators::scan::{BitmapNode, FusedCandidates, PrefilterTier};
use crate::exec::planner::bitmap_plan_to_node;
use crate::exec::{AccessMode, ExecOperator, FlowResult, ValueBatch, ValueBatchStream};

/// A node of the `EXPLAIN` display tree, representing a step of a planned
/// statement.
#[derive(Debug)]
pub(crate) struct ExplainNode {
	pub(crate) name: &'static str,
	pub(crate) context: ContextLevel,
	pub(crate) attrs: Vec<(String, String)>,
	pub(crate) children: Vec<Arc<dyn ExecOperator>>,
	pub(crate) metrics: Option<Arc<OperatorMetrics>>,
	/// For a KNN scan under `EXPLAIN ANALYZE`: what its prefilter's allow-list
	/// chose, once [`Self::measured`] has been evaluated.
	pub(in crate::exec::vm) tier: Option<Arc<PrefilterChoice>>,
	/// For a bitmap scan under `EXPLAIN ANALYZE`: whether an overflowing
	/// branch made it answer with its fallback.
	pub(in crate::exec::vm) fallback: Option<Arc<AtomicBool>>,
	/// For the root of an analysed plan: the bitmap trees it evaluates after
	/// the statement has run.
	pub(in crate::exec::vm) measured: Vec<Arc<Measured>>,
	/// For a node whose one child is a measured bitmap tree: that tree, as an
	/// index of the root's [`Self::measured`].
	pub(in crate::exec::vm) measure: Option<u16>,
	/// Whether every access path beneath renders as `<permission>`; see
	/// [`ExecOperator::redacts_access_paths_beneath`].
	pub(in crate::exec::vm) redacts_access_paths: bool,
	/// Whether `EXPLAIN ANALYZE` withholds the metrics beneath; see
	/// [`ExecOperator::hides_metrics_beneath`].
	pub(in crate::exec::vm) hides_metrics: bool,
}

impl ExplainNode {
	pub(in crate::exec::vm) fn new(
		name: &'static str,
		context: ContextLevel,
		attrs: Vec<(String, String)>,
		children: Vec<ExplainChild>,
		metrics: Option<&Arc<OperatorMetrics>>,
	) -> Self {
		Self {
			name,
			context,
			attrs,
			children: children.into_iter().map(|child| child.0).collect(),
			metrics: metrics.cloned(),
			tier: None,
			fallback: None,
			measured: Vec::new(),
			measure: None,
			redacts_access_paths: false,
			hides_metrics: false,
		}
	}

	/// The context the first child of this node requires, if it has one.
	pub(in crate::exec::vm) fn first_child_context(&self) -> Option<ContextLevel> {
		self.children.first().map(|child| child.required_context())
	}

	pub(in crate::exec::vm) fn shared(self) -> ExplainChild {
		ExplainChild(Arc::new(self))
	}
}

/// A child of a display node: a node of the tree, or a bitmap tree a scan reads, which displays
/// itself.
#[derive(Clone, Debug)]
pub(crate) struct ExplainChild(Arc<dyn ExecOperator>);

impl ExplainChild {
	/// The bitmap tree `bitmap`, displayed by its own nodes.
	pub(in crate::exec::vm) fn bitmap(bitmap: Arc<BitmapNode>) -> Self {
		Self(bitmap)
	}

	/// The operator that displays this child.
	pub(in crate::exec::vm) fn into_operator(self) -> Arc<dyn ExecOperator> {
		self.0
	}

	/// This child, and every node beneath it, as display nodes that report `metrics`.
	pub(in crate::exec::vm) fn described(
		&self,
		metrics: Option<&Arc<OperatorMetrics>>,
	) -> ExplainNode {
		described(self.0.as_ref(), metrics)
	}
}

/// `operator`, and every operator beneath it, as display nodes that report `metrics`.
fn described(operator: &dyn ExecOperator, metrics: Option<&Arc<OperatorMetrics>>) -> ExplainNode {
	let children = operator
		.explain_children()
		.into_iter()
		.map(|child| described(child.as_ref(), metrics).shared())
		.collect();
	ExplainNode::new(
		operator.name(),
		operator.required_context(),
		operator.attrs(),
		children,
		metrics,
	)
}

impl ExecOperator for ExplainNode {
	fn name(&self) -> &'static str {
		self.name
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = self.attrs.clone();
		if let Some(choice) = self.tier.as_ref()
			&& let Some(label) = PrefilterTier::label(choice.tier.load(Ordering::Relaxed))
		{
			attrs.push(("prefilter_tier".to_string(), label.to_string()));
			// The width of a walk the allow-list gates, where the triage chose
			// one.
			let ef = choice.walk_ef.load(Ordering::Relaxed);
			if ef > 0 {
				attrs.push(("prefilter_ef".to_string(), ef.to_string()));
			}
		}
		if self.fallback.as_ref().is_some_and(|taken| taken.load(Ordering::Relaxed)) {
			attrs.push(("union_tier".to_string(), "fallback".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		self.context
	}

	fn redacts_access_paths_beneath(&self) -> bool {
		self.redacts_access_paths
	}

	fn hides_metrics_beneath(&self) -> bool {
		self.hides_metrics
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.children.iter().collect()
	}

	fn explain_children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.children.iter().collect()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		self.metrics.as_deref()
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		Ok(Box::pin(futures::stream::empty()))
	}
}

/// What a KNN scan's prefilter chose under `EXPLAIN ANALYZE`: the tier as a
/// [`PrefilterTier`] code (`0` = not measured), and the width of the walk the
/// allow-list gates (`0` = no walk).
#[derive(Debug, Default)]
pub(in crate::exec::vm) struct PrefilterChoice {
	tier: AtomicU8,
	walk_ef: AtomicU32,
}

/// A bitmap tree of the plan, evaluated again for the analysed plan: its nodes
/// record the cardinality they produce, and the outcome of the evaluation
/// tells which strategy the run took.
///
/// A bitmap depends only on the transaction the statement ran in, so this
/// evaluation matches what the run saw. The spec is plain data a program
/// holds; the cells are where one evaluation writes its outcome.
#[derive(Debug)]
pub(in crate::exec::vm) struct Measured {
	pub(in crate::exec::vm) spec: MeasureSpec,
	pub(in crate::exec::vm) bitmap: Arc<BitmapNode>,
	/// For an allow-list: what it chose.
	pub(in crate::exec::vm) tier: Option<Arc<PrefilterChoice>>,
	/// For a fused plan: whether it answered with its fallback.
	pub(in crate::exec::vm) fallback: Option<Arc<AtomicBool>>,
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
	/// The tree `spec` states, with fresh cells for its outcome.
	pub(in crate::exec::vm) fn of(spec: MeasureSpec) -> Self {
		let bitmap = bitmap_plan_to_node((*spec.root).clone());
		let (tier, fallback) = match &spec.kind {
			MeasureKind::Prefilter {
				..
			} => (Some(Arc::new(PrefilterChoice::default())), None),
			MeasureKind::Fusion {
				..
			} => (None, Some(Arc::new(AtomicBool::new(false)))),
		};
		Self {
			spec,
			bitmap,
			tier,
			fallback,
		}
	}

	async fn measure(&self, ctx: &ExecutionContext) -> FlowResult<()> {
		let table = &self.spec.table;
		match &self.spec.kind {
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
					match self
						.bitmap
						.build_allowlist(ctx, table, *surrealdb_cnf::BITMAP_BRANCH_BUDGET)
						.await?
					{
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
				if let Some(tier) = &self.tier {
					tier.tier.store(chosen as u8, Ordering::Relaxed);
					tier.walk_ef.store(walk_ef.unwrap_or(0), Ordering::Relaxed);
				}
			}
			MeasureKind::Fusion {
				anchored,
			} => {
				if matches!(
					evaluate_fusion(ctx, table, &self.bitmap, *anchored).await?,
					FusedCandidates::Overflow
				) && let Some(fallback) = &self.fallback
				{
					fallback.store(true, Ordering::Relaxed);
				}
			}
		}
		Ok(())
	}
}

/// The display tree `EXPLAIN ANALYZE` renders, as a program holds it: the
/// nodes of the plan, and the bitmap trees measured after the statement
/// runs. A run builds its display nodes from it ([`Self::instantiate`]),
/// every node but a measured bitmap's reporting the one set of metrics the
/// run records.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AnalysedPlan {
	root: AnalysedNode,
	measures: Vec<MeasureSpec>,
}

/// A node of an [`AnalysedPlan`].
#[derive(Debug, Clone, PartialEq)]
struct AnalysedNode {
	name: String,
	context: ContextLevel,
	attrs: Vec<(String, String)>,
	children: Vec<AnalysedNode>,
	/// Whether the node reports the run's metrics.
	reports: bool,
	redacts_access_paths: bool,
	hides_metrics: bool,
	/// The measured bitmap tree that is this node's one child, as an index
	/// of [`AnalysedPlan::measures`].
	measure: Option<u16>,
}

assert_plain!(AnalysedPlan);

impl AnalysedPlan {
	/// The plan the display tree `root` shows, whose nodes report its one set
	/// of metrics and whose measured bitmap trees are `root.measured`.
	///
	/// `None` for a node that is not a display node of the walk, beneath a
	/// node no measured tree stands for.
	pub(crate) fn of(root: &ExplainNode) -> Option<Self> {
		Some(Self {
			root: analysed_node(root)?,
			measures: root.measured.iter().map(|measured| measured.spec.clone()).collect(),
		})
	}

	/// The display nodes of this plan, all reporting one fresh set of
	/// metrics, the root holding the measured bitmap trees.
	pub(in crate::exec::vm) fn instantiate(&self) -> Arc<ExplainNode> {
		let metrics = Arc::new(OperatorMetrics::new());
		let measured: Vec<Arc<Measured>> =
			self.measures.iter().cloned().map(|spec| Arc::new(Measured::of(spec))).collect();
		let mut root = displayed(&self.root, &metrics, &measured);
		root.measured = measured;
		Arc::new(root)
	}
}

/// The node of an [`AnalysedPlan`] `operator` shows.
fn analysed_node(operator: &dyn ExecOperator) -> Option<AnalysedNode> {
	let node = (operator as &dyn std::any::Any).downcast_ref::<ExplainNode>();
	let measure = node.and_then(|node| node.measure);
	let children = match measure {
		Some(_) => Vec::new(),
		None => operator
			.explain_children()
			.into_iter()
			.map(|child| analysed_node(child.as_ref()))
			.collect::<Option<_>>()?,
	};
	Some(AnalysedNode {
		name: operator.name().to_owned(),
		context: operator.required_context(),
		attrs: node.map_or_else(|| operator.attrs(), |node| node.attrs.clone()),
		children,
		reports: operator.metrics().is_some(),
		redacts_access_paths: operator.redacts_access_paths_beneath(),
		hides_metrics: operator.hides_metrics_beneath(),
		measure,
	})
}

/// The display node of `node`, reporting `metrics` where it reports any, over
/// the bitmap trees of `measured` it stands for.
fn displayed(
	node: &AnalysedNode,
	metrics: &Arc<OperatorMetrics>,
	measured: &[Arc<Measured>],
) -> ExplainNode {
	let mut children: Vec<ExplainChild> =
		node.children.iter().map(|child| displayed(child, metrics, measured).shared()).collect();
	let taken = node.measure.and_then(|at| measured.get(usize::from(at)));
	if let Some(taken) = taken {
		children.push(ExplainChild::bitmap(Arc::clone(&taken.bitmap)));
	}
	let mut shown = ExplainNode::new(
		operator_name(&node.name),
		node.context,
		node.attrs.clone(),
		children,
		node.reports.then_some(metrics),
	);
	shown.redacts_access_paths = node.redacts_access_paths;
	shown.hides_metrics = node.hides_metrics;
	shown.measure = node.measure;
	if let Some(taken) = taken {
		shown.tier.clone_from(&taken.tier);
		shown.fallback.clone_from(&taken.fallback);
	}
	shown
}

/// The name `name` as the display node that shows it holds it.
///
/// A display node's name is an operator's, from the closed set of names the plan walk gives its
/// nodes, so each distinct name is kept once for the life of the process.
fn operator_name(name: &str) -> &'static str {
	static NAMES: std::sync::LazyLock<std::sync::RwLock<std::collections::HashSet<&'static str>>> =
		std::sync::LazyLock::new(Default::default);
	use std::sync::PoisonError;
	if let Some(kept) = NAMES.read().unwrap_or_else(PoisonError::into_inner).get(name) {
		return kept;
	}
	let mut names = NAMES.write().unwrap_or_else(PoisonError::into_inner);
	if let Some(kept) = names.get(name) {
		return kept;
	}
	let kept: &'static str = Box::leak(name.to_owned().into_boxed_str());
	names.insert(kept);
	kept
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
	let plan = analysis.plan.instantiate();
	let metrics =
		plan.metrics.as_ref().map_or_else(|| Arc::new(OperatorMetrics::new()), Arc::clone);
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
	for measured in &plan.measured {
		measured.measure(&frame).await?;
	}
	Ok(crate::exec::operators::ExplainPlan::render_analyzed(
		plan.as_ref(),
		analysis.format,
		analysis.redact,
		total,
	))
}

/// A VM run's display and counters. Display nodes never execute a statement.
#[derive(Debug)]
pub(super) struct VmAnalysis {
	pub(super) plan: Arc<ExplainNode>,
	pub(super) format: crate::expr::ExplainFormat,
	pub(super) redact: bool,
}
impl VmAnalysis {
	pub(super) fn frame(&self, ctx: &ExecutionContext) -> ExecutionContext {
		let metrics = self.plan.metrics.as_ref().expect("analyzed plans hold counters");
		metrics.enable();
		ctx.with_vm_metrics(Arc::clone(metrics))
	}
	pub(super) fn run(&self, input: ValueBatchStream, ctx: &ExecutionContext) -> ValueBatchStream {
		let mut input = crate::exec::monitor_stream(
			input,
			self.plan.name,
			self.plan.metrics.as_ref().expect("analyzed plans hold counters"),
		);
		let plan = Arc::clone(&self.plan);
		let format = self.format;
		let redact = self.redact;
		let ctx = ctx.clone();
		Box::pin(futures::stream::once(async move {
			let mut total_rows = 0;
			while let Some(batch) = input.next().await {
				match batch {
					Ok(batch) => total_rows += batch.len() as u64,
					Err(crate::expr::ControlFlow::Break | crate::expr::ControlFlow::Return(_)) => {
						break;
					}
					Err(crate::expr::ControlFlow::Continue) => continue,
					Err(error @ crate::expr::ControlFlow::Err(_)) => return Err(error),
				}
			}
			for measured in &plan.measured {
				measured.measure(&ctx).await?;
			}
			let value = crate::exec::operators::ExplainPlan::render_analyzed(
				plan.as_ref(),
				format,
				redact,
				total_rows,
			);
			Ok(ValueBatch::new(vec![value]))
		}))
	}
}
