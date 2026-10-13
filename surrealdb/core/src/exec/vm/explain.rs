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

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::metrics::OperatorMetrics;
use crate::exec::operators::scan::bitmap::evaluate_fusion;
use crate::exec::operators::scan::{BitmapNode, FusedCandidates, PrefilterTier};
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
/// evaluation matches what the run saw.
#[derive(Debug)]
pub(in crate::exec::vm) enum Measured {
	/// The allow-list a KNN search is restricted to, whose size chose the
	/// strategy of the search.
	Prefilter {
		table: TableName,
		bitmap: Arc<BitmapNode>,
		tier: Arc<PrefilterChoice>,
		/// Whether the statement reads at a `VERSION`, which no allow-list
		/// serves.
		versioned: bool,
		/// The index searched, and the `ef` the search was asked for.
		index: crate::exec::index::access_path::IndexRef,
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
		table: TableName,
		bitmap: Arc<BitmapNode>,
		/// Whether the plan has no fallback, so no branch may overflow.
		anchored: bool,
		fallback: Arc<AtomicBool>,
	},
}

impl Measured {
	async fn measure(&self, ctx: &ExecutionContext) -> FlowResult<()> {
		match self {
			Self::Prefilter {
				table,
				bitmap,
				tier,
				versioned,
				index,
				ef,
				judges_condition,
				judges_permission,
			} => {
				let (chosen, walk_ef) = if *versioned {
					(PrefilterTier::Fallback, None)
				} else {
					match bitmap
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
				tier.tier.store(chosen as u8, Ordering::Relaxed);
				tier.walk_ef.store(walk_ef.unwrap_or(0), Ordering::Relaxed);
			}
			Self::Fusion {
				table,
				bitmap,
				anchored,
				fallback,
			} => {
				if matches!(
					evaluate_fusion(ctx, table, bitmap, *anchored).await?,
					FusedCandidates::Overflow
				) {
					fallback.store(true, Ordering::Relaxed);
				}
			}
		}
		Ok(())
	}
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
