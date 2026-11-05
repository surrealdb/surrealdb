//! Binding-table steps: the nodes of a GQL `MATCH` plan that turn one binding
//! row into none, one or several.
//!
//! A binding row is an object keyed by binding name. A step reads the rows of
//! the pipeline above it, one at a time, and hands each row it yields to the
//! pipeline below it, as a split hands its rows on: the lowering ends the
//! pipeline above in a [`Sink::Split`](super::lower) and starts the one below
//! from [`Source::Split`](super::program::Source), and the host runs the step
//! in between ([`run_step`]). A step that keeps something across rows — the
//! caches of an expansion, the table of a join, the rows a `DISTINCT` has seen
//! — holds it in the run's [`StepRun`] for its split.
//!
//! A step's scalars (an expansion's predicate, a join's residual) are
//! evaluated by the host over the candidate row as its document; a join's
//! build side is a group of the program, run when the first probe row reaches
//! the join.
//!
//! The reads a step makes — adjacency, edge and node records — go through the
//! helpers of the graph operators, which apply the table and field
//! permissions and the computed fields a `SELECT` of the table would.
//!
//! A step hands the rows it yields to a [`StepSink`] in chunks, so a step that
//! can yield very many rows for one (a quantified path) stops the moment the
//! pipeline below takes no more. A write stage is a pipeline breaker: it keeps
//! every row it is handed and, once the pipeline above has finished, applies
//! its writes in row order and yields the rows ([`finish_step`]).

// The IR lowering emits these items, so some may be unused in a given build.
#![allow(dead_code)]

use std::borrow::Cow;

use surrealdb_strand::TableName;

use super::host::{Ended, eval_on_row};
use super::interp::{PooledState, State};
use super::program::HostScalar;
use crate::exec::operators::graph::distinct_edges::row_has_distinct_edges;
use crate::exec::operators::graph::endpoint::bind_endpoints;
use crate::exec::operators::graph::expand::{
	CandidateJudge, ExpandCaches, ExpandParams, edge_specs_for, expand_input_row,
};
use crate::exec::operators::graph::path_expand::{
	PathCaches, PathFlush, PathMode, PathParams, expand_paths,
};
use crate::exec::operators::graph::shortest_path_expand::{ShortestSelector, expand_shortest};
use crate::exec::operators::join::hash_join::{
	BuildTable, emit, join_key, merge_rows, null_filled,
};
use crate::exec::operators::mutate::{
	InsertEdgeOp, InsertNodeOp, MutationParam, RowEval, RunWrite, UpdateOp, delete_row, insert_row,
	update_row,
};
use crate::exec::operators::scan::fetch::FetchFieldStateCache;
use crate::exec::operators::{EdgeBinding, EndpointField, ExpandDir, JoinType};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::expr::match_plan::DetachMode;
use crate::val::{Object, RecordId, Value};

/// A single-hop expansion off the node bound at `source`. `P` is how the
/// predicate is held: a logical expression in a plan, a host scalar in a
/// program.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExpandStep<P> {
	pub(crate) source: String,
	pub(crate) direction: ExpandDir,
	/// The edge tables traversed; none means every edge table.
	pub(crate) edge_tables: Vec<TableName>,
	pub(crate) edge_binding: EdgeBinding,
	pub(crate) target_binding: String,
	pub(crate) target_label: Option<TableName>,
	/// The predicate a candidate row must satisfy.
	pub(crate) predicate: Option<P>,
	/// Whether an input row with no survivor yields one row with the edge and
	/// the target bound to `NULL`.
	pub(crate) optional: bool,
}

/// The endpoint node of the edge bound at `edge`, bound beside it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EndpointStep {
	pub(crate) edge: String,
	pub(crate) field: EndpointField,
	pub(crate) name: String,
	pub(crate) label: Option<TableName>,
}

/// A quantified expansion off the node bound at `source`: every path the
/// pattern admits, or the ones a shortest-path selector keeps.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PathStep {
	pub(crate) source: String,
	pub(crate) direction: ExpandDir,
	/// The edge tables traversed; none means every edge table.
	pub(crate) edge_tables: Vec<TableName>,
	pub(crate) min: u32,
	pub(crate) max: Option<u32>,
	pub(crate) target_binding: String,
	pub(crate) target_label: Option<TableName>,
	/// The binding the edges of a path are bound to, as an array.
	pub(crate) group_binding: Option<String>,
	/// The binding the alternating node and edge array of a path is bound to.
	pub(crate) path_binding: Option<String>,
	pub(crate) mode: PathMode,
	/// `None` enumerates every path.
	pub(crate) selector: Option<ShortestSelector>,
	/// Whether the pattern was anchored on its far node, so the paths are
	/// reversed to read in the order the pattern is written.
	pub(crate) reversed: bool,
}

/// A write stage over the rows of the pipeline above it. `P` is how its
/// row-scoped values are held.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MutationStep<P> {
	/// `SET` / `REMOVE` of the record bound at `target`.
	Update {
		target: String,
		op: UpdateOp<P>,
		/// The group of the program that writes one record.
		write: u16,
	},
	/// `[DETACH|NODETACH] DELETE` of the record bound at `target`.
	Delete {
		target: String,
		detach: DetachMode,
		/// Whether the target is an edge, which has no connected edges to refuse.
		is_edge: bool,
		/// The group of the program that deletes one record.
		write: u16,
	},
	/// `INSERT` of new nodes and edges, each created by a group of the program of
	/// its own.
	Insert {
		nodes: Vec<InsertNodeOp<P>>,
		edges: Vec<InsertEdgeOp<P>>,
	},
}

/// A join of the rows above it, the probe side, with the rows of `build`.
/// `B` is how the build side is held: a plan, or the group of a program.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct JoinStep<B, P> {
	pub(crate) build: B,
	/// The node bindings both sides share, which the join is keyed on.
	pub(crate) keys: Vec<String>,
	pub(crate) join_type: JoinType,
	/// The bindings the build side introduces, which a probe row with no
	/// match carries as `NULL` under a left join.
	pub(crate) null_template: Vec<String>,
	pub(crate) residual: Option<P>,
}

/// One step of a binding-table plan.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BindingStep<P, B> {
	/// `{ name: row }`.
	Bind {
		name: String,
	},
	Expand(ExpandStep<P>),
	Endpoint(EndpointStep),
	/// The rows that bind no edge twice across `edges`.
	DistinctEdges {
		edges: Vec<String>,
	},
	/// The first of each distinct row.
	Distinct,
	Join(JoinStep<B, P>),
	Path(PathStep),
	/// A write stage, which keeps its rows until the pipeline above has
	/// finished.
	Mutate(MutationStep<P>),
	/// Takes every row and yields none.
	Drain,
}

/// A step as a program holds it: scalars as host scalars, a join's build side
/// as a group.
pub(crate) type ProgramStep = BindingStep<HostScalar, u16>;

assert_plain!(ProgramStep);

impl<P, B> BindingStep<P, B> {
	/// The name the step is rendered under.
	pub(crate) fn name(&self) -> &'static str {
		match self {
			Self::Bind {
				..
			} => "Bind",
			Self::Expand(step) if step.optional => "OptionalExpand",
			Self::Expand(_) => "Expand",
			Self::Endpoint(_) => "EndpointBind",
			Self::DistinctEdges {
				..
			} => "DistinctEdges",
			Self::Distinct => "Distinct",
			Self::Join(_) => "HashJoin",
			Self::Path(step) if step.selector.is_some() => "ShortestPathExpand",
			Self::Path(_) => "PathExpand",
			Self::Mutate(MutationStep::Update {
				..
			}) => "UpdateBinding",
			Self::Mutate(MutationStep::Delete {
				..
			}) => "DeleteBinding",
			Self::Mutate(MutationStep::Insert {
				..
			}) => "InsertGraph",
			Self::Drain => "DrainSink",
		}
	}

	/// Whether the step may keep rows it has not yet answered, which it
	/// answers once the pipeline above has finished: [`finish_step`], or
	/// [`finish_distinct`] for a distinct that spilled and deferred them.
	pub(crate) fn buffers(&self) -> bool {
		matches!(self, Self::Mutate(_) | Self::Endpoint(_) | Self::Distinct | Self::Join(_))
	}
}

/// What a step keeps across the rows of one run of its program.
pub(super) enum StepRun {
	Expand {
		params: ExpandParams,
		caches: ExpandCaches,
		judge: Option<PooledState>,
	},
	Endpoint {
		cache: FetchFieldStateCache,
		/// The rows kept for the next batch fetch of endpoint nodes.
		pending: Vec<Value>,
		/// How many rows make a batch.
		batch: usize,
	},
	Distinct(super::distinct_spill::Distinct),
	Join {
		/// The build side's rows, once the first probe row has built them.
		table: Option<super::join_spill::Built>,
		emitted: usize,
		judge: Option<PooledState>,
	},
	Path {
		params: PathParams,
		caches: PathCaches,
	},
	/// A write stage's rows, kept until the pipeline above has finished.
	Buffer {
		rows: Vec<Value>,
		eval: Option<PooledState>,
	},
	/// A window's run ([`super::window_step`]), held under the split that reaches it.
	Window(Box<super::window_step::WindowState>),
}

impl StepRun {
	/// The state a run of `step` starts from, or `None` for a step that keeps
	/// nothing.
	fn start(step: &ProgramStep, ctx: &ExecutionContext) -> Option<Self> {
		let exec = &ctx.root().ctx.config.exec;
		match step {
			BindingStep::Expand(step) => Some(Self::Expand {
				params: ExpandParams {
					direction: step.direction,
					edge_specs: edge_specs_for(&step.edge_tables),
					edge_name: match &step.edge_binding {
						EdgeBinding::Full(name) | EdgeBinding::IdOnly(name) => name.clone(),
					},
					fetch_full_edge: matches!(step.edge_binding, EdgeBinding::Full(_)),
					target_binding: step.target_binding.clone(),
					target_label: step.target_label.clone(),
					optional: step.optional,
					scan_batch_size: exec.scan_batch_size,
					max_output_rows: exec.gql_max_output_rows,
				},
				caches: ExpandCaches::new(),
				judge: None,
			}),
			BindingStep::Endpoint(_) => Some(Self::Endpoint {
				cache: FetchFieldStateCache::new(),
				pending: Vec::new(),
				batch: exec.scan_batch_size.max(1),
			}),
			BindingStep::Distinct => Some(Self::Distinct(super::distinct_spill::Distinct::start(
				exec.gql_max_join_build_rows,
				ctx,
			))),
			BindingStep::Join(_) => Some(Self::Join {
				table: None,
				emitted: 0,
				judge: None,
			}),
			BindingStep::Path(step) => Some(Self::Path {
				params: PathParams {
					source: step.source.clone(),
					direction: step.direction,
					edge_specs: edge_specs_for(&step.edge_tables),
					min: step.min,
					max: step.max,
					target_binding: step.target_binding.clone(),
					target_label: step.target_label.clone(),
					group_binding: step.group_binding.clone(),
					path_binding: step.path_binding.clone(),
					mode: step.mode,
					reversed: step.reversed,
					scan_batch_size: exec.scan_batch_size,
					path_row_limit: exec.gql_max_path_rows,
				},
				caches: PathCaches::new(),
			}),
			BindingStep::Mutate(_) => Some(Self::Buffer {
				rows: Vec::new(),
				eval: None,
			}),
			BindingStep::Bind {
				..
			}
			| BindingStep::DistinctEdges {
				..
			}
			| BindingStep::Drain => None,
		}
	}
}

/// Judges a candidate row with a host scalar, over a state kept for the
/// run of the step.
pub(super) struct HostJudge<'a> {
	pub(super) program: &'a super::program::Program,
	pub(super) scalar: Option<&'a HostScalar>,
	pub(super) ctx: &'a ExecutionContext,
	pub(super) state: &'a mut Option<PooledState>,
}

impl HostJudge<'_> {
	async fn truthy(&mut self, row: &Value) -> FlowResult<bool> {
		let Some(scalar) = self.scalar else {
			return Ok(true);
		};
		match scalar {
			HostScalar::Const(value) => Ok(Value::clone(value).is_truthy()),
			HostScalar::Code(pipeline) => {
				let answer =
					eval_on_row(self.program, *pipeline, Cow::Borrowed(row), self.state, self.ctx)
						.await?;
				Ok(answer.is_truthy())
			}
		}
	}
}

impl CandidateJudge for HostJudge<'_> {
	/// Boxed: the evaluation runs a program, which may run this step again,
	/// and a type that holds its own future cannot be proven `Send`.
	#[allow(refining_impl_trait)]
	fn passes<'a>(
		&'a mut self,
		candidate: &'a Value,
	) -> futures::future::BoxFuture<'a, FlowResult<bool>> {
		Box::pin(self.truthy(candidate))
	}
}

/// Where the rows a step yields go: the pipeline below it.
pub(super) trait StepSink: Send {
	/// Run `rows`, in order, through the pipeline below the step. `Stop` once
	/// it takes no more.
	fn rows<'a>(
		&'a mut self,
		rows: Vec<Value>,
		state: &'a mut State,
	) -> futures::future::BoxFuture<'a, FlowResult<Ended>>;
}

/// The sink that receives the full batches a path expansion produces.
struct SinkFlush<'a> {
	sink: &'a mut dyn StepSink,
	state: &'a mut State,
}

impl PathFlush for SinkFlush<'_> {
	fn flush<'a>(
		&'a mut self,
		rows: Vec<Value>,
	) -> futures::future::BoxFuture<'a, FlowResult<bool>> {
		Box::pin(async move { Ok(self.sink.rows(rows, self.state).await? != Ended::Stop) })
	}
}

/// What a step did with a row.
pub(super) enum Stepped {
	/// It yields these rows, which the caller runs through the pipeline below.
	Rows(Vec<Value>),
	/// It handed what it yields to the sink as it went, and the pipeline below
	/// answered `Step`.
	Delivered(Ended),
}

/// Keep `row` for step `step` of split `split` where the step only keeps it:
/// a write stage's rows, an endpoint bind's rows short of a batch, a drain's
/// rows, which it drops. The row is handed back where the step has more to do
/// with it, which [`run_step`] does.
pub(super) fn keep_row(
	step: &ProgramStep,
	split: u16,
	row: Value,
	state: &mut State,
) -> Result<(), Value> {
	match (step, state.steps[split as usize].as_deref_mut()) {
		(
			BindingStep::Endpoint(_),
			Some(StepRun::Endpoint {
				pending,
				batch,
				..
			}),
		) if pending.len() + 1 < *batch => {
			pending.push(row);
			Ok(())
		}
		(
			BindingStep::Mutate(_),
			Some(StepRun::Buffer {
				rows,
				..
			}),
		) => {
			rows.push(row);
			Ok(())
		}
		(BindingStep::Drain, _) => Ok(()),
		_ => Err(row),
	}
}

/// Run step `step` of split `split` over `row`. A step that can yield very
/// many rows for one hands them to `sink` in chunks, so it stops the moment
/// the pipeline below takes no more; any other yields them as one.
///
/// The step's run state is taken out of `state` while the step runs and put
/// back after, so a join can run its build side over `state`.
pub(super) async fn run_step(
	program: &super::program::Program,
	split: u16,
	step: &ProgramStep,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
	sink: &mut dyn StepSink,
) -> FlowResult<Stepped> {
	super::fuel::tick(ctx, 1).await?;
	let mut run = match state.steps[split as usize].take() {
		Some(run) => Some(run),
		None => StepRun::start(step, ctx).map(Box::new),
	};
	let stepped = step_chunks(program, step, run.as_deref_mut(), row, state, ctx, sink).await;
	state.steps[split as usize] = run;
	stepped
}

async fn step_chunks(
	program: &super::program::Program,
	step: &ProgramStep,
	run: Option<&mut StepRun>,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
	sink: &mut dyn StepSink,
) -> FlowResult<Stepped> {
	match (step, run) {
		(
			BindingStep::Path(step),
			Some(StepRun::Path {
				params,
				caches,
			}),
		) => {
			let mut out = Vec::with_capacity(params.scan_batch_size);
			let more = {
				let mut flush = SinkFlush {
					sink: &mut *sink,
					state: &mut *state,
				};
				match step.selector {
					None => expand_paths(ctx, params, caches, &row, &mut out, &mut flush).await?,
					Some(selector) => {
						expand_shortest(ctx, params, selector, caches, &row, &mut out, &mut flush)
							.await?
					}
				}
			};
			if !more {
				return Ok(Stepped::Delivered(Ended::Stop));
			}
			if out.is_empty() {
				return Ok(Stepped::Delivered(Ended::Next));
			}
			Ok(Stepped::Delivered(sink.rows(out, state).await?))
		}
		(
			BindingStep::Mutate(_),
			Some(StepRun::Buffer {
				rows,
				..
			}),
		) => {
			rows.push(row);
			Ok(Stepped::Rows(Vec::new()))
		}
		(BindingStep::Drain, _) => Ok(Stepped::Rows(Vec::new())),
		(step, run) => Ok(Stepped::Rows(step_rows(program, step, run, row, state, ctx).await?)),
	}
}

/// Runs the write groups of a write stage's program, each in the step's own
/// execution context with the row's values bound as parameters.
struct GroupWrites<'s> {
	program: std::sync::Arc<super::program::Program>,
	state: &'s mut State,
	ctx: &'s ExecutionContext,
}

impl RunWrite for GroupWrites<'_> {
	fn run<'a>(
		&'a mut self,
		write: u16,
		values: Vec<(MutationParam, Value)>,
	) -> futures::future::BoxFuture<'a, Result<Vec<Value>, ControlFlow>> {
		Box::pin(async move {
			let frame = self.ctx.with_params(values.into_iter().map(|(p, v)| (p.name(), v)));
			let (rows, ran) =
				super::host::run_group(&self.program, write, self.state, &frame).await;
			ran?;
			Ok(rows)
		})
	}
}

/// Evaluates the row-scoped values of a write stage as host scalars, over a
/// state kept for the run of the step.
struct HostEval<'a> {
	program: &'a super::program::Program,
	ctx: &'a ExecutionContext,
	state: &'a mut Option<PooledState>,
}

impl RowEval<HostScalar> for HostEval<'_> {
	fn eval<'a>(
		&'a mut self,
		value: &'a HostScalar,
		row: &'a Value,
	) -> futures::future::BoxFuture<'a, Result<Value, ControlFlow>> {
		Box::pin(async move {
			match value {
				HostScalar::Const(value) => Ok(Value::clone(value)),
				HostScalar::Code(pipeline) => {
					eval_on_row(self.program, *pipeline, Cow::Borrowed(row), self.state, self.ctx)
						.await
				}
			}
		})
	}
}

/// The rows a step deferred once it spilled — a distinct's rows first seen,
/// a join's pairings — to hand on in order once the pipeline above has
/// finished. Returns `None` where step `split` deferred none.
pub(super) async fn finish_deferred(
	program: &super::program::Program,
	split: u16,
	step: &ProgramStep,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Option<super::spill::Merge>> {
	match (step, state.steps[split as usize].as_deref_mut()) {
		(BindingStep::Distinct, Some(StepRun::Distinct(distinct))) if distinct.has_spilled() => {
			distinct.finish(ctx).await
		}
		(
			BindingStep::Join(step),
			Some(StepRun::Join {
				table,
				emitted,
				judge,
			}),
		) if matches!(table, Some(super::join_spill::Built::Spilled(_))) => {
			let Some(super::join_spill::Built::Spilled(spilled)) = table.take() else {
				return Ok(None);
			};
			let mut with = super::join_spill::Pairing {
				step,
				residual: HostJudge {
					program,
					scalar: step.residual.as_ref(),
					ctx,
					state: judge,
				},
				emitted,
				max_output_rows: ctx.root().ctx.config.exec.gql_max_output_rows,
			};
			spilled.finish(&mut with).await.map(Some)
		}
		_ => Ok(None),
	}
}

pub(super) async fn finish_step(
	program: &super::program::Program,
	split: u16,
	step: &ProgramStep,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Value>> {
	let Some(run) = state.steps[split as usize].take() else {
		return Ok(Vec::new());
	};
	let (rows, mut eval) = match (step, *run) {
		(
			BindingStep::Endpoint(step),
			StepRun::Endpoint {
				mut cache,
				pending,
				..
			},
		) => {
			return bind_endpoints(
				ctx,
				&mut cache,
				&step.edge,
				step.field,
				&step.name,
				step.label.as_ref(),
				&pending,
			)
			.await;
		}
		(
			BindingStep::Mutate(_),
			StepRun::Buffer {
				rows,
				eval,
			},
		) => (rows, eval),
		_ => return Ok(Vec::new()),
	};
	let BindingStep::Mutate(mutation) = step else {
		return Ok(Vec::new());
	};
	let mut out = Vec::with_capacity(rows.len());
	let mut host = HostEval {
		program,
		ctx,
		state: &mut eval,
	};
	// Each write the step applies is a group of its program, compiled with it.
	let mut writes = GroupWrites {
		program: state.program.clone().ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!("a mutation step run with no program"))
		})?,
		state,
		ctx,
	};
	match mutation {
		MutationStep::Update {
			target,
			op,
			write,
		} => {
			for mut row in rows {
				super::fuel::tick(ctx, 1).await?;
				update_row(op, *write, target, &mut row, &mut host, &mut writes).await?;
				out.push(row);
			}
		}
		MutationStep::Delete {
			target,
			detach,
			is_edge,
			write,
		} => {
			let ids = {
				let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(anyhow::anyhow!(e)))?;
				(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id)
			};
			let mut deleted: std::collections::HashSet<RecordId> = std::collections::HashSet::new();
			for mut row in rows {
				super::fuel::tick(ctx, 1).await?;
				delete_row(
					&mut row,
					target,
					*detach,
					*is_edge,
					ids,
					&mut deleted,
					*write,
					&mut writes,
					ctx,
				)
				.await?;
				out.push(row);
			}
		}
		MutationStep::Insert {
			nodes,
			edges,
		} => {
			for mut row in rows {
				super::fuel::tick(ctx, 1).await?;
				insert_row(nodes, edges, &mut row, &mut host, &mut writes).await?;
				out.push(row);
			}
		}
	}
	Ok(out)
}

async fn step_rows(
	program: &super::program::Program,
	step: &ProgramStep,
	run: Option<&mut StepRun>,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Value>> {
	match (step, run) {
		(
			BindingStep::Bind {
				name,
			},
			_,
		) => {
			let mut bound = Object::default();
			bound.insert(name.clone(), row);
			Ok(vec![Value::Object(bound)])
		}
		(
			BindingStep::Expand(step),
			Some(StepRun::Expand {
				params,
				caches,
				judge,
			}),
		) => {
			let mut judge = HostJudge {
				program,
				scalar: step.predicate.as_ref(),
				ctx,
				state: judge,
			};
			let mut out = Vec::new();
			expand_input_row(ctx, params, &step.source, caches, &mut judge, &row, &mut out).await?;
			Ok(out)
		}
		(
			BindingStep::Endpoint(step),
			Some(StepRun::Endpoint {
				cache,
				pending,
				batch,
			}),
		) => {
			// Endpoint nodes are fetched a batch of rows at a time, the same
			// batching the graph endpoint operator uses.
			pending.push(row);
			if pending.len() < *batch {
				return Ok(Vec::new());
			}
			let rows = std::mem::take(pending);
			bind_endpoints(
				ctx,
				cache,
				&step.edge,
				step.field,
				&step.name,
				step.label.as_ref(),
				&rows,
			)
			.await
		}
		(
			BindingStep::DistinctEdges {
				edges,
			},
			_,
		) => Ok(if row_has_distinct_edges(&row, edges) {
			vec![row]
		} else {
			Vec::new()
		}),
		(BindingStep::Distinct, Some(StepRun::Distinct(distinct))) => {
			Ok(distinct.take(row, ctx).await?.into_iter().collect())
		}
		(
			BindingStep::Join(step),
			Some(StepRun::Join {
				table,
				emitted,
				judge,
			}),
		) => join_row(program, step, table, emitted, judge, row, state, ctx).await,
		_ => Err(ControlFlow::Err(anyhow::anyhow!("a binding step run of another kind"))),
	}
}

/// What probe `row` of an inner, left, semi or anti join `step` yields given
/// the build rows its key `matches`, in order: a build row matches only if
/// the merged row passes the residual, and a left join's probe row with no
/// passing build row is carried with the build's bindings `NULL`.
pub(super) async fn pairings(
	step: &JoinStep<u16, HostScalar>,
	matches: &[&Value],
	row: Value,
	residual: &mut HostJudge<'_>,
	emitted: &mut usize,
	max_output_rows: usize,
) -> FlowResult<Vec<Value>> {
	let mut out = Vec::new();
	match step.join_type {
		JoinType::Cross | JoinType::Inner => {
			for build_row in matches {
				super::fuel::tick(residual.ctx, 1).await?;
				let merged = merge_rows(build_row, &row);
				if residual.truthy(&merged).await? {
					emit(&mut out, emitted, max_output_rows, merged)?;
				}
			}
		}
		JoinType::Left => {
			let mut matched = false;
			for build_row in matches {
				super::fuel::tick(residual.ctx, 1).await?;
				let merged = merge_rows(build_row, &row);
				if residual.truthy(&merged).await? {
					emit(&mut out, emitted, max_output_rows, merged)?;
					matched = true;
				}
			}
			if !matched {
				emit(&mut out, emitted, max_output_rows, null_filled(&row, &step.null_template))?;
			}
		}
	}
	Ok(out)
}

/// The table of `step`'s build side: its group run, its rows keyed on the
/// bindings the join is keyed on. A join with keys reads the run a batch at
/// a time and spills it once its lease no longer covers it
/// ([`super::join_spill`]); a cross join gathers it.
async fn build_table(
	step: &JoinStep<u16, HostScalar>,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<super::join_spill::Built> {
	let max_rows = ctx.root().ctx.config.exec.gql_max_join_build_rows;
	let Some(running) = state.program.clone() else {
		return Err(ControlFlow::Err(anyhow::anyhow!("a join run with no program")));
	};
	let spills = step.join_type != JoinType::Cross
		&& ctx.ctx().memory().is_some_and(|memory| memory.can_spill())
		&& !running.groups.get(step.build as usize).is_some_and(|spec| spec.only);
	if spills {
		let mut sink = super::join_spill::BuildSink::new(&step.keys, max_rows, step.build, ctx);
		super::host::run_group_into(&running, step.build, state, ctx, &mut sink).await?;
		return sink.finish(ctx).await;
	}
	let (rows, ran) = super::host::run_group(&running, step.build, state, ctx).await;
	ran?;
	let mut built = BuildTable::new();
	for build_row in rows {
		super::fuel::tick(ctx, 1).await?;
		if let Some(key) = join_key(&build_row, &step.keys) {
			built.insert(key, build_row, max_rows)?;
		}
	}
	Ok(super::join_spill::Built::Memory(built))
}

/// Run the build side of join `step` of split `split` now, so the rows the
/// join takes find the table built.
pub(super) async fn prepare_join(
	split: u16,
	step: &ProgramStep,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<()> {
	let BindingStep::Join(join) = step else {
		return Ok(());
	};
	let built = build_table(join, state, ctx).await?;
	state.steps[split as usize] = Some(Box::new(StepRun::Join {
		table: Some(built),
		emitted: 0,
		judge: None,
	}));
	Ok(())
}

/// The rows a probe row joins into: built from the build side's rows, run on
/// the first probe row to arrive.
///
/// An empty probe never runs the build side. A build row counts as a match only
/// if the merged row passes the residual; under a left join a probe row with no
/// passing build row — including one with no key — is carried with the
/// build-introduced bindings `NULL`, not dropped.
#[allow(clippy::too_many_arguments)]
async fn join_row(
	program: &super::program::Program,
	step: &JoinStep<u16, HostScalar>,
	table: &mut Option<super::join_spill::Built>,
	emitted: &mut usize,
	judge: &mut Option<PooledState>,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
) -> FlowResult<Vec<Value>> {
	let exec = &ctx.root().ctx.config.exec;
	let max_output_rows = exec.gql_max_output_rows;
	if table.is_none() {
		*table = Some(build_table(step, state, ctx).await?);
	}
	let mut residual = HostJudge {
		program,
		scalar: step.residual.as_ref(),
		ctx,
		state: judge,
	};
	let table = match table.as_mut() {
		None => return Ok(Vec::new()),
		Some(super::join_spill::Built::Memory(table)) => &*table,
		Some(super::join_spill::Built::Spilled(spilled)) => {
			let mut with = super::join_spill::Pairing {
				step,
				residual,
				emitted,
				max_output_rows,
			};
			spilled.defer(join_key(&row, &step.keys), row, &mut with).await?;
			return Ok(Vec::new());
		}
	};
	let mut out = Vec::new();
	match step.join_type {
		JoinType::Cross => {
			for build_row in table.all_rows() {
				super::fuel::tick(ctx, 1).await?;
				let merged = merge_rows(build_row, &row);
				if residual.truthy(&merged).await? {
					emit(&mut out, emitted, max_output_rows, merged)?;
				}
			}
		}
		JoinType::Inner | JoinType::Left => {
			let key = join_key(&row, &step.keys);
			let matches: Vec<&Value> = match &key {
				Some(key) => table.matches(key).collect(),
				None => Vec::new(),
			};
			out = pairings(step, &matches, row, &mut residual, emitted, max_output_rows).await?;
		}
	}
	Ok(out)
}
