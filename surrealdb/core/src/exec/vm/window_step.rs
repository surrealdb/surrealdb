//! The window step: the host's side of `phys.window`.
//!
//! A window is a breaker reached through a split. The pipeline that feeds it reads its input in
//! partition-then-order sequence (a sort on the partition keys, then the order keys, or the input
//! itself where the window has neither) and ends in [`Instr::SplitPush`](super::program::Instr),
//! which hands each row to the step; the pipeline that reads the window's rows starts at
//! [`Source::Split`](super::program::Source). The host runs the step in between: [`run_window`]
//! for each row, then [`WindowState::finish`] and [`WindowState::next_chunk`] once the feeder has
//! finished.
//!
//! The step evaluates each row's partition keys, order keys and call arguments on the host, as
//! host scalars over the row, and pushes them with the row into a [`WindowRun`]. The run buffers
//! what it cannot answer yet, charges it to the query's memory lease and spills it; the step holds
//! no row of its own beyond the chunk it is handing on. The rows the run has answered are drained
//! in chunks of at most `scan_batch_size` and handed to the pipeline below, in input order.
//!
//! Every way a run ends lets go of its buffer through [`WindowRun::clear`]: the feeder finishing
//! and the reader draining it, the pipeline below taking no more rows (a `LIMIT` above the window,
//! after which the run takes no more rows and its remaining partitions are never computed), an
//! error, and the start of a new run of the group that owns the window.

use std::borrow::Cow;
use std::sync::Arc;

use super::binding::{StepRun, StepSink};
use super::host::{Ended, eval_on_row};
use super::interp::{PooledState, State};
use super::program::{HostScalar, KeyOrder, Program};
use super::window::{CallSpec, WindowInput, WindowRun, WindowSpec};
use crate::exec::ir::logical::ops::WindowShape;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
use crate::val::Value;

/// A window as a program holds it: its shape, and the host scalars of its key and argument
/// regions.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct WindowStep {
	pub(super) shape: WindowShape,
	/// The partition keys, then the order keys: one per key region, in region order.
	pub(super) keys: Vec<HostScalar>,
	/// The arguments of every call, in call order: one per argument region.
	pub(super) args: Vec<HostScalar>,
}

assert_plain!(WindowStep);

impl WindowStep {
	/// The spec a run of the window computes. Partition keys compare in ascending default order,
	/// as the sort beneath the window orders them, so only the order keys carry flags.
	fn spec(&self) -> Arc<WindowSpec> {
		Arc::new(WindowSpec {
			partitions: usize::from(self.shape.partitions),
			order: self
				.shape
				.order
				.iter()
				.map(|key| KeyOrder {
					ascending: key.ascending,
					collate: key.collate,
					numeric: key.numeric,
				})
				.collect(),
			calls: self
				.shape
				.calls
				.iter()
				.map(|call| CallSpec {
					output: call.output.clone(),
					function: call.function,
					args: usize::from(call.args),
					frame: call.frame,
				})
				.collect(),
		})
	}
}

/// What a window keeps across the rows of one run of its program.
pub(super) struct WindowState {
	run: WindowRun,
	/// The state the host scalars of the keys and arguments are evaluated on.
	eval: Option<PooledState>,
	/// How many rows a chunk handed on holds at most.
	chunk: usize,
	/// Whether the run has ended early: the pipeline below took no more rows, or a row failed.
	/// An ended run takes no more rows and yields none.
	ended: bool,
}

impl WindowState {
	fn start(step: &WindowStep, ctx: &ExecutionContext) -> Self {
		Self {
			run: WindowRun::new(step.spec(), ctx),
			eval: None,
			chunk: ctx.root().ctx.config.exec.scan_batch_size.max(1),
			ended: false,
		}
	}

	/// Lets go of everything the run holds, its buffer's lease included. The run takes no more
	/// rows after.
	pub(super) fn clear(&mut self) {
		self.run.clear();
		self.ended = true;
	}

	/// Pushes `row` into the run, then hands every row the run has answered to `sink`: `Stop`
	/// once the pipeline below takes no more.
	async fn take(
		&mut self,
		program: &Program,
		step: &WindowStep,
		row: Value,
		state: &mut State,
		ctx: &ExecutionContext,
		sink: &mut dyn StepSink,
	) -> FlowResult<Ended> {
		if self.ended {
			return Ok(Ended::Stop);
		}
		let mut keys = Vec::with_capacity(step.keys.len());
		for key in &step.keys {
			keys.push(self.eval(program, key, &row, ctx).await?);
		}
		let mut args = Vec::with_capacity(step.args.len());
		for arg in &step.args {
			args.push(self.eval(program, arg, &row, ctx).await?);
		}
		self.run
			.push(
				WindowInput {
					keys,
					args,
					row,
				},
				ctx,
			)
			.await?;
		loop {
			let rows = self.run.drain(self.chunk, ctx).await?;
			if rows.is_empty() {
				return Ok(Ended::Next);
			}
			if sink.rows(rows, state).await? == Ended::Stop {
				return Ok(Ended::Stop);
			}
		}
	}

	async fn eval(
		&mut self,
		program: &Program,
		scalar: &HostScalar,
		row: &Value,
		ctx: &ExecutionContext,
	) -> FlowResult<Value> {
		match scalar {
			HostScalar::Const(value) => Ok(Value::clone(value)),
			HostScalar::Code(pipeline) => {
				eval_on_row(program, *pipeline, Cow::Borrowed(row), &mut self.eval, ctx).await
			}
		}
	}

	/// Tells the run its input has ended, so every row it holds is answered. `false` for a run
	/// that ended early, which yields no more rows.
	pub(super) async fn finish(&mut self, ctx: &ExecutionContext) -> FlowResult<bool> {
		if self.ended {
			return Ok(false);
		}
		self.run.finish(ctx).await?;
		Ok(true)
	}

	/// The next rows the run has answered, at most a chunk of them; none once it is drained.
	pub(super) async fn next_chunk(&mut self, ctx: &ExecutionContext) -> FlowResult<Vec<Value>> {
		if self.ended {
			return Ok(Vec::new());
		}
		self.run.drain(self.chunk, ctx).await
	}
}

/// Run window `step` of split `split` over `row`, handing the rows it answers to `sink` in chunks:
/// `Stop` once the pipeline below takes no more, or once the run ended early.
///
/// The run starts on the first row. A run that hands on its last row, or fails, is cleared and
/// takes no more rows.
pub(super) async fn run_window(
	program: &Program,
	split: u16,
	step: &WindowStep,
	row: Value,
	state: &mut State,
	ctx: &ExecutionContext,
	sink: &mut dyn StepSink,
) -> FlowResult<Ended> {
	super::fuel::tick(ctx, 1).await?;
	let at = split as usize;
	let mut run = match state.steps[at].take() {
		Some(run) => run,
		None => Box::new(StepRun::Window(Box::new(WindowState::start(step, ctx)))),
	};
	let StepRun::Window(window) = &mut *run else {
		state.steps[at] = Some(run);
		return Err(ControlFlow::Err(anyhow::anyhow!("a window step run of another kind")));
	};
	let ended = window.take(program, step, row, state, ctx, sink).await;
	match &ended {
		Ok(Ended::Next) => {}
		Ok(Ended::Stop) | Err(_) => window.clear(),
	}
	state.steps[at] = Some(run);
	ended
}

/// The run of the window of split `split`, taken out of `state`: none where no row reached it. A
/// run of another kind is left where it is.
pub(super) fn take_window(split: u16, state: &mut State) -> Option<WindowState> {
	let slot = state.steps.get_mut(split as usize)?;
	if !matches!(slot.as_deref(), Some(StepRun::Window(_))) {
		return None;
	}
	match *slot.take()? {
		StepRun::Window(window) => Some(*window),
		StepRun::Expand {
			..
		}
		| StepRun::Endpoint {
			..
		}
		| StepRun::Distinct(_)
		| StepRun::Join {
			..
		}
		| StepRun::Path {
			..
		}
		| StepRun::Buffer {
			..
		} => None,
	}
}

/// Clears the run of the window of every split of `program` whose pipeline belongs to `group`,
/// so a new run of the group starts every window afresh.
pub(super) fn reset_windows(program: &Program, group: u16, state: &mut State) {
	for (at, split) in program.splits.iter().enumerate() {
		if matches!(split.kind, super::program::SplitKind::Window(_))
			&& program
				.pipelines
				.get(split.pipeline)
				.is_some_and(|pipeline| pipeline.group == Some(group))
			&& let Some(mut window) = take_window(at as u16, state)
		{
			window.clear();
		}
	}
}
