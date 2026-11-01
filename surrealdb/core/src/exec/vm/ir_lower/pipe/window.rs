//! A window: `phys.window`.
//!
//! A window is a breaker the host runs between two pipelines, as a binding step is
//! ([`super::step`]). The pipeline that feeds it ends in [`Sink::Split`](super::Sink), which hands
//! each row to the window's split; the pipeline that reads the window's rows starts at
//! [`Source::Split`](crate::exec::vm::program::Source). The split's spec is a
//! [`SplitKind::Window`], which holds the window's shape and its regions as host scalars: the
//! partition keys, then the order keys, then each call's arguments in call order, each evaluated by
//! the host over the row the window takes. The keys are evaluated in the step rather than read from
//! the sort beneath it, because a sort's keys do not cross the split.
//!
//! The window computes no read and no write of its own, so it adds nothing to what the program
//! needs beyond what its scalars need.

use super::step::row_scalar;
use super::{Needs, Reads, pool_index};
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::error::IrError;
use crate::exec::ir::logical::ops::WindowShape;
use crate::exec::ir::logical::seam::invariant;
use crate::exec::vm::ir_lower::emit::Emitter;
use crate::exec::vm::ir_lower::units::LowerCx;
use crate::exec::vm::program::{SplitKind, SplitSpec};
use crate::exec::vm::window_step::WindowStep;

/// Registers the window `data`, which `shape` states, in the pools of `reads`, and answers the
/// index of its split in the program. The pipeline its rows run through is set when that pipeline
/// is lowered.
pub(super) fn push_window(
	em: &mut Emitter,
	cx: &mut LowerCx<'_>,
	ir: &Ir,
	reads: &mut Reads,
	data: &OpData,
	shape: &WindowShape,
) -> Result<u16, IrError> {
	let regions = data.regions.as_slice();
	if regions.len() != shape.key_regions() + shape.arg_regions() {
		return Err(invariant("a window whose regions disagree with its keys and arguments"));
	}
	if shape.calls.is_empty() {
		return Err(invariant("a window with no call"));
	}
	let (key_regions, arg_regions) = regions.split_at(shape.key_regions());
	let mut needs = Needs::none();
	let mut scalars = |regions: &[_]| {
		regions
			.iter()
			.map(|region| row_scalar(em, cx, ir, *region, &mut needs))
			.collect::<Result<Vec<_>, IrError>>()
	};
	let keys = scalars(key_regions)?;
	let args = scalars(arg_regions)?;
	reads.needs = reads.needs.merged(&needs);
	let id = pool_index(reads.base.splits, reads.splits.len(), "vm splits")?;
	reads.splits.push(SplitSpec {
		kind: SplitKind::Window(WindowStep {
			shape: shape.clone(),
			keys,
			args,
		}),
		pipeline: 0,
		build_before: None,
	});
	Ok(id)
}
