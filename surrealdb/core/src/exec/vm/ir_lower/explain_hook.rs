//! The hook through which shared lowering reaches the lowering of `EXPLAIN`.
//!
//! Shared lowering reaches the rendering of a plan and the analysis of a statement only through
//! [`ExplainLowering`], held by the lowering context as an `Option`. The context an entry builds
//! from its [`Surface`](crate::exec::surface::Surface) holds [`FULL_EXPLAIN_LOWERING`] or nothing,
//! so an entry that does not serve `EXPLAIN` names none of the functions [`FullExplainLowering`]
//! forwards to. Where the hook is absent an `EXPLAIN` is refused with [`NOT_SERVED_EXPLAIN`]. A
//! unit lowered under a context (a function body, a closure, a subquery, an event) inherits the
//! hook, so stored code is refused the same way.
//!
//! This module also holds what shared lowering needs to find an `EXPLAIN` and to name what one
//! lowers to ([`explain_in`], [`explain_of`], [`Purpose`], [`Explained`]): a tag test and plain
//! data, which reach none of the code that builds a tree.

use super::emit::Emitter;
use super::explain;
use super::expr::{Claim, Needs};
use super::pipe::{PoolBase, ReadScope};
use super::place::Lowered;
use super::units::LowerCx;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{OpId, RegionId};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;
use crate::exec::vm::program::{Analysis, Pipeline};

/// The refusal of an `EXPLAIN` the entry's surface leaves out.
pub(crate) const NOT_SERVED_EXPLAIN: &str =
	"ir_lower: an EXPLAIN statement is not served by this entry";

/// The lowering `cx` holds for `EXPLAIN`, or [`NOT_SERVED_EXPLAIN`] where its entry does not serve
/// it.
pub(super) fn explained_served(
	cx: &LowerCx<'_>,
) -> Result<&'static (dyn ExplainLowering + Sync), IrError> {
	cx.explain().ok_or(IrError::NotYet {
		what: NOT_SERVED_EXPLAIN,
	})
}

/// Lowers `EXPLAIN`. Each method lowers the one form its name states, with the contract of the
/// function it forwards to.
pub(crate) trait ExplainLowering {
	/// Lowers the container `ir`, whose `phys.explain` `explain` is a plain `EXPLAIN`
	/// ([`explain::lower_plain`]).
	fn lower_plain(&self, cx: &LowerCx<'_>, ir: &Ir, explain: OpId) -> Result<Lowered, IrError>;

	/// Lowers the `EXPLAIN` the statement `region` holds, whose op is `explain`, into `em`
	/// ([`explain::lower_explained`]).
	#[allow(clippy::too_many_arguments)]
	fn lower_explained(
		&self,
		cx: &mut LowerCx<'_>,
		em: &mut Emitter,
		ir: &Ir,
		region: RegionId,
		explain: OpId,
		base: PoolBase,
		purpose: Purpose,
	) -> Result<Explained, IrError>;

	/// What `EXPLAIN ANALYZE` of the statement `explain` explains renders, its tree built for
	/// `purpose` ([`explain::analysis_in`]).
	fn analysis(
		&self,
		cx: &LowerCx<'_>,
		ir: &Ir,
		explain: OpId,
		purpose: Purpose,
	) -> Result<Analysis, IrError>;

	/// The name of the operator at the top of the plan `EXPLAIN` renders for the statement
	/// `region` holds ([`explain::region_root_name`]).
	fn region_root_name(&self, ir: &Ir, region: RegionId) -> Option<String>;
}

/// The lowering of `EXPLAIN`: each method is the function it names.
pub(crate) struct FullExplainLowering;

/// The one [`FullExplainLowering`], named only by the surface that serves `EXPLAIN`.
pub(crate) static FULL_EXPLAIN_LOWERING: FullExplainLowering = FullExplainLowering;

impl ExplainLowering for FullExplainLowering {
	fn lower_plain(&self, cx: &LowerCx<'_>, ir: &Ir, explain: OpId) -> Result<Lowered, IrError> {
		explain::lower_plain(cx, ir, explain)
	}

	#[allow(clippy::too_many_arguments)]
	fn lower_explained(
		&self,
		cx: &mut LowerCx<'_>,
		em: &mut Emitter,
		ir: &Ir,
		region: RegionId,
		explain: OpId,
		base: PoolBase,
		purpose: Purpose,
	) -> Result<Explained, IrError> {
		explain::lower_explained(cx, em, ir, region, explain, base, purpose)
	}

	fn analysis(
		&self,
		cx: &LowerCx<'_>,
		ir: &Ir,
		explain: OpId,
		purpose: Purpose,
	) -> Result<Analysis, IrError> {
		explain::analysis_in(cx, ir, explain, purpose)
	}

	fn region_root_name(&self, ir: &Ir, region: RegionId) -> Option<String> {
		explain::region_root_name(ir, region)
	}
}

/// What the tree is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Purpose {
	/// `EXPLAIN`: the plan alone.
	Plan,
	/// `EXPLAIN ANALYZE` of a statement: every node reports the one set of metrics the run
	/// records, and a bitmap tree the run decided by is measured by evaluating it again after the
	/// run.
	Measured,
	/// `EXPLAIN ANALYZE` of a statement a block holds among others: every node reports the one
	/// set of metrics that run records, and a bitmap tree is described by its nodes alone.
	Described,
}

/// What an `EXPLAIN` that a block's statement holds lowers to.
pub(crate) enum Explained {
	/// A plain `EXPLAIN`: the one pipeline that answers the rendering of the plan, as a constant.
	/// Nothing of the explained statement runs.
	Plain(Pipeline),
	/// `EXPLAIN ANALYZE`: the explained statement as a read group, and what its run renders.
	Analysed {
		scope: ReadScope,
		analysis: Analysis,
	},
	/// `EXPLAIN ANALYZE` of a statement that reads no rows (an expression, a control-flow
	/// statement, a plain `EXPLAIN`): one pipeline over [`Source::Once`] that evaluates the
	/// explained value and emits it as its one row, with the instructions each op emitted for
	/// itself, what it needs of the program and what its run renders. A `RETURN`, `BREAK` or
	/// `CONTINUE` the statement signals ends the run with no row.
	AnalysedValue {
		pipeline: Pipeline,
		claims: Vec<Claim>,
		needs: Needs,
		analysis: Analysis,
	},
}

/// The `phys.explain` that heads the container `ir`, when it holds one in its top region.
pub(super) fn explain_in(ir: &Ir) -> Option<(OpId, bool)> {
	explain_of(ir, ir.top())
}

/// The `phys.explain` that `region` holds, with whether it is `ANALYZE`: the outermost one, which
/// no other `phys.explain` of the region explains, where an `EXPLAIN` explains another.
pub(super) fn explain_of(ir: &Ir, region: RegionId) -> Option<(OpId, bool)> {
	ops_in(ir, region).find_map(|op| {
		let data = ir.op(op)?;
		let OpKind::Phys(PhysOp::Explain {
			analyze,
			..
		}) = &data.kind
		else {
			return None;
		};
		let explained = data.results.iter().any(|result| {
			ir.uses(*result).iter().any(|read| {
				matches!(
					ir.op(read.user).map(|user| &user.kind),
					Some(OpKind::Phys(PhysOp::Explain { .. }))
				)
			})
		});
		(!explained).then_some((op, *analyze))
	})
}
