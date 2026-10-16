//! The lowering of the statements that are not data statements: schema statements, `INFO`,
//! `LIVE`, `KILL`, `USE`, `OPTION`, `ACCESS` and `SHOW`.
//!
//! Shared lowering reaches these only through [`SchemaLowering`], held by the lowering context as
//! an `Option`. The context an entry builds from its [`Surface`](crate::exec::surface::Surface)
//! holds [`FULL_SCHEMA_LOWERING`] or nothing, so an entry that serves data statements alone names
//! none of the functions [`FullSchemaLowering`] forwards to. Where the hook is absent a statement
//! of these families is refused with [`NOT_SERVED`]. A unit lowered under a context (a function
//! body, a closure, a subquery, an event) inherits the hook, so stored code is refused the same
//! way.

use super::access::{lower_access, lower_show};
use super::ddl::{Reservations, lower_ddl};
use super::emit::Emitter;
use super::expr::{Claim, Needs};
use super::info::{InfoLowered, lower_info, lower_kill, lower_live, lower_option, lower_use};
use super::units::LowerCx;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::vm::program::Pipeline;

/// The refusal of a statement the entry's surface leaves out.
pub(crate) const NOT_SERVED: &str = "ir_lower: a schema, INFO, LIVE, KILL, USE, OPTION, ACCESS or SHOW statement is not served by this entry";

/// The lowering `cx` holds for the statements that are not data statements, or [`NOT_SERVED`]
/// where its entry serves data statements alone.
pub(super) fn served(cx: &LowerCx<'_>) -> Result<&'static (dyn SchemaLowering + Sync), IrError> {
	cx.schema().ok_or(IrError::NotYet {
		what: NOT_SERVED,
	})
}

/// Lowers the statements that are not data statements. Each method lowers the one statement
/// family its name states, with the contract of the function it forwards to.
pub(crate) trait SchemaLowering {
	/// Lowers the schema statement `op` ([`lower_ddl`]).
	fn ddl(
		&self,
		em: &mut Emitter,
		ir: &Ir,
		facts: &FactCache,
		cx: &mut LowerCx<'_>,
		op: OpId,
		reservations: &mut Reservations,
	) -> Result<Needs, IrError>;

	/// Lowers the `phys.info` `op` ([`lower_info`]).
	fn info(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError>;

	/// Lowers the `phys.kill` `op` ([`lower_kill`]).
	fn kill(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError>;

	/// Lowers the `phys.live` `op` ([`lower_live`]).
	fn live(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError>;

	/// Lowers the `phys.option` `op` ([`lower_option`]).
	fn option(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError>;

	/// Lowers the `phys.use` `op` ([`lower_use`]).
	fn use_(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError>;

	/// Lowers the `phys.access` `op` ([`lower_access`]).
	fn access(
		&self,
		cx: &mut LowerCx<'_>,
		em: &mut Emitter,
		ir: &Ir,
		op: OpId,
	) -> Result<(Pipeline, Needs, Vec<Claim>), IrError>;

	/// Lowers the `phys.show` `op` ([`lower_show`]).
	fn show(&self, em: &mut Emitter, ir: &Ir, op: OpId) -> Result<Pipeline, IrError>;
}

/// The lowering of every statement family: each method is the function it names.
pub(crate) struct FullSchemaLowering;

/// The one [`FullSchemaLowering`], named only by the surface that serves every statement family.
pub(crate) static FULL_SCHEMA_LOWERING: FullSchemaLowering = FullSchemaLowering;

impl SchemaLowering for FullSchemaLowering {
	fn ddl(
		&self,
		em: &mut Emitter,
		ir: &Ir,
		facts: &FactCache,
		cx: &mut LowerCx<'_>,
		op: OpId,
		reservations: &mut Reservations,
	) -> Result<Needs, IrError> {
		lower_ddl(em, ir, facts, cx, op, reservations)
	}

	fn info(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError> {
		lower_info(em, cx, ir, op)
	}

	fn kill(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError> {
		lower_kill(em, cx, ir, op)
	}

	fn live(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError> {
		lower_live(em, cx, ir, op)
	}

	fn option(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError> {
		lower_option(em, cx, ir, op)
	}

	fn use_(
		&self,
		em: &mut Emitter,
		cx: &mut LowerCx<'_>,
		ir: &Ir,
		op: OpId,
	) -> Result<InfoLowered, IrError> {
		lower_use(em, cx, ir, op)
	}

	fn access(
		&self,
		cx: &mut LowerCx<'_>,
		em: &mut Emitter,
		ir: &Ir,
		op: OpId,
	) -> Result<(Pipeline, Needs, Vec<Claim>), IrError> {
		lower_access(cx, em, ir, op)
	}

	fn show(&self, em: &mut Emitter, ir: &Ir, op: OpId) -> Result<Pipeline, IrError> {
		lower_show(em, ir, op)
	}
}
