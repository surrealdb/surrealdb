//! Lowering of `phys.show` and `phys.access`: `SHOW CHANGES` and the four `ACCESS` statements,
//! each the one pipeline of its statement's program.
//!
//! # Contract
//!
//! - `SHOW CHANGES` is a pipeline over [`Source::Changes`] whose body hands each change set on as a
//!   row, so its value is the array of them. Opening the source checks the session's view of the
//!   database's tables before it resolves the namespace and the database, so the program requires
//!   no context level of its own, and it reads only.
//! - `ACCESS ... GRANT`, `SHOW`, `REVOKE` and `PURGE` are one scalar pipeline over no source whose
//!   store instruction ([`StoreInstr::IssueGrant`], [`StoreInstr::ShowGrants`],
//!   [`StoreInstr::RevokeGrants`], [`StoreInstr::PurgeGrants`]) makes the statement's checks, reads
//!   or writes the grants and answers its value, which the pipeline emits. A grant's record subject
//!   is evaluated first, before any check. The program requires no context level of its own: each
//!   instruction resolves the base and raises a missing namespace or database after its permission
//!   check.
//! - The `Clause(Where)` region of a `SHOW` or a `REVOKE` is a host pipeline whose one row is the
//!   grant in its redacted, shown form. The instruction runs it once per grant. The plaintext key
//!   of a grant is never the row the filter reads.
//! - `GRANT`, `REVOKE` and `PURGE` write: their program runs in a transaction that may write, and
//!   the instruction is preceded by [`StoreInstr::RealRunOnly`], so a speculative run issues,
//!   revokes and deletes nothing. `SHOW` reads only.
//!
//! # What lowering decides
//!
//! Nothing. The access method, the base, the grant named, the purge's kind and grace and the
//! subject's shape are attributes of the op; the subject is its one operand and the filter its one
//! region.

#![deny(clippy::wildcard_enum_match_arm)]

use super::super::program::{GrantSubject, Instr, Operand, Pipeline, ROW, Source, StoreInstr};
use super::emit::Emitter;
use super::expr::{Claim, Lowerer, Needs};
use super::units::LowerCx;
use crate::exec::AccessMode;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::OpId;
use crate::exec::ir::iface::statements::{AccessOp, AccessSubjectShape};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysOp;

/// Lowers the `phys.show` `op` of `ir` into `em` and ends the pipeline it emitted: the rows of
/// the change feed, each handed on as it is. The op takes no operand and no region.
pub(super) fn lower_show(em: &mut Emitter, ir: &Ir, op: OpId) -> Result<Pipeline, IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::access: op"))?;
	let OpKind::Phys(PhysOp::Show(attrs)) = &data.kind else {
		return Err(IrError::Stale("ir_lower::access: not a phys.show"));
	};
	if !data.operands.is_empty() || !data.regions.is_empty() {
		return Err(IrError::Stale("ir_lower::access: phys.show arity"));
	}
	em.emit(Instr::Emit {
		src: ROW,
	});
	em.emit(Instr::EndRow);
	em.finish_pipeline(Source::Changes {
		table: attrs.table.clone(),
		since: attrs.since.clone(),
		limit: attrs.limit,
	})
}

/// Lowers the `phys.access` `op` of `ir` into `em` under the lowering context `cx`, and ends the
/// pipeline it emitted. Answers the pipeline, what the program needs of its run (a transaction
/// that may write for every form but `SHOW`), and the instructions emitted by the subject's ops.
///
/// The operands of the op are the record a `GRANT` is issued to, for a record subject, and none
/// otherwise; its regions are the `WHERE` of a `SHOW` or a `REVOKE`, when written. Any other arity
/// is [`IrError::Stale`].
pub(super) fn lower_access(
	cx: &mut LowerCx<'_>,
	em: &mut Emitter,
	ir: &Ir,
	op: OpId,
) -> Result<(Pipeline, Needs, Vec<Claim>), IrError> {
	let data = ir.op(op).ok_or(IrError::Stale("ir_lower::access: op"))?;
	let OpKind::Phys(PhysOp::Access(attrs)) = &data.kind else {
		return Err(IrError::Stale("ir_lower::access: not a phys.access"));
	};
	let ac = attrs.ac.to_string();
	let base = attrs.base;
	let mut lowerer = Lowerer::in_context(em, ir, cx);
	let dst = lowerer.em.reg()?;
	let (instr, writes) = match &attrs.op {
		AccessOp::Grant {
			subject,
		} => {
			let subject = match (subject, data.operands.as_slice(), data.regions.as_slice()) {
				(AccessSubjectShape::User(name), [], []) => GrantSubject::User(name.to_string()),
				(AccessSubjectShape::Record, [record], []) => {
					let src = lowerer.value(*record)?;
					let reg = lowerer.em.reg()?;
					lowerer.emit(Instr::Load {
						dst: reg,
						src,
					});
					GrantSubject::Record(reg)
				}
				_ => return Err(IrError::Stale("ir_lower::access: phys.access arity")),
			};
			(
				StoreInstr::IssueGrant {
					ac,
					base,
					subject,
					dst,
				},
				true,
			)
		}
		AccessOp::Show {
			gr,
		} => (
			StoreInstr::ShowGrants {
				ac,
				base,
				gr: gr.as_ref().map(ToString::to_string),
				filter: filter_of(&mut lowerer, data)?,
				dst,
			},
			false,
		),
		AccessOp::Revoke {
			gr,
		} => (
			StoreInstr::RevokeGrants {
				ac,
				base,
				gr: gr.as_ref().map(ToString::to_string),
				filter: filter_of(&mut lowerer, data)?,
				dst,
			},
			true,
		),
		AccessOp::Purge {
			kind,
			grace,
		} => {
			if !data.operands.is_empty() || !data.regions.is_empty() {
				return Err(IrError::Stale("ir_lower::access: phys.access arity"));
			}
			(
				StoreInstr::PurgeGrants {
					ac,
					base,
					kind: kind.clone(),
					grace: *grace,
					dst,
				},
				true,
			)
		}
	};
	if writes {
		lowerer.emit(Instr::Store(StoreInstr::RealRunOnly));
	}
	lowerer.emit(Instr::Store(instr));
	lowerer.emit(Instr::EmitEnd {
		src: Operand::Reg(dst),
	});
	let mut needs = lowerer.needs();
	let claims = lowerer.take_claims();
	if writes {
		needs.access_mode = needs.access_mode.combine(AccessMode::ReadWrite);
	}
	Ok((em.finish_pipeline(Source::Once)?, needs, claims))
}

/// The host pipeline of the `WHERE` region of a `SHOW` or a `REVOKE` in `data`, over the grant as
/// its row; `None` where none was written. The op takes no operand.
fn filter_of(lowerer: &mut Lowerer<'_>, data: &OpData) -> Result<Option<u16>, IrError> {
	if !data.operands.is_empty() {
		return Err(IrError::Stale("ir_lower::access: phys.access arity"));
	}
	match data.regions.as_slice() {
		[] => Ok(None),
		[region] => lowerer.host_code_over_row(*region).map(Some),
		_ => Err(IrError::Stale("ir_lower::access: phys.access arity")),
	}
}
