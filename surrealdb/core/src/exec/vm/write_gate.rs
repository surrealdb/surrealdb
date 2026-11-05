//! The host's side of the gate stages run as instructions: CheckRecordUserAccess,
//! CheckPermissionsQuickCreate, the create, update and delete permission checks,
//! RecheckUpdatePermissions and CheckSelectPermissions.
//!
//! The instructions act on the record in flight through the write state; an
//! expression they evaluate is a host pipeline over one of the record's
//! images (see `doc/executor/VM_WRITE_STAGES.md`).
//!
//! What a gate decides from the table's declaration — which clause, and
//! whether it is `FULL`, `NONE` or a predicate — is settled when the statement
//! is planned. What it decides from the caller and the record is decided here,
//! per record: whether the record has an id, whether the caller's reads or
//! writes are permission-checked at all (false for an exempt principal and
//! inside a permission predicate), and the predicate's verdict.

use super::{State, internal, run_image};
use crate::exec::FlowResult;
use crate::exec::operators::write::stage::RecordImage;
use crate::exec::permission::{should_check_perms, validate_record_user_access};
use crate::exec::vm::program::{Label, Program};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;

/// An instruction of the gate stages.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::exec::vm) enum GateInstr {
	/// Fail the statement when the caller's authenticated level does not reach
	/// the namespace and database the statement writes to: tenant confinement,
	/// which the session's selected namespace and database do not decide.
	Tenant,
	/// Jump to `target` when the record has no id: a temporary record, which
	/// no gate judges.
	JumpIfNoId {
		target: Label,
	},
	/// Jump to `target` when the caller's `action`s on the database are not
	/// permission-checked.
	JumpIfUnchecked {
		action: Action,
		target: Label,
	},
	/// Judge a copy of `image` with host pipeline
	/// [`Program::host_code`]`[plain]`, which answers the permission's verdict
	/// over its row, and jump to `skip` when the verdict is falsy. With
	/// `derived`, the copy is judged by that pipeline instead, which derives
	/// the `COMPUTED` fields the permission reads on it before the verdict,
	/// whatever the image carries under their names: values the statement
	/// assigned, or derived for the caller. The image itself is stored or
	/// shown as it stands, so they are never derived on it.
	Judge {
		image: RecordImage,
		plain: u16,
		derived: Option<u16>,
		skip: Label,
	},
}

assert_plain!(GateInstr);

impl GateInstr {
	/// The jump target the instruction holds, for the lowering to patch.
	pub(in crate::exec::vm) fn target_mut(&mut self) -> Option<&mut Label> {
		match self {
			GateInstr::Tenant => None,
			GateInstr::JumpIfNoId {
				target,
			}
			| GateInstr::JumpIfUnchecked {
				target,
				..
			}
			| GateInstr::Judge {
				skip: target,
				..
			} => Some(target),
		}
	}
}

/// Take `instr` for the record in flight, and answer the instruction the body
/// continues at: `next`, or a target the instruction holds.
pub(super) async fn step(
	program: &Program,
	instr: &GateInstr,
	next: usize,
	state: &mut State,
) -> FlowResult<usize> {
	let write =
		state.write.as_deref_mut().ok_or_else(|| internal("a write body with no write state"))?;
	let (row, ctx) = write.in_flight()?;
	match instr {
		GateInstr::Tenant => {
			let db_ctx =
				ctx.exec().database().context("the tenant guard requires a database context")?;
			validate_record_user_access(db_ctx).map_err(|e| ControlFlow::Err(e.into()))?;
			Ok(next)
		}
		GateInstr::JumpIfNoId {
			target,
		} => Ok(if row.id.is_none() {
			*target as usize
		} else {
			next
		}),
		GateInstr::JumpIfUnchecked {
			action,
			target,
		} => {
			let db_ctx =
				ctx.exec().database().context("a permission gate requires a database context")?;
			let checked =
				should_check_perms(db_ctx, *action).map_err(|e| ControlFlow::Err(e.into()))?;
			Ok(if checked {
				next
			} else {
				*target as usize
			})
		}
		GateInstr::Judge {
			image,
			plain,
			derived,
			skip,
		} => {
			let subject = match image {
				RecordImage::Current => row.current.clone(),
				RecordImage::Initial => row.initial.clone(),
			};
			let pipeline = derived.unwrap_or(*plain);
			let verdict = run_image(program, pipeline, subject, None, state, ctx.exec()).await?;
			Ok(if verdict.is_truthy() {
				next
			} else {
				*skip as usize
			})
		}
	}
}
