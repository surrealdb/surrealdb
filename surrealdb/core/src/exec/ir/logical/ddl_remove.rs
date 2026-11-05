//! Planning `REMOVE` and `REBUILD INDEX`: the operands and attributes of each form.
//!
//! Operands are allocated in the order the form's body evaluates them (see
//! [`guard_order`](super::ddl::guard_order)). `IF EXISTS` and `EXPUNGE` are attributes of the op.

#![deny(clippy::wildcard_enum_match_arm)]

use super::ddl::{DdlForm, DdlOperand, DdlPlan, Planned, Slots};
use super::ops::{LogOp, RebuildAttrs, RemoveAttrs};
use crate::exec::ir::iface::statements::{DdlChecks, RemoveObject};
use crate::expr::Base;
use crate::expr::statements::RemoveStatement;
use crate::expr::statements::rebuild::RebuildStatement;
use crate::iam::{Action, ResourceKind};

const EDIT: Action = Action::Edit;

/// Plans a `REMOVE`.
pub(super) fn plan_remove(remove: &RemoveStatement, checks: DdlChecks) -> Planned<DdlPlan<'_>> {
	let mut s = Slots::new();
	let (form, allow, object, if_exists, expunge) = match remove {
		RemoveStatement::Namespace(stmt) => (
			DdlForm::RemoveNamespace,
			(
				EDIT,
				if stmt.expunge {
					ResourceKind::Any
				} else {
					ResourceKind::Namespace
				},
				Base::Root,
			),
			RemoveObject::Namespace {
				name: s.name(DdlOperand::Name, &stmt.name)?,
			},
			stmt.if_exists,
			stmt.expunge,
		),
		RemoveStatement::Database(stmt) => (
			DdlForm::RemoveDatabase,
			(
				EDIT,
				if stmt.expunge {
					ResourceKind::Any
				} else {
					ResourceKind::Database
				},
				Base::Ns,
			),
			RemoveObject::Database {
				name: s.name(DdlOperand::Name, &stmt.name)?,
			},
			stmt.if_exists,
			stmt.expunge,
		),
		RemoveStatement::Function(stmt) => (
			DdlForm::RemoveFunction,
			(EDIT, ResourceKind::Function, Base::Db),
			RemoveObject::Function {
				name: stmt.name.clone(),
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Analyzer(stmt) => (
			DdlForm::RemoveAnalyzer,
			(EDIT, ResourceKind::Analyzer, Base::Db),
			RemoveObject::Analyzer {
				name: s.name(DdlOperand::Name, &stmt.name)?,
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Access(stmt) => (
			DdlForm::RemoveAccess,
			(EDIT, ResourceKind::Actor, stmt.base),
			RemoveObject::Access {
				name: s.name(DdlOperand::Name, &stmt.name)?,
				base: stmt.base,
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Param(stmt) => (
			DdlForm::RemoveParam,
			(EDIT, ResourceKind::Parameter, Base::Db),
			RemoveObject::Param {
				name: stmt.name.clone(),
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Table(stmt) => (
			DdlForm::RemoveTable,
			(
				EDIT,
				if stmt.expunge {
					ResourceKind::Any
				} else {
					ResourceKind::Table
				},
				Base::Db,
			),
			RemoveObject::Table {
				name: s.name(DdlOperand::Name, &stmt.name)?,
			},
			stmt.if_exists,
			stmt.expunge,
		),
		RemoveStatement::Event(stmt) => {
			let table = s.name(DdlOperand::Table, &stmt.table_name)?;
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			(
				DdlForm::RemoveEvent,
				(EDIT, ResourceKind::Event, Base::Db),
				RemoveObject::Event {
					name,
					table,
				},
				stmt.if_exists,
				false,
			)
		}
		RemoveStatement::Field(stmt) => {
			let table = s.name(DdlOperand::Table, &stmt.table_name)?;
			let name = s.field_name(&stmt.name)?;
			(
				DdlForm::RemoveField,
				(EDIT, ResourceKind::Field, Base::Db),
				RemoveObject::Field {
					name,
					table,
				},
				stmt.if_exists,
				false,
			)
		}
		RemoveStatement::Index(stmt) => {
			let name = s.name(DdlOperand::Name, &stmt.name)?;
			let table = s.name(DdlOperand::Table, &stmt.what)?;
			(
				DdlForm::RemoveIndex,
				(EDIT, ResourceKind::Index, Base::Db),
				RemoveObject::Index {
					name,
					table,
				},
				stmt.if_exists,
				false,
			)
		}
		RemoveStatement::User(stmt) => (
			DdlForm::RemoveUser,
			(EDIT, ResourceKind::Actor, stmt.base),
			RemoveObject::User {
				name: s.name(DdlOperand::Name, &stmt.name)?,
				base: stmt.base,
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Model(stmt) => (
			DdlForm::RemoveModel,
			(EDIT, ResourceKind::Model, Base::Db),
			RemoveObject::Model {
				name: stmt.name.clone(),
				version: stmt.version.clone(),
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Api(stmt) => (
			DdlForm::RemoveApi,
			(EDIT, ResourceKind::Api, Base::Db),
			RemoveObject::Api {
				name: s.name(DdlOperand::Name, &stmt.name)?,
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Type(stmt) => (
			DdlForm::RemoveType,
			(EDIT, ResourceKind::Parameter, Base::Db),
			RemoveObject::Type {
				name: stmt.name.clone(),
				object: stmt.object,
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Bucket(stmt) => (
			DdlForm::RemoveBucket,
			(EDIT, ResourceKind::Bucket, stmt.base),
			RemoveObject::Bucket {
				name: s.name(DdlOperand::Name, &stmt.name)?,
				base: stmt.base,
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Sequence(stmt) => (
			DdlForm::RemoveSequence,
			(EDIT, ResourceKind::Sequence, Base::Db),
			RemoveObject::Sequence {
				name: s.name(DdlOperand::Name, &stmt.name)?,
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Module(stmt) => (
			DdlForm::RemoveModule,
			(EDIT, ResourceKind::Module, Base::Db),
			RemoveObject::Module {
				name: stmt.name.clone(),
			},
			stmt.if_exists,
			false,
		),
		RemoveStatement::Config(stmt) => {
			let base = crate::ddl::define::config::config_kind_base(&stmt.kind);
			(
				DdlForm::RemoveConfig,
				(EDIT, ResourceKind::Config(stmt.kind.clone()), base.into()),
				RemoveObject::Config {
					kind: stmt.kind.clone(),
				},
				stmt.if_exists,
				false,
			)
		}
	};
	let op = LogOp::Remove(Box::new(RemoveAttrs {
		object,
		if_exists,
		expunge,
		checks,
	}));
	Ok(s.finish(form, allow, op))
}

/// Plans a `REBUILD INDEX`, whose index and table are attributes.
pub(super) fn plan_rebuild(rebuild: &RebuildStatement) -> Planned<DdlPlan<'_>> {
	let s = Slots::new();
	match rebuild {
		RebuildStatement::Index(stmt) => {
			let op = LogOp::Rebuild(Box::new(RebuildAttrs {
				index: stmt.name.clone(),
				table: stmt.table.clone(),
				if_exists: stmt.if_exists,
				concurrently: stmt.concurrently,
			}));
			Ok(s.finish(DdlForm::RebuildIndex, (EDIT, ResourceKind::Index, Base::Db), op))
		}
	}
}
