use std::sync::Arc;

use reblessive::tree::Stk;

use super::IgnoreError;
use crate::catalog::{FieldDefinition, Permission, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::expr::{FlowResultExt, Idiom, SelectStatement};
use crate::iam::Action;
use crate::val::Value;

impl Document {
	#[instrument(level = "trace", name = "Document::select", skip_all)]
	pub(super) async fn select(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		stmt: &SelectStatement,
		omit: &[Idiom],
		table: Option<Arc<TableDefinition>>,
		table_fields: Option<Arc<[FieldDefinition]>>,
	) -> Result<Value, IgnoreError> {
		self.check_record_exists()?;
		check_select_permissions_quick(opt, table.as_ref())?;
		self.check_select_where_condition(stk, ctx, opt, stmt).await?;
		check_select_permissions_table(stk, ctx, opt, table.as_ref(), &self.current).await?;
		self.pluck_select(stk, ctx, opt, stmt, omit, table_fields.as_ref()).await
	}
}

#[instrument(level = "trace", name = "Document::check_select_permissions_quick", skip_all)]
fn check_select_permissions_quick(
	opt: &Options,
	table: Option<&Arc<TableDefinition>>,
) -> Result<(), IgnoreError> {
	// Should we run permissions checks?
	if !opt.check_perms(Action::View)? {
		return Ok(());
	}

	let Some(table) = table else {
		return Ok(());
	};

	// Exit early if permissions are NONE
	if table.permissions.select.is_none() {
		return Err(IgnoreError::Ignore);
	}

	Ok(())
}

/// Checks the `PERMISSIONS` clause on the table for
/// this record, processing all advanced permissions
/// clauses and evaluating the expression. This
/// function checks and evaluates `FULL`, `NONE`,
/// and specific permissions clauses on the table.
#[instrument(level = "trace", name = "Document::check_select_permissions_table", skip_all)]
pub(super) async fn check_select_permissions_table(
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	table: Option<&Arc<TableDefinition>>,
	current: &CursorDoc,
) -> Result<(), IgnoreError> {
	if !opt.check_perms(Action::View)? {
		return Ok(());
	}

	// Check that record authentication matches session
	if opt.auth.is_record() {
		let ns = opt.ns()?;
		if opt.auth.level().ns() != Some(ns) {
			return Err(IgnoreError::from(anyhow::Error::new(Error::NsNotAllowed {
				ns: ns.into(),
			})));
		}
		let db = opt.db()?;
		if opt.auth.level().db() != Some(db) {
			return Err(IgnoreError::from(anyhow::Error::new(Error::DbNotAllowed {
				db: db.into(),
			})));
		}
	}

	let Some(table) = table else {
		return Ok(());
	};
	// Get the permission clause
	let perms = &table.permissions.select;
	// Process the table permissions
	match perms {
		Permission::None => return Err(IgnoreError::Ignore),
		Permission::Full => return Ok(()),
		Permission::Specific(e) => {
			// Disable permissions
			let opt = &opt.new_with_perms(false);
			// Process the PERMISSION clause
			if !stk
				.run(|stk| e.compute(stk, ctx, opt, Some(current)))
				.await
				.catch_return()?
				.is_truthy()
			{
				return Err(IgnoreError::Ignore);
			}
		}
	}

	// Carry on
	Ok(())
}
