use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use super::IgnoreError;
use crate::catalog::Permission;
use crate::ctx::Context;
use crate::dbs::{Options, Statement, Workable};
use crate::doc::Document;
use crate::doc::Permitted::*;
use crate::doc::compute::DocKind;
use crate::err::Error;
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{FlowResultExt as _, Part};
use crate::iam::Action;
use crate::val::{RecordId, Value};

impl Document {
	/// Checks whether this operation is allowed on
	/// the table for this document. When inserting
	/// an edge or relation, we check that the table
	/// type is `ANY` or `RELATION`. When inserting
	/// a node or normal record, we check that the
	/// table type is `ANY` or `NORMAL`.
	pub(super) async fn check_table_type(
		&mut self,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		// Get the table for this document
		let tb = match &self.tb {
			Some(tb) => Arc::clone(tb),
			None => self.tb(ctx, opt).await?,
		};
		// Determine the type of statement
		match stm {
			Statement::Create(_) => {
				ensure!(
					tb.allows_normal(),
					Error::TableCheck {
						record: self.id()?.to_sql(),
						relation: false,
						target_type: tb.table_type.to_sql(),
					}
				);
			}
			Statement::Upsert(_) => {
				ensure!(
					tb.allows_normal(),
					Error::TableCheck {
						record: self.id()?.to_sql(),
						relation: false,
						target_type: tb.table_type.to_sql(),
					}
				);
			}
			Statement::Relate(_) => {
				ensure!(
					tb.allows_relation(),
					Error::TableCheck {
						record: self.id()?.to_sql(),
						relation: true,
						target_type: tb.table_type.to_sql(),
					}
				);
			}
			Statement::Insert(_) => match self.extras {
				Workable::Relate(_, _, _) => {
					ensure!(
						tb.allows_relation(),
						Error::TableCheck {
							record: self.id()?.to_sql(),
							relation: true,
							target_type: tb.table_type.to_sql(),
						}
					);
				}
				_ => {
					ensure!(
						tb.allows_normal(),
						Error::TableCheck {
							record: self.id()?.to_sql(),
							relation: false,
							target_type: tb.table_type.to_sql(),
						}
					);
				}
			},
			_ => {}
		}
		// Carry on
		Ok(())
	}
	/// Checks that a specifically selected record
	/// actually exists in the underlying datastore.
	/// If the user specifies a record directly
	/// using a Record ID, and that record does not
	/// exist, then this function will exit early.
	pub(super) async fn check_record_exists(
		&self,
		_ctx: &Context,
		_opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), IgnoreError> {
		// Check if this record exists
		if self.id.is_some() && self.current.doc.as_ref().is_none() {
			return Err(IgnoreError::Ignore);
		}
		// Carry on
		Ok(())
	}
	/// Checks that the fields of a document are
	/// correct. If an `id` field is specified then
	/// it will check that the `id` field does not
	/// conflict with the specified `id` field for
	/// this document process. In addition, it checks
	/// that the `in` and `out` fields, if specified,
	/// match the in and out values specified in the
	/// statement, or present in any record which
	/// is being updated.
	pub(super) async fn check_data_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		fn check(v: &Value, p: &[Part], r: &RecordId) -> Result<()> {
			match v.pick(p) {
				Value::RecordId(v) if v.key.is_range() => {
					bail!(Error::IdInvalid {
						value: v.to_sql(),
					})
				}
				Value::RecordId(v) if v.eq(r) => {}
				Value::None => {}
				v => {
					ensure!(
						r.key == v,
						Error::IdMismatch {
							value: v.to_sql()
						}
					)
				}
			}
			Ok(())
		}

		// Don't bother checking if we generated the document id
		if self.r#gen.is_some() {
			return Ok(());
		}
		// Get the record id
		let rid = self.id()?;

		// You cannot store a range id as the id field on a document
		ensure!(
			!rid.key.is_range(),
			Error::IdInvalid {
				value: rid.to_sql(),
			}
		);

		// Get the input data, needs to happen before the workable::relate borrow from self.extras
		let data = self.compute_input_data(stk, ctx, opt, stm).await?;
		if data.is_some_and(|x| x.is_patch()) {
			return Ok(());
		}

		// Get value from data
		let value = data.map(|x| x.value());

		// This is a CREATE, UPSERT, UPDATE statement
		if let Workable::Normal = &self.extras {
			// This is a CONTENT, MERGE or SET clause
			if let Some(value) = value {
				// Check if there is an id field specified
				check(value.as_ref(), ID.as_ref(), rid.as_ref())?;
			}
		}
		// This is a RELATE statement
		else if let Workable::Relate(l, r, v) = &self.extras {
			if let Some(value) = value {
				// Check if there is an id field specified
				check(value.as_ref(), ID.as_ref(), rid.as_ref())?;
				check(value.as_ref(), IN.as_ref(), l)?;
				check(value.as_ref(), OUT.as_ref(), r)?;
			}
			// This is a INSERT RELATION statement
			else if let Some(value) = v {
				check(value.as_ref(), ID.as_ref(), rid.as_ref())?;
				check(value.as_ref(), IN.as_ref(), l)?;
				check(value.as_ref(), OUT.as_ref(), r)?;
			}
		}
		// Carry on
		Ok(())
	}
	/// Checks that the `WHERE` condition on a query
	/// matches before proceeding with processing
	/// the document. This ensures that records from
	/// a table, or from an index can be filtered out
	/// before being included within the query output.
	pub(super) async fn check_where_condition(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), IgnoreError> {
		// Check if we have already processed a condition
		if !self.is_condition_checked() {
			// Check if a WHERE condition is specified
			if let Some(cond) = stm.cond() {
				// Process the permitted documents
				let current = if self.reduced(stk, ctx, opt, Current).await? {
					self.computed_fields(stk, ctx, opt, DocKind::CurrentReduced).await?;
					&self.current_reduced
				} else {
					self.computed_fields(stk, ctx, opt, DocKind::Current).await?;
					&self.current
				};
				// Check if the expression is truthy
				if !stk
					.run(|stk| cond.0.compute(stk, ctx, opt, Some(current)))
					.await
					.catch_return()?
					.is_truthy()
				{
					// Ignore this document
					return Err(IgnoreError::Ignore);
				}
			}
		}
		// Carry on
		Ok(())
	}
	/// Checks the `PERMISSIONS` clause for viewing a
	/// record, based on the `select` permissions for
	/// the table that this record belongs to. This
	/// function checks and evaluates `FULL`, `NONE`,
	/// and specific permissions clauses on the table.
	/// This function is used when outputting a record,
	/// ensuring that a user has the permission to view
	/// the record after it has been updated or modified.
	pub(super) async fn check_permissions_view(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), IgnoreError> {
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(Action::View)? {
				// Get the table for this document
				let tb = match &self.tb {
					Some(tb) => Arc::clone(tb),
					None => self.tb(ctx, opt).await?,
				};
				// Get the correct document to check
				let doc = match stm.is_delete() {
					true => &self.initial,
					false => &self.current,
				};
				// Process the table permissions
				match &tb.permissions.select {
					Permission::None => return Err(IgnoreError::Ignore),
					Permission::Full => (),
					Permission::Specific(e) => {
						// Disable permissions
						let opt = &opt.new_with_perms(false);
						// Process the PERMISSION clause
						if !stk
							.run(|stk| e.compute(stk, ctx, opt, Some(doc)))
							.await
							.catch_return()?
							.is_truthy()
						{
							return Err(IgnoreError::Ignore);
						}
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
	/// Checks the `PERMISSIONS` clause on the table
	/// for this record, returning immediately if the
	/// permissions are `NONE`. This function does not
	/// check any custom advanced table permissions,
	/// which should be checked at a later stage.
	pub(super) async fn check_permissions_quick(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), IgnoreError> {
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(stm.into())? {
				// Get the table for this document
				let tb = match &self.tb {
					Some(tb) => Arc::clone(tb),
					None => self.tb(ctx, opt).await?,
				};
				// Get the permissions for this table
				let perms = stm.permissions(&tb, self.is_new());
				// Exit early if permissions are NONE
				if perms.is_none() {
					return Err(IgnoreError::Ignore);
				}
			}
		}
		// Carry on
		Ok(())
	}
	/// Checks the `PERMISSIONS` clause on the table for
	/// this record, processing all advanced permissions
	/// clauses and evaluating the expression. This
	/// function checks and evaluates `FULL`, `NONE`,
	/// and specific permissions clauses on the table.
	pub(super) async fn check_permissions_table(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), IgnoreError> {
		// Check if this record exists
		if self.id.is_some() {
			// Should we run permissions checks?
			if opt.check_perms(stm.into())? {
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
				// Get the table definition
				let tb = match &self.tb {
					Some(tb) => Arc::clone(tb),
					None => self.tb(ctx, opt).await?,
				};
				// Get the permission clause
				let perms = stm.permissions(&tb, self.is_new());
				// Process the table permissions
				match perms {
					Permission::None => return Err(IgnoreError::Ignore),
					Permission::Full => return Ok(()),
					Permission::Specific(e) => {
						// Disable permissions
						let opt = &opt.new_with_perms(false);
						// Process the PERMISSION clause
						if !stk
							.run(|stk| {
								e.compute(
									stk,
									ctx,
									opt,
									Some(match stm.is_delete() {
										true => &self.initial,
										false => &self.current,
									}),
								)
							})
							.await
							.catch_return()?
							.is_truthy()
						{
							return Err(IgnoreError::Ignore);
						}
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}
