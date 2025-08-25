use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use super::IgnoreError;
use crate::catalog::Permission;
use crate::ctx::Context;
use crate::dbs::{Options, Statement, Workable};
use crate::doc::Document;
use crate::doc::Permitted::*;
use crate::doc::compute::DocKind;
use crate::err::Error;
use crate::expr::FlowResultExt as _;
use crate::expr::paths::{ID, IN, OUT};
use crate::iam::Action;
use crate::sql::ToSql;
use crate::val::Value;

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
		let tb = self.tb(ctx, opt).await?;
		// Determine the type of statement
		match stm {
			Statement::Create(_) => {
				ensure!(
					tb.allows_normal(),
					Error::TableCheck {
						thing: self.id()?.to_string(),
						relation: false,
						target_type: tb.table_type.to_sql(),
					}
				);
			}
			Statement::Upsert(_) => {
				ensure!(
					tb.allows_normal(),
					Error::TableCheck {
						thing: self.id()?.to_string(),
						relation: false,
						target_type: tb.table_type.to_sql(),
					}
				);
			}
			Statement::Relate(_) => {
				ensure!(
					tb.allows_relation(),
					Error::TableCheck {
						thing: self.id()?.to_string(),
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
							thing: self.id()?.to_string(),
							relation: true,
							target_type: tb.table_type.to_sql(),
						}
					);
				}
				_ => {
					ensure!(
						tb.allows_normal(),
						Error::TableCheck {
							thing: self.id()?.to_string(),
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
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		// Get the record id
		let rid = self.id()?;
		// Don't bother checking if we generated the document id
		if self.r#gen.is_some() {
			return Ok(());
		}
		// You cannot store a range id as the id field on a document
		ensure!(
			!rid.key.is_range(),
			Error::IdInvalid {
				value: rid.to_string(),
			}
		);
		// This is a CREATE, UPSERT, UPDATE statement
		if let Workable::Normal = &self.extras {
			// This is a CONTENT, MERGE or SET clause
			if let Some(data) = stm.data() {
				// Check if there is an id field specified
				match data.pick(stk, ctx, opt, "id").await? {
					// You cannot store a range id as the id field on a document
					Value::RecordId(v) if v.key.is_range() => {
						bail!(Error::IdInvalid {
							value: v.to_string(),
						})
					}
					// The id is a match, so don't error
					Value::RecordId(v) if v.eq(&rid) => {}
					Value::None => {}
					v => {
						ensure!(
							rid.key == v,
							Error::IdMismatch {
								value: v.to_string(),
							}
						)
					}
				}
			}
		}
		// This is a RELATE statement
		else if let Workable::Relate(l, r, v) = &self.extras {
			// This is a RELATE statement
			if let Some(data) = stm.data() {
				// Check that the 'id' field matches
				match data.pick(stk, ctx, opt, "id").await? {
					// You cannot store a range id as the id field on a document
					Value::RecordId(v) if v.key.is_range() => {
						bail!(Error::IdInvalid {
							value: v.to_string(),
						})
					}
					// The id field is a match, so don't error
					Value::RecordId(v) if v.eq(&rid) => (),
					// There was no id field specified
					Value::None => {}
					v => {
						ensure!(
							rid.key == v,
							Error::IdMismatch {
								value: v.to_string(),
							}
						)
					}
				}
				// Check that the 'in' field matches
				match data.pick(stk, ctx, opt, "in").await? {
					// You cannot store a range id as the in field on a document
					Value::RecordId(v) if v.key.is_range() => {
						bail!(Error::InInvalid {
							value: v.to_string(),
						})
					}
					// The in field is a match, so don't error
					Value::RecordId(v) if v.eq(l) => (),
					Value::None => {}
					v => {
						ensure!(
							l.key == v,
							Error::InMismatch {
								value: v.to_string(),
							}
						)
					}
				}
				// Check that the 'out' field matches
				match data.pick(stk, ctx, opt, "out").await? {
					// You cannot store a range id as the out field on a document
					Value::RecordId(v) if v.key.is_range() => {
						bail!(Error::OutInvalid {
							value: v.to_string(),
						})
					}
					// The out field is a match, so don't error
					Value::RecordId(v) if v.eq(r) => {}
					Value::None => {}
					v => {
						ensure!(
							r.key == v,
							Error::OutMismatch {
								value: v.to_string(),
							}
						)
					}
				}
			}
			// This is a INSERT RELATION statement
			else if let Some(data) = v {
				// Check that the 'id' field matches
				match data.pick(&*ID) {
					// You cannot store a range id as the id field on a document
					Value::RecordId(v) if v.key.is_range() => {
						bail!(Error::IdInvalid {
							value: v.to_string(),
						})
					}
					// The id field is a match, so don't error
					Value::RecordId(v) if v.eq(&rid) => (),
					// The id is a match, so don't error
					v if rid.key == v => (),
					// There was no id field specified
					v if v.is_none() => (),
					// The id field does not match
					v => {
						bail!(Error::IdMismatch {
							value: v.to_string(),
						})
					}
				}
				// Check that the 'in' field matches
				match data.pick(&*IN) {
					// You cannot store a range id as the in field on a document
					Value::RecordId(v) if v.key.is_range() => {
						bail!(Error::InInvalid {
							value: v.to_string(),
						})
					}
					// The in field is a match, so don't error
					Value::RecordId(v) if v.eq(l) => (),
					// The in is a match, so don't error
					v if l.key == v => (),
					// The in field does not match
					v => {
						bail!(Error::InMismatch {
							value: v.to_string(),
						})
					}
				}
				// Check that the 'out' field matches
				match data.pick(&*OUT) {
					// You cannot store a range id as the out field on a document
					Value::RecordId(v) if v.key.is_range() => {
						bail!(Error::OutInvalid {
							value: v.to_string(),
						})
					}
					// The out field is a match, so don't error
					Value::RecordId(v) if v.eq(r) => (),
					// The out is a match, so don't error
					v if r.key == v => (),
					// The out field does not match
					v => {
						bail!(Error::OutMismatch {
							value: v.to_string(),
						})
					}
				}
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
				let table = self.tb(ctx, opt).await?;
				// Get the correct document to check
				let doc = match stm.is_delete() {
					true => &self.initial,
					false => &self.current,
				};
				// Process the table permissions
				match &table.permissions.select {
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
				let table = self.tb(ctx, opt).await?;
				// Get the permissions for this table
				let perms = stm.permissions(&table, self.is_new());
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
				// Get the table
				let table = self.tb(ctx, opt).await?;
				// Get the permission clause
				let perms = stm.permissions(&table, self.is_new());
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
