use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use super::IgnoreError;
use crate::catalog::Permission;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::compute::DocKind;
use crate::doc::{CursorDoc, Document, Extras};
use crate::err::Error;
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{Cond, FlowResultExt};
use crate::iam::Action;
use crate::val::{RecordId, Value};

impl Document {
	/// Checks that a specifically selected record
	/// actually exists in the underlying datastore.
	/// If the user specifies a record directly
	/// using a Record ID, and that record does not
	/// exist, then this function will exit early.
	#[inline]
	pub(super) fn check_record_exists(&self) -> Result<(), IgnoreError> {
		// Check if this record exists
		if self.id.is_some() && self.current.doc.as_ref().is_none() {
			return Err(IgnoreError::Ignore);
		}
		// Carry on
		Ok(())
	}

	/// Checks whether a CREATE statement is allowed on
	/// the table for this document. When creating a
	/// normal record, we check that the table type
	/// is `ANY` or `NORMAL`.
	#[inline]
	pub(super) fn check_table_type_create(&self) -> Result<()> {
		// Get the table for this document
		let tb = self.doc_ctx.tb()?;
		// Ensure the table allows normal records
		ensure!(
			tb.allows_normal(),
			Error::TableCheck {
				record: self.id()?.to_sql(),
				relation: false,
				target_type: tb.table_type.to_sql(),
			}
		);
		// Carry on
		Ok(())
	}

	/// Checks whether a UPSERT statement is allowed on
	/// the table for this document. When creating a
	/// normal record, we check that the table type
	/// is `ANY` or `NORMAL`.
	#[inline]
	pub(super) fn check_table_type_upsert(&self) -> Result<()> {
		// Get the table for this document
		let tb = self.doc_ctx.tb()?;
		// Ensure the table allows normal records
		ensure!(
			tb.allows_normal(),
			Error::TableCheck {
				record: self.id()?.to_sql(),
				relation: false,
				target_type: tb.table_type.to_sql(),
			}
		);
		// Carry on
		Ok(())
	}

	/// Checks whether a RELATE statement is allowed on
	/// the table for this document. When creating a
	/// normal record, we check that the table type
	/// is `ANY` or `NORMAL`.
	#[inline]
	pub(super) fn check_table_type_relate(&self) -> Result<()> {
		// Get the table for this document
		let tb = self.doc_ctx.tb()?;
		// Ensure the table allows normal records
		ensure!(
			tb.allows_relation(),
			Error::TableCheck {
				record: self.id()?.to_sql(),
				relation: true,
				target_type: tb.table_type.to_sql(),
			}
		);
		// Carry on
		Ok(())
	}

	/// Checks whether an INSERT statement is allowed on
	/// the table for this document. When inserting a
	/// normal record, we check that the table type
	/// is `ANY` or `NORMAL`.
	#[inline]
	pub(super) fn check_table_type_insert(&self) -> Result<()> {
		// Get the table for this document
		let tb = self.doc_ctx.tb()?;
		// Ensure the table allows normal records
		match self.extras {
			Extras::Relate(_, _, _) => {
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
		};
		// Carry on
		Ok(())
	}

	/// Quick `PERMISSIONS FOR create` preflight that only short-circuits
	/// `Permission::None`. Used by the create-side of CREATE / UPSERT /
	/// INSERT / RELATE to bail before computing the data clause when
	/// the table forbids creates outright.
	///
	/// `Permission::Specific(predicate)` is **not** evaluated here:
	/// CREATE-side predicates typically reference the new record's
	/// fields (e.g. `PERMISSIONS FOR create WHERE published = false`)
	/// and the data clause has not yet been applied to `self.current`,
	/// so an early predicate evaluation would see an empty document
	/// and reject valid creates. The full
	/// [`Self::check_create_table_permission`] runs later in the
	/// pipeline against the populated record.
	#[inline]
	pub(super) fn check_permissions_quick_create(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<(), IgnoreError> {
		// Ensure this is not a temporary document
		if self.id.is_some() {
			// Should we run permissions checks?
			if ctx.check_perms(opt, Action::Edit)? {
				// Get the table for this document
				let table = self.doc_ctx.tb()?;
				// Exit early if table CREATE permissions are NONE
				if table.permissions.create.is_none() {
					return Err(IgnoreError::Ignore);
				}
			}
		}
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
	pub(super) fn check_data_fields(&self) -> Result<()> {
		// An inline helper function to check the value at the given path
		fn check(found: Value, expected: &RecordId) -> Result<()> {
			match found {
				// We found a record id which is a range
				Value::RecordId(v) if v.key.is_range() => {
					bail!(Error::IdInvalid {
						value: v.to_sql(),
					})
				}
				// We found a record id which matches
				Value::RecordId(v) if v.eq(expected) => Ok(()),
				// We didn't find any value at the given path, which is allowed.
				// This occurs when a specific record ID is already determined from the statement
				// itself. Examples:
				//   CREATE person:tobie SET name = 'Tobie';
				//   CREATE person:jaime CONTENT { name: 'Jaime' };
				//   RELATE user:tobie->likes->product:laptop SET when = time::now();
				Value::None => Ok(()),
				// We found a non RecordId value (e.g., string, number, array, object, uuid)
				// which is the shorthand notation where users can specify just the key portion.
				// We validate that the provided key matches the expected key from the statement.
				// This can occur in CREATE, UPSERT, UPDATE, INSERT, and RELATE statements when:
				// - A specific record ID is already determined (e.g., CREATE person:other or
				//   RELATE's in/out)
				// - That field uses shorthand notation instead of a full Record ID
				// Examples:
				//   CREATE user CONTENT { id: 123 };
				//   CREATE city CONTENT { id: 'london' };
				v if expected.key == v => Ok(()),
				// Anything else is an error
				v => {
					bail!(Error::IdMismatch {
						value: v.to_sql()
					})
				}
			}
		}
		// Skip the check when the document id was generated from the
		// statement's table rather than being explicitly specified
		// (e.g. `CREATE foo CONTENT { id: bar:123 }` extracts the key
		// from the content and reuses the statement's table).
		if self.r#gen.is_some() {
			return Ok(());
		}
		// Get the specified record id
		let rid = self.id()?;
		// Prevent ranges as record ids
		ensure!(
			!rid.key.is_range(),
			Error::IdInvalid {
				value: rid.to_sql(),
			}
		);
		// Get the computed input data
		let data = self.input_data.as_ref();
		// PATCH clauses cannot be statically checked
		if data.is_some_and(|x| x.is_patch()) {
			return Ok(());
		}
		// This is a CREATE, UPSERT, UPDATE statement
		if let Extras::Normal = &self.extras {
			if let Some(data) = data {
				check(data.pick(ID.as_ref()), rid.as_ref())?;
			}
		}
		// This is a RELATE / INSERT RELATION statement
		else if let Extras::Relate(l, r, v) = &self.extras {
			if let Some(data) = data {
				check(data.pick(ID.as_ref()), rid.as_ref())?;
				check(data.pick(IN.as_ref()), l)?;
				check(data.pick(OUT.as_ref()), r)?;
			} else if let Some(value) = v {
				check(value.pick(ID.as_ref()), rid.as_ref())?;
				check(value.pick(IN.as_ref()), l)?;
				check(value.pick(OUT.as_ref()), r)?;
			}
		}
		// Carry on
		Ok(())
	}

	/// Evaluates a `WHERE` predicate against the row about to be
	/// projected and signals `IgnoreError::Ignore` when the row does
	/// not match. Short-circuits for key-only iteration and for a
	/// missing `WHERE` clause. Computed fields are populated on the
	/// relevant view first so predicates like `WHERE flag` see the
	/// materialised value.
	pub(super) async fn check_where_condition(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		cond: Option<&Cond>,
	) -> Result<(), IgnoreError> {
		// Exit early for key-only iteration
		if self.is_key_only_iteration() {
			return Ok(());
		}
		// Get the WHERE clause from the statement
		let Some(cond) = cond else {
			return Ok(());
		};
		// Check if we need to reduce the document
		if self.reduction_required(ctx, opt)? {
			// Materialise the reduced view via the cached helper. On
			// UPDATE / UPSERT this is a cache hit because
			// `compute_input_data` has already built it against the same
			// pre-mutation `self.current` — saves one `reduce_document`
			// call per row. On DELETE / SELECT this is the first reduce.
			let _ = self.reduce_current(stk, ctx, opt).await?;
			// Populate computed fields on the reduced view so predicates
			// like `WHERE flag` see the materialised value.
			self.compute_fields(stk, ctx, opt, DocKind::CurrentReduced, None).await?;
			// Re-borrow the reduced view we just materialised
			let doc: &CursorDoc = self.current_reduced.as_ref().unwrap_or(&self.current);
			// Check the WHERE clause against the reduced view
			if !stk
				.run(|stk| cond.0.compute(stk, ctx, opt, Some(doc)))
				.await
				.catch_return()?
				.is_truthy()
			{
				return Err(IgnoreError::Ignore);
			}
		} else {
			// Compute the fields on the current document
			self.compute_fields(stk, ctx, opt, DocKind::Current, None).await?;
			// Check the WHERE clause against the computed document
			if !stk
				.run(|stk| cond.0.compute(stk, ctx, opt, Some(&self.current)))
				.await
				.catch_return()?
				.is_truthy()
			{
				return Err(IgnoreError::Ignore);
			}
		}
		// Carry on
		Ok(())
	}

	/// Check the `PERMISSIONS FOR select` clause on this table. Short-
	/// circuits if the record being processed does not have an id,
	/// so temporary documents never trip the permissions lookup.
	pub(super) async fn check_select_permissions(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: &CursorDoc,
	) -> Result<(), IgnoreError> {
		if self.id.is_some() && ctx.check_perms(opt, Action::View)? {
			self.process_permissions(stk, ctx, opt, doc, &self.doc_ctx.tb()?.permissions.select)
				.await?;
		}
		Ok(())
	}

	/// Check the `PERMISSIONS FOR create` clause on this table. Short-
	/// circuits if the record being processed does not have an id,
	/// so temporary documents never trip the permissions lookup.
	pub(super) async fn check_create_permissions(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: &CursorDoc,
	) -> Result<(), IgnoreError> {
		if self.id.is_some() && ctx.check_perms(opt, Action::Edit)? {
			self.process_permissions(stk, ctx, opt, doc, &self.doc_ctx.tb()?.permissions.create)
				.await?;
		}
		Ok(())
	}

	/// Check the `PERMISSIONS FOR update` clause on this table. Short-
	/// circuits if the record being processed does not have an id,
	/// so temporary documents never trip the permissions lookup.
	pub(super) async fn check_update_permissions(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: &CursorDoc,
	) -> Result<(), IgnoreError> {
		if self.id.is_some() && ctx.check_perms(opt, Action::Edit)? {
			self.process_permissions(stk, ctx, opt, doc, &self.doc_ctx.tb()?.permissions.update)
				.await?;
		}
		Ok(())
	}

	/// Check the `PERMISSIONS FOR delete` clause on this table. Short-
	/// circuits if the record being processed does not have an id,
	/// so temporary documents never trip the permissions lookup.
	pub(super) async fn check_delete_permissions(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: &CursorDoc,
	) -> Result<(), IgnoreError> {
		if self.id.is_some() && ctx.check_perms(opt, Action::Edit)? {
			self.process_permissions(stk, ctx, opt, doc, &self.doc_ctx.tb()?.permissions.delete)
				.await?;
		}
		Ok(())
	}

	/// Recheck the `PERMISSIONS FOR update` clause on this table.
	/// Short-circuits if the record being processed does not have
	/// an id, so temporary documents never trip the permissions
	/// lookup. This is used after editing a record, to check that
	/// it still conforms to the table permissions requirements.
	pub(super) async fn recheck_update_permissions(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: &CursorDoc,
	) -> Result<(), IgnoreError> {
		if matches!(&self.doc_ctx.tb()?.permissions.update, Permission::Specific(_)) {
			self.check_update_permissions(stk, ctx, opt, doc).await?;
		}
		Ok(())
	}

	/// Evaluate a `Permission` clause against the given document and
	/// signal `IgnoreError::Ignore` when access is denied.
	///
	/// Shared by `check_select_permissions` / `check_create_permissions`
	/// / `check_update_permissions` / `check_delete_permissions` so the
	/// `Permission::None` / `Permission::Full` / `Permission::Specific`
	/// dispatch lives in one place. For `Specific(expr)` the predicate
	/// is computed against `doc` with permission checks disabled on the
	/// nested `Options`, so the predicate itself cannot recursively trip
	/// table-level permission gates.
	async fn process_permissions(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: &CursorDoc,
		perms: &Permission,
	) -> Result<(), IgnoreError> {
		match perms {
			Permission::None => Err(IgnoreError::Ignore),
			Permission::Full => Ok(()),
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
				Ok(())
			}
		}
	}
}
