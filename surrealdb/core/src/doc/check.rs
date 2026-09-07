use std::borrow::Cow;
use std::collections::HashSet;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use super::IgnoreError;
use crate::catalog::{Permission, TableType};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document, Error as DocError, Extras};
use crate::exe::FlowResultExt;
use crate::exec::Error as ExecError;
use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{Cond, Error};
use crate::iam::Action;
use crate::val::{RecordId, Value};

impl Document {
	/// Tenant-boundary gate for the write pipeline (GHSA-2v9j).
	///
	/// The read path enforces the same boundary in
	/// [`crate::exec::permission::validate_record_user_access`], but that gate
	/// is only reachable through the scan operators, so it never runs for
	/// `CREATE` / `UPDATE` / `DELETE`, which perform no scan. Without this
	/// mirror, a principal that has pivoted into a foreign namespace/database
	/// (for example via the RPC `use` method, which sets the context without
	/// rejecting the switch) could blind-write (create, tamper, or delete) any
	/// table there whose write `PERMISSIONS` are satisfiable, even though the
	/// same session's reads are denied.
	///
	/// The authenticated auth level is the source of truth; the session's
	/// *selected* ns/db can be steered by `USE` and must not be trusted. Root
	/// and anonymous principals are unconfined, a namespace principal may write
	/// any database within its namespace, and database and record principals are
	/// pinned to their exact namespace/database.
	#[inline]
	pub(super) fn check_record_user_access(&self, opt: &Options) -> Result<()> {
		// Root and anonymous principals carry no tenant to confine; skip the
		// ns/db lookup so tenant-less contexts are unaffected.
		if opt.auth.is_root() || opt.auth.is_anon() {
			return Ok(());
		}
		let (ns, db) = opt.ns_db()?;
		if opt.auth.can_access_ns_db(ns, db) {
			return Ok(());
		}
		// A namespace match with a database mismatch is a database-scope
		// violation; otherwise the namespace itself is out of reach.
		if opt.auth.level().ns() == Some(ns) {
			bail!(ExecError::DbNotAllowed {
				db: db.to_owned(),
			});
		}
		bail!(ExecError::NsNotAllowed {
			ns: ns.to_owned(),
		});
	}

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
			DocError::TableCheck {
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
			DocError::TableCheck {
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
			DocError::TableCheck {
				record: self.id()?.to_sql(),
				relation: true,
				target_type: tb.table_type.to_sql(),
			}
		);
		// Carry on
		Ok(())
	}

	/// Whether this document's table is a lightweight relation — one whose
	/// edges are vertex-side adjacency keys alone, synthesized on read from
	/// their canonical `[in, out]` ids.
	#[inline]
	pub(super) fn table_is_lightweight(&self) -> Result<bool> {
		let tb = self.doc_ctx.tb()?;
		Ok(matches!(&tb.table_type, TableType::Relation(rel) if rel.lightweight))
	}

	/// Rejects a mutation of a lightweight relation's edges. Their whole
	/// state is the canonical id: there is nothing to set, and readers
	/// synthesize the record, so a stored mutation could never be observed
	/// consistently.
	#[inline]
	pub(super) fn check_table_not_lightweight(&self, action: &str) -> Result<()> {
		ensure!(
			!self.table_is_lightweight()?,
			crate::exec::Error::Thrown(format!(
				"a LIGHTWEIGHT relation's edges cannot be {action}: they carry no data beyond `id`, `in` and `out`"
			))
		);
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
					DocError::TableCheck {
						record: self.id()?.to_sql(),
						relation: true,
						target_type: tb.table_type.to_sql(),
					}
				);
				// INSERT RELATION is the data-bearing edge-write form, and a
				// lightweight edge carries no data; RELATE is the only way in.
				self.check_table_not_lightweight("written with INSERT")?;
			}
			_ => {
				ensure!(
					tb.allows_normal(),
					DocError::TableCheck {
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

	/// Checks that the table for this document is not a view
	/// (`DEFINE TABLE ... AS SELECT`). View tables are read-only:
	/// their records are computed from the source query and are
	/// maintained automatically, so manual CREATE / INSERT / UPSERT /
	/// UPDATE / DELETE / RELATE statements are rejected.
	///
	/// Imports are exempt: a database export emits the stored rows of
	/// a materialised view as `INSERT` statements, so replaying them
	/// must be allowed. This mirrors the `opt.import` guard in
	/// [`Self::process_table_views`], which likewise skips view
	/// maintenance while importing.
	///
	/// Call this *after* the table-level permission check in each write
	/// path (as the sibling [`Self::check_table_type_create`] is), so an
	/// actor who lacks permission still gets the normal permission
	/// outcome (e.g. a silent skip) rather than an error that would
	/// disclose the table is a view.
	#[inline]
	pub(super) fn check_table_not_view(&self, opt: &Options) -> Result<()> {
		// Allow writes to view tables while replaying an export
		if opt.import {
			return Ok(());
		}
		// Get the table for this document
		let tb = self.doc_ctx.tb()?;
		// Ensure the table is not a computed view
		ensure!(
			tb.view.is_none(),
			DocError::TableIsView {
				table: tb.name.to_string(),
			}
		);
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
					bail!(DocError::IdMismatch {
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
		// Materialise the pre-mutation snapshot. On a mutating statement the
		// data clause reads this same image, so the predicate and the data
		// clause cannot disagree about a computed field's value.
		let doc: &CursorDoc = self.materialise_current_snapshot(stk, ctx, opt).await?;
		// Check the WHERE clause against the snapshot
		if !stk
			.run(|stk| crate::legacy::expr_compute(&cond.0, stk, ctx, opt, Some(doc)))
			.await
			.catch_return()?
			.is_truthy()
		{
			return Err(IgnoreError::Ignore);
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
			self.process_permissions(
				stk,
				ctx,
				opt,
				doc,
				&self.doc_ctx.tb()?.permissions.select,
				PermissionClauseKind::Read,
			)
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
			let perm = &self.doc_ctx.tb()?.permissions.create;
			let doc = self.gate_doc(stk, ctx, opt, perm, doc).await?;
			self.process_permissions(stk, ctx, opt, &doc, perm, PermissionClauseKind::Write)
				.await?;
		}
		Ok(())
	}

	/// Build the document a write `PERMISSIONS` predicate is evaluated against:
	/// `doc` with the computed fields that predicate reads materialised onto a
	/// scratch copy.
	///
	/// Computed fields are stripped before storage, so the record about to be
	/// written (`self.current`, passed as `doc`) carries none of them. A
	/// `Permission::Specific` predicate naming a computed field would resolve it
	/// to `NONE`, so a clause written to *deny* (`WHERE blocked != true`,
	/// `WHERE status != 'final'`) is satisfied by `NONE != true` and lets every
	/// caller through.
	///
	/// The values are materialised on a copy, never on `doc` itself, because
	/// `store_record_data` persists `self.current` verbatim: computing them in
	/// place would write the derived values into storage. `Cow::Borrowed(doc)`
	/// is returned whenever nothing needs materialising, so a table with no
	/// computed fields — or a predicate that reads none — pays only a dependency
	/// scan and no clone.
	///
	/// Only the fields the predicate reads are evaluated, resolved the same way
	/// [`Document::compute_fields`] resolves a projection's roots; an
	/// unresolvable dependency set falls back to evaluating every computed
	/// field. The bodies run under the definer's auth in a write frame, exactly
	/// as on the read path, so evaluating them ahead of the gate grants the
	/// caller nothing — though a body that errors surfaces its error rather than
	/// the permission denial, as the `SELECT` gate already does.
	async fn gate_doc<'a>(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		perm: &Permission,
		doc: &'a CursorDoc,
	) -> Result<Cow<'a, CursorDoc>> {
		// Only a `Specific` predicate reads the document; `Full` / `None` decide
		// without it.
		let Permission::Specific(e) = perm else {
			return Ok(Cow::Borrowed(doc));
		};
		// A temporary document with no id, or a table with no computed fields,
		// has nothing to prepare.
		if self.id.is_none() || !self.has_computed_fields() {
			return Ok(Cow::Borrowed(doc));
		}
		let deps = crate::expr::computed_deps::extract_computed_deps(e);
		// A fully-resolved predicate that names no computed field needs no copy.
		if deps.is_complete && deps.fields.is_empty() {
			return Ok(Cow::Borrowed(doc));
		}
		// `None` roots means "evaluate every computed field", which is how
		// `compute_fields` reads an unresolvable dependency set.
		let roots = deps.is_complete.then(|| deps.fields.into_iter().collect::<HashSet<String>>());
		let mut scratch = doc.clone();
		self.compute_fields_into(stk, ctx, opt, &mut scratch, roots.as_ref()).await?;
		Ok(Cow::Owned(scratch))
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
			let perm = &self.doc_ctx.tb()?.permissions.update;
			let doc = self.gate_doc(stk, ctx, opt, perm, doc).await?;
			self.process_permissions(stk, ctx, opt, &doc, perm, PermissionClauseKind::Write)
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
			let perm = &self.doc_ctx.tb()?.permissions.delete;
			let doc = self.gate_doc(stk, ctx, opt, perm, doc).await?;
			self.process_permissions(stk, ctx, opt, &doc, perm, PermissionClauseKind::Write)
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
	///
	/// A `Read` clause (`SELECT`) is evaluated write-blocked; a `Write` clause
	/// (create/update/delete) is not (see [`permission_predicate_frame`]).
	async fn process_permissions(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: &CursorDoc,
		perms: &Permission,
		clause: PermissionClauseKind,
	) -> Result<(), IgnoreError> {
		// Check that record authentication matches the execution scope,
		// mirroring validate_record_user_access on the streaming path
		if opt.auth.is_record() {
			let ns: &str = &self.doc_ctx.ns().name;
			if opt.auth.level().ns() != Some(ns) {
				return Err(IgnoreError::from(anyhow::Error::new(ExecError::NsNotAllowed {
					ns: ns.into(),
				})));
			}
			let db: &str = &self.doc_ctx.db().name;
			if opt.auth.level().db() != Some(db) {
				return Err(IgnoreError::from(anyhow::Error::new(ExecError::DbNotAllowed {
					db: db.into(),
				})));
			}
		}
		match perms {
			Permission::None => Err(IgnoreError::Ignore),
			Permission::Full => Ok(()),
			Permission::Specific(e) => {
				// Disable permission recursion, and block side effects on the
				// clauses a read evaluates.
				let opt = &permission_predicate_frame(opt, clause);
				// Process the PERMISSION clause
				if !stk
					.run(|stk| crate::legacy::expr_compute(e, stk, ctx, opt, Some(doc)))
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

/// Which kind of permission clause is being evaluated, for the runtime
/// write-frame decision.
#[derive(Clone, Copy)]
pub(crate) enum PermissionClauseKind {
	/// `PERMISSIONS FOR select` — evaluated on reads; always write-blocked.
	Read,
	/// `PERMISSIONS FOR create/update/delete` — evaluated during a write, and
	/// free to carry side effects of its own.
	Write,
}

/// Derive the `Options` frame for evaluating a permission predicate.
///
/// Always disables permission recursion. A `Read` clause is evaluated under
/// the frame that rejects data-modifying statements (GHSA-66r2-5gwj-gxm2): it
/// runs on behalf of a reader who did not write it, with permission
/// enforcement already disabled. A `Write` clause is reached only from a
/// statement that is writing anyway, so its predicate may write too.
pub(crate) fn permission_predicate_frame(opt: &Options, clause: PermissionClauseKind) -> Options {
	match clause {
		PermissionClauseKind::Read => opt.new_for_permission_predicate(),
		PermissionClauseKind::Write => opt.new_for_mutable_permission_predicate(),
	}
}
