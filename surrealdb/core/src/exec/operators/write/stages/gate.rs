//! The permission gates.
//!
//! Every stage that asks whether the caller may perform this write, or may be
//! shown its result.
//!
//! A stored clause is compiled through
//! [`permission_to_physical`](crate::exec::permission::permission_to_physical)
//! rather than evaluated as an AST, which is what keeps these gates on the
//! streaming evaluator.
//!
//! # What each gate answers, and when
//!
//! [`CheckRecordUserAccess`] is tenant confinement and heads all nine orders,
//! ahead of every permission gate and every create/update selection. A record
//! user writing outside its own tenant is stopped there and nowhere else: the
//! read path enforces the same boundary at the scan operators, and a write
//! performs no scan. It fails the statement; every other gate here skips the
//! record.
//!
//! The remaining gates split into three:
//!
//! - the **pre-write** gate of a creating order ([`CheckPermissionsQuickCreate`]), which
//!   short-circuits only on a `NONE` create permission. A conditional one is deliberately left to
//!   the post-write gate, whose subject is the record the data clause has not yet built;
//! - the **pre-write** gate of an updating order and of `DELETE`, evaluated in full before any user
//!   expression in the condition or the data clause, so a `WHERE THROW …` cannot exfiltrate field
//!   values ahead of the refusal;
//! - the **post-write** gates, judged against the record the statement built, and the output gate
//!   judged against the image the statement is about to project.
//!
//! A write predicate is judged against a *scratch* copy carrying the record's
//! `COMPUTED` values — see [`gate_subject`]. The post-image itself is what the
//! store writes verbatim, so materialising in place would persist derived
//! values.

use std::borrow::Cow;
use std::sync::Arc;

use crate::catalog::{Permission, PermissionKind};
use crate::exec::BoxFut;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::operators::write::stage::{RecordImage, StageId, StageOutcome, WriteStage};
use crate::exec::permission::{
	GuardKind, PhysicalPermission, check_permission_for_value, check_write_permission_for_value,
	permission_to_physical, should_check_perms, validate_record_user_access,
};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::val::Value;

/// This module's stages, by id.
///
/// `None` is a stage whose body has not landed:
/// [`WritePlan::resolve`](crate::exec::operators::write::plan::WritePlan)
/// refuses to bind a plan that names one, rather than running a partial order.
#[allow(
	clippy::wildcard_enum_match_arm,
	reason = "a stage this module does not own is not this module's to name, and `super::lookup` \
	          routes none of them here"
)]
pub(crate) fn stage(id: StageId) -> Option<Arc<dyn WriteStage>> {
	match id {
		StageId::CheckRecordUserAccess => Some(Arc::new(CheckRecordUserAccess)),
		StageId::CheckPermissionsQuickCreate => Some(Arc::new(CheckPermissionsQuickCreate)),
		StageId::CheckCreatePermissions => Some(Arc::new(TableWriteGate {
			id,
			clause: PermissionKind::Create,
			only_if_predicate: false,
		})),
		StageId::CheckUpdatePermissions => Some(Arc::new(TableWriteGate {
			id,
			clause: PermissionKind::Update,
			only_if_predicate: false,
		})),
		StageId::CheckDeletePermissions => Some(Arc::new(TableWriteGate {
			id,
			clause: PermissionKind::Delete,
			only_if_predicate: false,
		})),
		StageId::RecheckUpdatePermissions => Some(Arc::new(TableWriteGate {
			id,
			clause: PermissionKind::Update,
			only_if_predicate: true,
		})),
		StageId::CheckSelectPermissions(image) => Some(Arc::new(CheckSelectPermissions(image))),
		// `super::lookup` routes only this module's stages here, so anything
		// else is a routing mistake rather than a missing body. Answering
		// `None` leaves it to `resolve` to name, as it names any unbound stage.
		_ => None,
	}
}

/// Tenant confinement, the head of all nine orders.
///
/// The authenticated auth level is the source of truth; the session's
/// *selected* namespace and database can be steered by `USE` — including the
/// RPC `use` method, which sets the context without rejecting a cross-tenant
/// switch — and must not be trusted. Root and anonymous principals carry no
/// tenant to confine, a namespace principal may write any database within its
/// namespace, and database and record principals are pinned to their exact
/// namespace and database.
///
/// This fails the statement rather than skipping the record: a principal
/// writing outside its tenant is told so, exactly as a read of the same table
/// would be.
///
/// It reads only the caller's identity and the record's table, so it is a pure
/// function of the statement and the principal — which is why a rolled-back
/// creating attempt may re-run it on the updating order at no cost, and why the
/// order selection may precede it.
#[derive(Debug)]
struct CheckRecordUserAccess;

impl WriteStage for CheckRecordUserAccess {
	fn name(&self) -> &'static str {
		StageId::CheckRecordUserAccess.name()
	}

	fn apply<'a>(
		&'a self,
		_row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			let db_ctx =
				ctx.exec().database().context("the tenant guard requires a database context")?;
			validate_record_user_access(db_ctx).map_err(|e| ControlFlow::Err(e.into()))?;
			Ok(StageOutcome::Continue)
		})
	}
}

/// The pre-write table permission of a creating order.
///
/// It short-circuits on a `NONE` create permission alone. A
/// `Permission::Specific` predicate is **not** evaluated here: a create-side
/// predicate typically names the new record's fields, and the data clause has
/// not yet been applied, so an early evaluation would judge an empty record and
/// refuse valid creates. [`TableWriteGate`] for
/// [`StageId::CheckCreatePermissions`] runs later, against the populated
/// record.
///
/// That timing distinction is observable: a data clause with side effects does
/// not run under a `NONE` create permission and does run under a conditional
/// one that then refuses the write.
///
/// A subject with no id is a temporary record and never trips the lookup.
#[derive(Debug)]
struct CheckPermissionsQuickCreate;

impl WriteStage for CheckPermissionsQuickCreate {
	fn name(&self) -> &'static str {
		StageId::CheckPermissionsQuickCreate.name()
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			if row.id.is_none() || !checks_apply(ctx, Action::Edit)? {
				return Ok(StageOutcome::Continue);
			}
			if ctx.table.permissions.create.is_none() {
				return Ok(StageOutcome::SkipRow);
			}
			Ok(StageOutcome::Continue)
		})
	}
}

/// A table `PERMISSIONS FOR create` / `update` / `delete` clause, judged
/// against the record the order has built so far.
///
/// One body serves four slots, because the four differ only in which clause
/// they read and whether they stand down when it is not a predicate:
///
/// - `check_create_permissions` — the post-write gate of a creating order, judged against the
///   record the data clause built;
/// - `check_update_permissions` and `check_delete_permissions` — the pre-write gates of an updating
///   order and of `DELETE`, judged against the record as read and evaluated in full *before* the
///   statement's condition or data clause;
/// - `recheck_update_permissions` — the post-write gate of an updating order. It is not a second
///   evaluation: it returns immediately unless the table's update permission is a predicate, so a
///   `FULL` or `NONE` update permission is judged once.
///
/// A refusal skips the record; the statement succeeds having written nothing
/// for it.
#[derive(Debug)]
struct TableWriteGate {
	/// The stage this is, which supplies the name an order knows it by.
	id: StageId,
	/// The table clause it reads.
	clause: PermissionKind,
	/// Whether it stands down unless that clause is a predicate.
	only_if_predicate: bool,
}

impl WriteStage for TableWriteGate {
	fn name(&self) -> &'static str {
		self.id.name()
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("clause".to_string(), self.clause.to_string())]
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			let permission = table_permission(ctx, self.clause);
			// A recheck answers only for a predicate, whose truth can have
			// changed with the record. `FULL` and `NONE` were settled by the
			// pre-write gate and are not re-asked.
			if self.only_if_predicate && !permission.is_specific() {
				return Ok(StageOutcome::Continue);
			}
			if row.id.is_none() || !checks_apply(ctx, Action::Edit)? {
				return Ok(StageOutcome::Continue);
			}
			let subject = gate_subject(row, ctx, permission).await?;
			let compiled = compile(ctx, self.clause, permission).await?;
			if admits(&compiled, subject.as_ref(), ctx, guard_kind(self.clause)).await? {
				Ok(StageOutcome::Continue)
			} else {
				Ok(StageOutcome::SkipRow)
			}
		})
	}
}

/// The table's `PERMISSIONS FOR select` clause, gating the image the statement
/// is about to project.
///
/// The payload names that image: the post-image on eight of the nine orders,
/// and the pre-image on `DELETE`, which has no post-image to gate. A refusal
/// skips the record — the write already happened and still counts as affected;
/// what is withheld is the row the statement would have returned.
///
/// The compiled clause comes from the same resolver the output projection uses
/// ([`WriteCtx::view_checked`]), so the gate and the projection cannot disagree
/// about which permission governs the table, and the clause is compiled once
/// per statement per table rather than once per record.
#[derive(Debug)]
struct CheckSelectPermissions(RecordImage);

impl WriteStage for CheckSelectPermissions {
	fn name(&self) -> &'static str {
		StageId::CheckSelectPermissions(self.0).name()
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let image = match self.0 {
			RecordImage::Initial => "initial",
			RecordImage::Current => "current",
		};
		vec![("image".to_string(), image.to_string())]
	}

	fn apply<'a>(
		&'a self,
		row: &'a mut WriteRow,
		ctx: &'a WriteCtx,
	) -> BoxFut<'a, Result<StageOutcome, ControlFlow>> {
		Box::pin(async move {
			// A temporary record has no identity to gate.
			if row.id.is_none() {
				return Ok(StageOutcome::Continue);
			}
			// Cloned out of the resolver so the lock is not held across the
			// predicate, which may itself read records.
			let permission = {
				let mut views = ctx.view_checked.lock().await;
				let view = views.view_for(ctx.exec(), &ctx.table.name).await?;
				if !view.checked() {
					return Ok(StageOutcome::Continue);
				}
				view.permission().clone()
			};
			let image = match self.0 {
				RecordImage::Initial => &row.initial,
				RecordImage::Current => &row.current,
			};
			if admits(&permission, image, ctx, GuardKind::Read).await? {
				Ok(StageOutcome::Continue)
			} else {
				Ok(StageOutcome::SkipRow)
			}
		})
	}
}

/// Whether this caller's writes are permission-checked at all.
///
/// False for a principal whose role and actor level exempt it, for an anonymous
/// session on a datastore with auth disabled, and inside a permission
/// predicate — where re-entering would recurse without end on a cyclic link.
fn checks_apply(ctx: &WriteCtx, action: Action) -> Result<bool, ControlFlow> {
	let db_ctx = ctx.exec().database().context("a permission gate requires a database context")?;
	should_check_perms(db_ctx, action).map_err(|e| ControlFlow::Err(e.into()))
}

/// The table clause a gate reads.
fn table_permission(ctx: &WriteCtx, clause: PermissionKind) -> &Permission {
	let permissions = &ctx.table.permissions;
	match clause {
		PermissionKind::Select => &permissions.select,
		PermissionKind::Create => &permissions.create,
		PermissionKind::Update => &permissions.update,
		PermissionKind::Delete => &permissions.delete,
	}
}

/// Compile a stored table clause into the engine's scalar, on first
/// use and once for the statement.
///
/// A clause is a function of the table definition and not of the record, so it
/// is memoised on the context every record shares rather than planned again
/// for each of them.
///
/// The planner is built without a transaction, as the read path's runtime
/// resolution is: a planner that resolves no table context cannot recurse into
/// a self-referential guard (`WHERE (SELECT FROM same_table) != NONE`), and a
/// gate running per record shares no parent cycle guard that would otherwise
/// break the recursion.
///
/// A clause the lowering has no form for — or one holding a write no operator
/// compiles — comes back carried as an uncompilable clause, which fails where
/// it is evaluated and so fails closed rather than letting the write through.
async fn compile(
	ctx: &WriteCtx,
	clause: PermissionKind,
	permission: &Permission,
) -> Result<Arc<PhysicalPermission>, ControlFlow> {
	{
		let cache = ctx.permissions_compiled.lock().await;
		if let Some((_, compiled)) = cache.iter().find(|(kind, _)| *kind == clause) {
			return Ok(Arc::clone(compiled));
		}
	}
	let exec = ctx.exec();
	let planner =
		crate::exec::vm::descriptor::write_unit_planner(exec.ctx(), exec.function_registry());
	let compiled = Arc::new(
		permission_to_physical(
			permission,
			ctx.table.permission_logical(clause),
			guard_kind(clause),
			&planner,
		)
		.await
		.map_err(|e| ControlFlow::Err(anyhow::Error::from(e)))?,
	);
	let mut cache = ctx.permissions_compiled.lock().await;
	if let Some((_, published)) = cache.iter().find(|(kind, _)| *kind == clause) {
		return Ok(Arc::clone(published));
	}
	cache.push((clause, Arc::clone(&compiled)));
	Ok(compiled)
}

/// Which guard a `PERMISSIONS` clause is, which decides the frame its
/// predicate is compiled and evaluated in.
///
/// A guard on what the caller may *write* is evaluated in a frame that allows
/// writes, where a `select` guard is evaluated write-refusing.
fn guard_kind(clause: PermissionKind) -> GuardKind {
	match clause {
		PermissionKind::Select => GuardKind::Read,
		PermissionKind::Create | PermissionKind::Update | PermissionKind::Delete => {
			GuardKind::Write
		}
	}
}

/// Judge a compiled clause against one record, and answer whether it admits the
/// caller.
///
/// The record is bound as both the evaluated value and the document root, which
/// is what lets `$parent` inside an idiom-level filter resolve to the record
/// under test.
async fn admits(
	permission: &PhysicalPermission,
	value: &Value,
	ctx: &WriteCtx,
	guard: GuardKind,
) -> Result<bool, ControlFlow> {
	match guard {
		GuardKind::Read => check_permission_for_value(permission, value, None, ctx.exec()).await,
		GuardKind::Write => {
			check_write_permission_for_value(permission, value, None, ctx.exec()).await
		}
	}
	.map_err(ControlFlow::Err)
}

/// The value a write `PERMISSIONS` predicate is judged against: the post-image
/// with the `COMPUTED` fields that predicate could read materialised onto a
/// scratch copy.
///
/// `COMPUTED` values are stripped before storage, so the record about to be
/// written carries none of them. A predicate naming a computed field would
/// resolve it to `NONE`, so a clause written to *deny* (`WHERE blocked != true`)
/// would be satisfied by `NONE != true` and let every caller through.
///
/// The values are materialised on a copy, never on the post-image itself,
/// because the store writes that image verbatim: computing in place would
/// persist the derived values.
///
/// `Cow::Borrowed` is returned whenever nothing needs materialising — the
/// clause is not a predicate, the record is temporary, the table declares no
/// computed field, the predicate reads none, or the post-image already carries
/// its computed values — so the common case pays only a dependency scan.
///
/// The bodies run through the write's *unchecked* resolver
/// ([`WriteCtx::view_unchecked`]), which is the resolver for an image a stage
/// derives rather than shows: it enforces neither the table guard nor the field
/// guards, so the predicate is judged against the record whole. The bodies
/// themselves still run under the definer's auth, exactly as on the read path,
/// so evaluating them ahead of the gate grants the caller nothing. A body that
/// errors surfaces its error rather than the permission denial, as the `SELECT`
/// gate already does.
async fn gate_subject<'a>(
	// Taken by exclusive reference rather than shared: a `&WriteRow` held
	// across an await would require `WriteRow: Sync`, which its memoised
	// comparison cell deliberately is not. Nothing here mutates the row.
	row: &'a mut WriteRow,
	ctx: &WriteCtx,
	permission: &Permission,
) -> Result<Cow<'a, Value>, ControlFlow> {
	// Only a predicate reads the record; `FULL` and `NONE` decide without it.
	let Permission::Specific(guard) = permission else {
		return Ok(Cow::Borrowed(&row.current));
	};
	// A temporary record has no schema to project against, and a table with no
	// computed fields has nothing to materialise.
	if row.id.is_none() || !ctx.fields.iter().any(|fd| fd.computed.is_some()) {
		return Ok(Cow::Borrowed(&row.current));
	}
	// A fully-resolved predicate that names no computed field needs no copy. An
	// unresolvable dependency set does, because it names nothing reliably.
	let deps = crate::expr::computed_deps::extract_computed_deps(guard);
	if deps.is_complete && deps.fields.is_empty() {
		return Ok(Cow::Borrowed(&row.current));
	}
	// The post-image already carries every computed value: the copy would
	// inherit them and derive nothing.
	if row.fields_computed {
		return Ok(Cow::Borrowed(&row.current));
	}
	let mut scratch = row.current.clone();
	let mut views = ctx.view_unchecked.lock().await;
	let view = views.view_for(ctx.exec(), &ctx.table.name).await?;
	// An unchecked view enforces nothing: applying it derives the `COMPUTED`
	// bodies and cuts no field, and it reports every record visible, so the
	// verdict it returns says nothing and is not the gate's answer.
	view.apply(ctx.exec(), &mut scratch).await?;
	Ok(Cow::Owned(scratch))
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::test_util::{TestDb, val};
	use crate::exec::operators::write::clauses::WriteClauses;
	use crate::exec::operators::write::ctx::TableExistence;
	use crate::iam::{Auth, Role};
	use crate::kvs::TransactionType;
	use crate::val::TableName;

	/// A write context over `table` for `session`.
	async fn write_ctx(db: &TestDb, session: &Session, table: &str) -> WriteCtx {
		let exec = db.exec_ctx_as(session, TransactionType::Write).await;
		WriteCtx::resolve(
			&exec,
			&TableName::from(table),
			TableExistence::Required,
			None,
			Arc::new(WriteClauses::default()),
		)
		.await
		.expect("the table resolves")
	}

	/// A row for a record that already exists, as a reading source yields one.
	async fn row_for(src: &str) -> WriteRow {
		WriteRow::for_record(val(src).await).expect("a record value")
	}

	/// An anonymous session on `test`/`test`, permission-checked under a
	/// datastore built with server auth enabled.
	fn anonymous() -> Session {
		Session::default().with_ns("test").with_db("test")
	}

	/// A record user of `test`/`test` whose *authenticated* level names
	/// `ns`/`db`, which is the state an RPC `use` pivot leaves behind when the
	/// two disagree.
	fn record_user_authenticated_at(ns: &str, db: &str) -> Session {
		let rid = crate::types::PublicValue::String("user:tobie".to_owned());
		Session {
			au: Arc::new(Auth::for_db(Role::Editor, ns, db)),
			..Session::for_record("test", "test", "user", rid)
		}
	}

	/// Whether an object row carries `name` at all.
	fn carries(row: &Value, name: &str) -> bool {
		matches!(row, Value::Object(obj) if obj.get(name).is_some())
	}

	/// A principal whose authenticated level names a foreign database is
	/// stopped by the tenant guard, and the refusal aborts the statement rather
	/// than skipping the record: a write outside the tenant is told so, exactly
	/// as a read of the same table would be.
	#[tokio::test]
	async fn the_tenant_guard_stops_a_foreign_database() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let session = record_user_authenticated_at("test", "other");
		let ctx = write_ctx(&db, &session, "t").await;
		let mut row = row_for("{ id: t:1 }").await;
		let stage = stage(StageId::CheckRecordUserAccess).expect("the stage is registered");
		let ControlFlow::Err(err) = stage.apply(&mut row, &ctx).await.unwrap_err() else {
			panic!("a tenant refusal is an error, not a loop signal");
		};
		assert!(
			err.to_string().contains("permission to change to the test database"),
			"a namespace match with a database mismatch is a database-scope violation, got: {err}"
		);
	}

	/// A principal within its own tenant passes the guard.
	#[tokio::test]
	async fn the_tenant_guard_admits_its_own_database() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS;").await;
		let session = record_user_authenticated_at("test", "test");
		let ctx = write_ctx(&db, &session, "t").await;
		let mut row = row_for("{ id: t:1 }").await;
		let stage = stage(StageId::CheckRecordUserAccess).expect("the stage is registered");
		assert_eq!(stage.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// The quick gate short-circuits on a `NONE` create permission and stands
	/// down on a conditional one. That is what leaves a conditional clause to be
	/// judged after the data clause has run — so a data clause with side effects
	/// runs under the conditional clause and not under the `NONE` one.
	#[tokio::test]
	async fn the_quick_create_gate_answers_only_for_none() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE pnone SCHEMALESS PERMISSIONS FOR create NONE;
			 DEFINE TABLE pcond SCHEMALESS PERMISSIONS FOR create WHERE false;",
		)
		.await;
		let session = anonymous();
		let quick = stage(StageId::CheckPermissionsQuickCreate).expect("the stage is registered");

		let none_ctx = write_ctx(&db, &session, "pnone").await;
		let mut row = row_for("{ id: pnone:1 }").await;
		assert_eq!(quick.apply(&mut row, &none_ctx).await.unwrap(), StageOutcome::SkipRow);

		let cond_ctx = write_ctx(&db, &session, "pcond").await;
		let mut row = row_for("{ id: pcond:1 }").await;
		assert_eq!(quick.apply(&mut row, &cond_ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// An exempt principal is not gated at all, so even a `NONE` clause admits
	/// the write.
	#[tokio::test]
	async fn an_exempt_principal_passes_a_none_clause() {
		let db = TestDb::new("DEFINE TABLE t SCHEMALESS PERMISSIONS FOR create NONE;").await;
		let ctx = write_ctx(&db, &TestDb::owner(), "t").await;
		let mut row = row_for("{ id: t:1 }").await;
		let quick = stage(StageId::CheckPermissionsQuickCreate).expect("the stage is registered");
		assert_eq!(quick.apply(&mut row, &ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// A write clause that refuses skips the record; the statement succeeds
	/// having written nothing for it. Failing instead would abort the whole
	/// statement and discard every record already written.
	#[tokio::test]
	async fn a_refusing_write_clause_skips_rather_than_fails() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FOR update WHERE open = true;",
		)
		.await;
		let ctx = write_ctx(&db, &anonymous(), "t").await;
		let gate = stage(StageId::CheckUpdatePermissions).expect("the stage is registered");

		let mut refused = row_for("{ id: t:1, open: false }").await;
		assert_eq!(gate.apply(&mut refused, &ctx).await.unwrap(), StageOutcome::SkipRow);

		let mut admitted = row_for("{ id: t:2, open: true }").await;
		assert_eq!(gate.apply(&mut admitted, &ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// The recheck stands down unless the update clause is a predicate, which is
	/// what keeps a `FULL` or `NONE` clause from being judged a second time
	/// after the record has already been built.
	#[tokio::test]
	async fn the_recheck_answers_only_for_a_predicate() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE flat SCHEMALESS PERMISSIONS FOR update NONE;
			 DEFINE TABLE cond SCHEMALESS PERMISSIONS FOR update WHERE open = true;",
		)
		.await;
		let session = anonymous();
		let recheck = stage(StageId::RecheckUpdatePermissions).expect("the stage is registered");

		let flat_ctx = write_ctx(&db, &session, "flat").await;
		let mut row = row_for("{ id: flat:1 }").await;
		assert_eq!(recheck.apply(&mut row, &flat_ctx).await.unwrap(), StageOutcome::Continue);

		let cond_ctx = write_ctx(&db, &session, "cond").await;
		let mut row = row_for("{ id: cond:1, open: false }").await;
		assert_eq!(recheck.apply(&mut row, &cond_ctx).await.unwrap(), StageOutcome::SkipRow);
	}

	/// A write predicate naming a `COMPUTED` field is judged against the value
	/// the field would hold, not against the `NONE` a stripped post-image
	/// carries — and the post-image is left untouched, because the store writes
	/// it verbatim.
	#[tokio::test]
	async fn a_write_predicate_reads_a_computed_field() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FOR update WHERE blocked != true;
			 DEFINE FIELD blocked ON t COMPUTED score > 5;",
		)
		.await;
		let ctx = write_ctx(&db, &anonymous(), "t").await;
		let gate = stage(StageId::CheckUpdatePermissions).expect("the stage is registered");

		let mut blocked = row_for("{ id: t:1, score: 9 }").await;
		assert_eq!(gate.apply(&mut blocked, &ctx).await.unwrap(), StageOutcome::SkipRow);
		assert!(
			!carries(&blocked.current, "blocked"),
			"the derived value must not reach the image the store writes"
		);

		let mut open = row_for("{ id: t:2, score: 1 }").await;
		assert_eq!(gate.apply(&mut open, &ctx).await.unwrap(), StageOutcome::Continue);
	}

	/// `DELETE` gates the pre-image, which is the only image it has left by the
	/// time the output gate runs.
	#[tokio::test]
	async fn the_delete_output_gate_reads_the_pre_image() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select WHERE open = true;",
		)
		.await;
		let ctx = write_ctx(&db, &anonymous(), "t").await;
		let gate =
			stage(StageId::CheckSelectPermissions(RecordImage::Initial)).expect("registered");

		// The post-image is blanked by `clear_record_data`, so only the
		// pre-image can answer.
		let mut shown = row_for("{ id: t:1, open: true }").await;
		shown.current = Value::None;
		assert_eq!(gate.apply(&mut shown, &ctx).await.unwrap(), StageOutcome::Continue);

		let mut hidden = row_for("{ id: t:2, open: false }").await;
		hidden.current = Value::None;
		assert_eq!(gate.apply(&mut hidden, &ctx).await.unwrap(), StageOutcome::SkipRow);
	}

	/// A write guard may itself write.
	///
	/// The two permission frames are not the same: a `select` guard is judged
	/// write-refusing, and a create/update/delete guard is not
	///. A guard that records the attempt it is judging —
	/// the shape an audit trail takes — is written that way, and refusing it
	/// would fail every statement against the table rather than deny one.
	#[tokio::test]
	async fn a_write_guard_may_perform_a_write() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE t SCHEMALESS
			   PERMISSIONS FOR update WHERE (UPSERT ONLY seen:1 SET n += 1).n >= 0;
			 DEFINE TABLE seen SCHEMALESS PERMISSIONS FULL;",
		)
		.await;
		let ctx = write_ctx(&db, &anonymous(), "t").await;
		let gate = stage(StageId::CheckUpdatePermissions).expect("the stage is registered");

		let mut row = row_for("{ id: t:1 }").await;
		assert_eq!(
			gate.apply(&mut row, &ctx).await.unwrap(),
			StageOutcome::Continue,
			"the guard's own write must not be refused as a write inside a predicate"
		);
	}
}
