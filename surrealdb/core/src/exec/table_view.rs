//! The per-table read-time view of a record.
//!
//! A [`TableView`] is what a `SELECT` on one table would show a given caller,
//! resolved once and applied per record: the table's compiled `PERMISSIONS FOR
//! select` guard together with the table's [`FieldState`] — the `COMPUTED`
//! bodies to derive and the per-field `PERMISSIONS FOR select` guards to
//! enforce.
//!
//! # What it does not own
//!
//! The view takes an **already-decoded record**. Reading is the caller's: the
//! record-by-id paths issue a single `get_record`, the batched paths a
//! `get_records` whose cache policy they choose, and some of them bind an id
//! without reading at all. A view that owned the read could serve only one of
//! those.
//!
//! [`TableView::apply`] likewise reports *visibility* rather than producing a
//! value, because callers represent a hidden record differently: a positional
//! `None` that keeps a candidate row's index, an element compacted out of a
//! batch, or `Value::None` substituted for a dereference.
//!
//! The tenant guard ([`crate::exec::permission::validate_record_user_access`])
//! is not applied here. It belongs at the point a batch of ids enters the
//! executor, which for the scan operators is upstream of any view.
//!
//! # Resolution scope
//!
//! [`TableViews`] resolves and caches one view per table for its own lifetime,
//! which is the owning operator's. Resolution compiles physical expressions and
//! reads the catalog, so it must not happen per record.
//!
//! Three things key a view and are therefore fixed for a resolver:
//!
//! - the table, which is the map key;
//! - `check_perms`, resolved once from the session, because [`FieldState`] itself is cached per
//!   `(table, check_perms)` and a view built with checks bypassed carries no field guards at all;
//! - the version stamp, because both the table guard and the [`FieldState`] bypass their
//!   database-context caches for a versioned read, and a view resolved at one stamp describes the
//!   schema at that stamp. A stamp change empties the resolver.
//!
//! A resolver must not be shared across contexts that differ in
//! `skip_fetch_perms`: that flag decides `check_perms`, which the resolver
//! remembers from the first context it sees.
//!
//! # Order of enforcement
//!
//! `apply` runs the table guard against the record as stored, then derives and
//! guards the fields. The field half is
//! [`materialise_fields_with_permissions`], the same kernel the scan pipeline
//! runs, which gives the reduce semantics: every guard is expanded through [`Value::each`], so it
//! is judged once per path the record actually carries and not at all for a path it
//! lacks; a denied path is cut, leaving the key absent rather than
//! `NONE`-valued; cuts run in reverse path order, so removing an array element
//! cannot invalidate a pending index; and a `COMPUTED` body is re-derived after
//! the stored guards have cut, so it cannot carry a value the caller's field
//! permissions deny.

// The surface is the contract the record-resolving paths share, not the subset
// any single one of them calls; this module's own tests exercise all of it.
#![cfg_attr(not(test), allow(dead_code))]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::exec::ExecutionContext;
use crate::exec::operators::fetch::table_select_permission;
use crate::exec::operators::scan::pipeline::{
	FieldState, build_field_state, compute_fields_for_value, materialise_fields_with_permissions,
};
use crate::exec::permission::{PhysicalPermission, check_permission_for_value, should_check_perms};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::val::{TableName, Value};

/// One table's read-time view, resolved for one caller.
///
/// Cheap to apply and expensive to build: construct it through [`TableViews`],
/// which holds it for the operator's lifetime.
#[derive(Debug, Clone)]
pub(crate) struct TableView {
	/// The table's compiled `PERMISSIONS FOR select` clause. [`Allow`] when
	/// the session bypasses permission checks, so the guard need not be
	/// consulted separately from `check_perms`.
	///
	/// [`Allow`]: PhysicalPermission::Allow
	permission: PhysicalPermission,
	/// The table's `COMPUTED` bodies and per-field `PERMISSIONS FOR select`
	/// guards. Built with `check_perms`, which decides whether it carries any
	/// field guards at all.
	field_state: FieldState,
	/// Whether this caller's reads are permission-checked. False for root and
	/// owner identities, with auth disabled, and inside a permission predicate.
	check_perms: bool,
}

impl TableView {
	/// Resolve the view of `table` a caller with this `check_perms` sees.
	///
	/// `needed_fields` narrows the `COMPUTED` bodies to those a projection
	/// requires; `None` derives every one. Narrowing never drops a body a field
	/// guard depends on.
	pub(crate) async fn resolve(
		ctx: &ExecutionContext,
		table: &TableName,
		check_perms: bool,
		needed_fields: Option<&HashSet<String>>,
	) -> Result<Self, ControlFlow> {
		// With checks bypassed there is no guard to compile and no catalog read
		// to pay for: the table is unconditionally visible and the field state
		// carries no guards either.
		let permission = if check_perms {
			table_select_permission(ctx, table).await?
		} else {
			PhysicalPermission::Allow
		};
		let field_state = build_field_state(ctx, table, check_perms, needed_fields).await?;
		Ok(Self {
			permission,
			field_state,
			check_perms,
		})
	}

	/// Apply this table's read-time view to one already-read record, in place.
	///
	/// Returns `false` when the table's `PERMISSIONS FOR select` clause hides
	/// the record, in which case `value` is left as it was read and the caller
	/// decides how a hidden record is represented. Returns `true` with `value`
	/// reduced to what the caller may see: `COMPUTED` fields derived, fields
	/// the caller may not select cut.
	///
	/// `skip_fetch_perms` is read from the context rather than taken as an
	/// argument. It governs whether a record dereference inside a `COMPUTED`
	/// body re-enters permission checking, and it is set precisely when this
	/// runs inside a permission predicate, where re-entering would recurse
	/// without end on a cyclic link. `check_perms` is already false whenever it
	/// is set, so it changes nothing about the guards run here.
	pub(crate) async fn apply(
		&self,
		ctx: &ExecutionContext,
		value: &mut Value,
	) -> Result<bool, ControlFlow> {
		if self.check_perms {
			let allowed = check_permission_for_value(&self.permission, value, None, ctx)
				.await
				.context("Failed to check the table permission")?;
			if !allowed {
				return Ok(false);
			}
		}
		materialise_fields_with_permissions(
			ctx,
			&self.field_state,
			value,
			ctx.root().skip_fetch_perms,
			self.check_perms,
		)
		.await?;
		Ok(true)
	}

	/// Derive this table's `COMPUTED` fields on one record and enforce nothing.
	///
	/// The deliberately degraded view: no table guard, no field guards, and
	/// record dereferences inside the bodies take the same unchecked path. It
	/// serves a dereference made from inside a permission predicate, where
	/// checking again would recurse without end on a cyclic link, and the value
	/// it produces is only ever read by that predicate.
	///
	/// It is not a degenerate [`Self::apply`]: a view resolved with
	/// `check_perms` still holds the field guards, and this skips them.
	pub(crate) async fn compute_only(
		&self,
		ctx: &ExecutionContext,
		value: &mut Value,
	) -> Result<(), ControlFlow> {
		compute_fields_for_value(ctx, &self.field_state, value, true).await
	}

	/// The table's compiled `PERMISSIONS FOR select` clause, for a caller that
	/// gates on the table without reading a record — binding an id it never
	/// materialises, say.
	pub(crate) fn permission(&self) -> &PhysicalPermission {
		&self.permission
	}

	/// The table's `COMPUTED` bodies and field guards.
	pub(crate) fn field_state(&self) -> &FieldState {
		&self.field_state
	}

	/// Whether this view enforces anything. False makes [`Self::apply`] a
	/// `COMPUTED`-field derivation and nothing else.
	pub(crate) fn checked(&self) -> bool {
		self.check_perms
	}
}

/// The per-table views an operator has resolved, built on demand and held for
/// the operator's lifetime.
///
/// One record-resolving operator owns one of these and threads it through every
/// batch, so each distinct table's guard compilation, `COMPUTED`-body
/// compilation and dependency analysis happen at most once.
#[derive(Debug)]
pub(crate) struct TableViews {
	views: HashMap<TableName, TableView>,
	/// `Some` once resolved from the session. Resolved lazily so an operator
	/// that resolves no record pays nothing.
	check_perms: Option<bool>,
	/// The version stamp the held views describe. A context reading at a
	/// different stamp describes a different schema, so the views are dropped.
	version: Option<u64>,
	/// The projection's required fields, shared by every view this resolver
	/// builds. `None` derives every `COMPUTED` field.
	needed_fields: Option<Arc<HashSet<String>>>,
}

impl TableViews {
	/// A resolver that derives every `COMPUTED` field of every table it sees.
	pub(crate) fn new() -> Self {
		Self {
			views: HashMap::new(),
			check_perms: None,
			version: None,
			needed_fields: None,
		}
	}

	/// A resolver whose views derive only the `COMPUTED` fields a projection
	/// requires, plus those any field guard depends on.
	pub(crate) fn for_projection(needed_fields: Arc<HashSet<String>>) -> Self {
		Self {
			needed_fields: Some(needed_fields),
			..Self::new()
		}
	}

	/// Whether this caller's reads are permission-checked, resolved once from
	/// the session.
	///
	/// Exposed for callers that bind a record id without reading the record and
	/// must still decide whether to gate the traversal on the table's guard.
	pub(crate) fn check_perms(&mut self, ctx: &ExecutionContext) -> Result<bool, ControlFlow> {
		if let Some(flag) = self.check_perms {
			return Ok(flag);
		}
		let db_ctx = ctx.database().context("a table view requires a database context")?;
		let flag = should_check_perms(db_ctx, Action::View)
			.map_err(|e| ControlFlow::Err(anyhow::Error::new(e)))?;
		self.check_perms = Some(flag);
		Ok(flag)
	}

	/// The view of `table`, resolving it on first use.
	pub(crate) async fn view_for(
		&mut self,
		ctx: &ExecutionContext,
		table: &TableName,
	) -> Result<&TableView, ControlFlow> {
		let version = ctx.version_stamp();
		if version != self.version {
			self.views.clear();
			self.version = version;
		}
		let check_perms = self.check_perms(ctx)?;
		if !self.views.contains_key(table) {
			// Cloning the handle ends the borrow on `self` before the map is
			// borrowed mutably.
			let needed = self.needed_fields.clone();
			let view = TableView::resolve(ctx, table, check_perms, needed.as_deref()).await?;
			self.views.insert(table.clone(), view);
		}
		Ok(self.views.get(table).expect("the view was just resolved"))
	}
}

impl Default for TableViews {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::Session;
	use crate::exec::operators::fetch::fetch_raw_record;
	use crate::exec::operators::test_util::TestDb;
	use crate::kvs::TransactionType;
	use crate::val::{RecordId, RecordIdKey};

	/// An anonymous session on `test`/`test`: permission-checked under a
	/// datastore built with server auth enabled.
	fn anonymous() -> Session {
		Session::default().with_ns("test").with_db("test")
	}

	/// `t:<n>`.
	fn rid(n: i64) -> RecordId {
		RecordId {
			table: "t".into(),
			key: RecordIdKey::Number(n),
		}
	}

	/// The record as stored, with its id spliced in and nothing derived or cut.
	async fn stored(ctx: &ExecutionContext, n: i64) -> Value {
		fetch_raw_record(ctx, &rid(n), None)
			.await
			.expect("the read succeeds")
			.expect("the record exists")
	}

	/// The view `ctx`'s caller gets of `t:<n>`, and whether the table guard
	/// admitted it.
	async fn view_of(ctx: &ExecutionContext, n: i64) -> (bool, Value) {
		let mut views = TableViews::new();
		let view = views.view_for(ctx, &TableName::from("t")).await.expect("the view resolves");
		let mut value = stored(ctx, n).await;
		let visible = view.apply(ctx, &mut value).await.expect("the view applies");
		(visible, value)
	}

	/// A top-level field of an object row, `NONE` when the key is absent.
	fn field(row: &Value, name: &str) -> Value {
		if let Value::Object(obj) = row {
			obj.get(name).cloned().unwrap_or(Value::None)
		} else {
			Value::None
		}
	}

	/// Whether an object row carries `name` at all. A denied field is cut, so
	/// its key is absent rather than present and `NONE`.
	fn carries(row: &Value, name: &str) -> bool {
		matches!(row, Value::Object(obj) if obj.get(name).is_some())
	}

	/// A `COMPUTED` body is derived from the record and lands on the view.
	#[tokio::test]
	async fn a_computed_field_is_derived_on_the_record() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMAFULL PERMISSIONS FULL;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD label ON t COMPUTED 'n=' + <string> n;
			CREATE t:1 SET n = 1;
			",
		)
		.await;
		let session = anonymous();
		let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;

		assert_eq!(field(&stored(&ctx, 1).await, "label"), Value::None, "not stored");

		let (visible, row) = view_of(&ctx, 1).await;
		assert!(visible, "the table is readable");
		assert_eq!(field(&row, "label"), Value::from("n=1"));
	}

	/// A field the caller may not select is cut from the view, so reading it
	/// yields `NONE` because the key is gone — not because it was set to `NONE`.
	#[tokio::test]
	async fn a_denied_field_is_cut_rather_than_set_to_none() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;
			DEFINE FIELD guarded ON t TYPE int PERMISSIONS FOR select WHERE $value > 10;
			CREATE t:1 SET n = 1, secret = 's', guarded = 1;
			CREATE t:2 SET n = 2, secret = 's', guarded = 11;
			",
		)
		.await;
		let session = anonymous();
		let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;

		let (visible, row) = view_of(&ctx, 1).await;
		assert!(visible);
		assert!(!carries(&row, "secret"), "a `NONE` clause cuts: {row:?}");
		assert!(!carries(&row, "guarded"), "a clause that does not hold cuts: {row:?}");
		assert_eq!(field(&row, "n"), Value::from(1), "an unrestricted field is kept");

		// The same clause against a record it holds for keeps the field, so the
		// cut above is the clause's answer and not a blanket removal.
		let (visible, row) = view_of(&ctx, 2).await;
		assert!(visible);
		assert_eq!(field(&row, "guarded"), Value::from(11));
	}

	/// A `COMPUTED` body that reads a field the caller may not select must see
	/// that field as the caller sees it — cut — and not the value it holds in
	/// storage. The field guards are therefore enforced before the body's value
	/// is settled, even though the body must be derived first for a guard that
	/// reads one to judge it.
	#[tokio::test]
	async fn a_computed_body_reads_the_guarded_value_of_a_restricted_field() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD secret ON t TYPE option<string> PERMISSIONS FOR select NONE;
			DEFINE FIELD echo ON t COMPUTED secret;
			CREATE t:1 SET secret = 's';
			",
		)
		.await;
		let session = anonymous();
		let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;

		let (visible, row) = view_of(&ctx, 1).await;
		assert!(visible);
		assert!(!carries(&row, "secret"), "the source field is cut: {row:?}");
		assert_eq!(
			field(&row, "echo"),
			Value::None,
			"a body reading a cut field must not carry its stored value: {row:?}"
		);
	}

	/// A `SCHEMALESS` table with nothing declared has no view to apply: the
	/// record reaches the caller as stored.
	#[tokio::test]
	async fn a_schemaless_table_passes_the_record_through() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			CREATE t:1 SET n = 1, nested = { a: 1 }, arr = [1, 2];
			",
		)
		.await;
		let session = anonymous();
		let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;

		let as_stored = stored(&ctx, 1).await;
		let (visible, row) = view_of(&ctx, 1).await;
		assert!(visible);
		assert_eq!(row, as_stored, "nothing is derived and nothing is cut");
	}

	/// A guard on a wildcard path is judged once per position the record
	/// carries, and cuts each of them independently.
	#[tokio::test]
	async fn a_wildcard_field_guard_cuts_every_matching_path() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD arr ON t TYPE array<object>;
			DEFINE FIELD arr[*].secret ON t TYPE option<string> PERMISSIONS FOR select NONE;
			CREATE t:1 SET arr = [
				{ secret: 'a', ok: 1 },
				{ secret: 'b', ok: 2 },
				{ ok: 3 }
			];
			",
		)
		.await;
		let session = anonymous();
		let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;

		let (visible, row) = view_of(&ctx, 1).await;
		assert!(visible);
		let Value::Array(arr) = field(&row, "arr") else {
			panic!("`arr` is an array: {row:?}");
		};
		assert_eq!(arr.len(), 3, "no element is removed, only the guarded key: {row:?}");
		for (i, element) in arr.iter().enumerate() {
			assert!(!carries(element, "secret"), "element {i} keeps a cut key: {row:?}");
			assert_eq!(field(element, "ok"), Value::from(i as i64 + 1), "siblings survive");
		}
	}

	/// A field's guard is judged once per path the record carries and not at all
	/// for a path it lacks: the guard of a field absent from a record never
	/// runs, so one that is total over the field's declared kind but not over
	/// `NONE` cannot fail on a record that simply omits it.
	///
	/// DIVERGENCE — NEEDS A DECISION. Two encodings of the same declaration give
	/// different answers here, and only one of them is asserted below.
	///
	/// This is the [`Value::each`]-driven answer: the one this primitive gives,
	/// because it expands the guard over the paths the record carries. The plan-time layer encoding
	/// in `exec::operators::layer` instead compiles a guard into a projection
	/// column, which is evaluated once per row with `$value` bound to `NONE`
	/// when the row lacks the field. Measured on this schema, a `SELECT` under
	/// that encoding fails outright — the guard throws on `t:1` — where the
	/// each-driven one returns both records. A guard with an observable effect
	/// rather than a throw likewise runs a different number of times: once per
	/// row under the column encoding, twice per row carrying the path and never
	/// otherwise under the each-driven one.
	///
	/// So the two are not interchangeable for any guard that can raise or that
	/// has an effect, and rewriting one into the other is not a
	/// semantics-preserving change. Which answer is intended is unsettled; the
	/// each-driven one is pinned here so a decision is a deliberate edit.
	#[tokio::test]
	async fn a_field_guard_is_not_judged_for_a_row_lacking_the_field() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD secret ON t TYPE option<string>
				PERMISSIONS FOR select
					WHERE IF $value = NONE THEN THROW 'judged without the field' ELSE true END;
			CREATE t:1 SET n = 1;
			CREATE t:2 SET n = 2, secret = 's';
			",
		)
		.await;
		let session = anonymous();
		let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;

		let (visible, row) = view_of(&ctx, 1).await;
		assert!(visible);
		assert!(!carries(&row, "secret"), "the record does not carry it: {row:?}");
		assert_eq!(field(&row, "n"), Value::from(1));

		// The same guard on a record that does carry the field is judged, and
		// admits it.
		let (visible, row) = view_of(&ctx, 2).await;
		assert!(visible);
		assert_eq!(field(&row, "secret"), Value::from("s"));
	}

	/// The table's own clause decides whether the record is visible at all, and
	/// a hidden record is reported rather than blanked, so each caller keeps its
	/// own representation of one.
	#[tokio::test]
	async fn the_table_guard_decides_visibility_before_any_field_is_touched() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select WHERE n > 10;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD label ON t COMPUTED 'derived';
			CREATE t:1 SET n = 1;
			CREATE t:2 SET n = 11;
			",
		)
		.await;
		let session = anonymous();
		let ctx = db.exec_ctx_as(&session, TransactionType::Read).await;

		let (visible, row) = view_of(&ctx, 1).await;
		assert!(!visible, "the clause does not hold for this record");
		assert_eq!(row, stored(&ctx, 1).await, "a hidden record is left as it was read");

		let (visible, row) = view_of(&ctx, 2).await;
		assert!(visible);
		assert_eq!(field(&row, "label"), Value::from("derived"));
	}

	/// A caller whose reads are not permission-checked gets the record as
	/// stored, with the `COMPUTED` fields derived and nothing cut.
	#[tokio::test]
	async fn an_unchecked_caller_sees_every_field() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;
			DEFINE FIELD label ON t COMPUTED 'derived';
			CREATE t:1 SET secret = 's';
			",
		)
		.await;
		let ctx = db.exec_ctx().await;

		let mut views = TableViews::new();
		assert!(!views.check_perms(&ctx).expect("resolves"), "owner reads are not checked");
		let view = views.view_for(&ctx, &TableName::from("t")).await.expect("the view resolves");
		assert!(!view.checked());
		assert!(matches!(view.permission(), PhysicalPermission::Allow));
		assert!(view.field_state().field_permissions.is_empty(), "no guard is compiled");

		let mut row = stored(&ctx, 1).await;
		assert!(view.apply(&ctx, &mut row).await.expect("the view applies"));
		assert_eq!(field(&row, "secret"), Value::from("s"), "nothing is cut");
		assert_eq!(field(&row, "label"), Value::from("derived"), "the body still runs");
	}

	/// `compute_only` derives the `COMPUTED` fields and enforces neither the
	/// table's clause nor any field's, so the value it produces is the record as
	/// stored plus its derivations.
	#[tokio::test]
	async fn compute_only_derives_computed_fields_and_enforces_nothing() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select WHERE n > 10;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;
			DEFINE FIELD label ON t COMPUTED 'derived';
			CREATE t:1 SET n = 1, secret = 's';
			",
		)
		.await;
		let ctx = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;

		let mut views = TableViews::new();
		assert!(views.check_perms(&ctx).expect("resolves"), "an anonymous read is checked");
		let view = views.view_for(&ctx, &TableName::from("t")).await.expect("the view resolves");

		let mut row = stored(&ctx, 1).await;
		view.compute_only(&ctx, &mut row).await.expect("the bodies run");
		assert_eq!(field(&row, "label"), Value::from("derived"));
		assert_eq!(field(&row, "secret"), Value::from("s"), "the field guard is not applied");
		assert_eq!(field(&row, "n"), Value::from(1), "the table clause is not applied");
	}

	/// A resolver built for a projection derives only the `COMPUTED` fields the
	/// projection needs — and, whatever the projection asks for, every one a
	/// field guard reads, so a guard is never judged against a row missing a
	/// value it names.
	#[tokio::test]
	async fn a_projection_narrows_the_bodies_but_never_a_guard_s_dependency() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD n ON t TYPE int;
			DEFINE FIELD wanted ON t COMPUTED 'w';
			DEFINE FIELD unwanted ON t COMPUTED 'u';
			DEFINE FIELD needed ON t COMPUTED 'x';
			DEFINE FIELD guarded ON t TYPE option<int> PERMISSIONS FOR select WHERE needed = 'x';
			CREATE t:1 SET n = 1, guarded = 5;
			",
		)
		.await;
		let ctx = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;

		let needed_fields = Arc::new(HashSet::from(["wanted".to_owned()]));
		let mut views = TableViews::for_projection(needed_fields);
		let view = views.view_for(&ctx, &TableName::from("t")).await.expect("the view resolves");

		let mut row = stored(&ctx, 1).await;
		assert!(view.apply(&ctx, &mut row).await.expect("the view applies"));
		assert_eq!(field(&row, "wanted"), Value::from("w"), "the projection's body runs");
		assert!(!carries(&row, "unwanted"), "a body nothing needs does not: {row:?}");
		assert_eq!(
			field(&row, "guarded"),
			Value::from(5),
			"a guard reading a body outside the projection still judges on its value: {row:?}"
		);
	}

	/// A resolver holds one view per table for its lifetime, and drops what it
	/// holds when the stamp it read at changes, because a view describes the
	/// schema at one point in time.
	#[tokio::test]
	async fn a_resolver_holds_one_view_per_table_until_the_version_stamp_moves() {
		let db = TestDb::new_with_auth(
			"
			DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			DEFINE FIELD label ON t COMPUTED 'derived';
			CREATE t:1;
			",
		)
		.await;
		let ctx = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;

		let mut views = TableViews::new();
		views.view_for(&ctx, &TableName::from("t")).await.expect("the view resolves");
		assert_eq!(views.views.len(), 1);
		views.view_for(&ctx, &TableName::from("t")).await.expect("the view resolves");
		assert_eq!(views.views.len(), 1, "the same table resolves once");

		views.version = Some(1);
		views.view_for(&ctx, &TableName::from("t")).await.expect("the view resolves");
		assert_eq!(views.version, ctx.version_stamp(), "the resolver follows the context");
		assert_eq!(views.views.len(), 1, "and holds only views resolved at that stamp");
	}
}
