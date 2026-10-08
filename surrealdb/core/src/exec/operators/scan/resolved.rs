//! Plan-time resolved table context.
//!
//! When a table source is planned with a transaction at hand, the planner
//! reads the table's definition once and hands the scan a
//! [`ResolvedTableContext`], so `execute()` performs no catalog lookups of its
//! own. The context also states who enforces what the schema declares for the
//! rows — the table `SELECT` permission, the computed fields and the
//! field-level `SELECT` permissions — as a [`RowEnforcement`]:
//!
//! - A source the analyzer resolved (`LogicalOp::Source(LogicalSource { schema: Some(_), .. })`)
//!   carries those declarations in the plan, as a `PermissionFilter` and a stack of projection
//!   layers above the scan. The scan is planned [`EnforcementMode::Planned`] and yields rows as
//!   stored; a scan that reduced them itself would hand the nodes above an already-cut row, and a
//!   computed field's own guard, written to read a stored field raw, would be judged against the
//!   cut.
//! - A source the analyzer did not resolve carries nothing, so the scan is planned
//!   [`EnforcementMode::Runtime`] and applies all three per row, as the scan pipeline does.
//!
//! The session-level gates (`should_check_perms`, `validate_record_user_access`)
//! remain the scan's, evaluated at execution time against the authority the
//! executor set up.

#[cfg(test)]
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

#[cfg(test)]
use surrealdb_strand::TableName;

#[cfg(test)]
use super::pipeline::build_field_state_raw;
use super::pipeline::{FieldState, filter_field_state_for_projection};
#[cfg(test)]
use crate::catalog::{DatabaseId, FieldDefinition, NamespaceId};
use crate::catalog::{Permission, TableDefinition};
#[cfg(test)]
use crate::err::{EngineError, Error};
use crate::exec::permission::PhysicalPermission;
#[cfg(test)]
use crate::exec::permission::table_select_permission_physical;
#[cfg(test)]
use crate::exec::planner::Planner;
use crate::expr::logical::Schema;
#[cfg(test)]
use crate::expr::logical::{LogicalOp, LogicalSource};

/// How a `Source` node asks its scan to be planned.
///
/// Decided by the physical planner from the node's `schema`: a resolved schema
/// means the analyzer inserted above the source whatever the read needs, and
/// nothing else may apply it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnforcementMode {
	/// The plan enforces; the scan yields rows as stored.
	Planned,
	/// The scan enforces per row.
	Runtime,
}

impl EnforcementMode {
	/// The mode for a source with the given `schema`.
	pub(crate) fn for_source(schema: Option<&Arc<Schema>>) -> Self {
		if schema.is_some() {
			Self::Planned
		} else {
			Self::Runtime
		}
	}

	#[cfg(test)]
	/// The mode for a pipeline's sources: [`Self::Planned`] when there is one
	/// source and the analyzer resolved it, [`Self::Runtime`] otherwise.
	pub(crate) fn of_sources(sources: &[LogicalOp]) -> Self {
		match sources {
			[
				LogicalOp::Source(LogicalSource {
					schema,
					..
				}),
			] => Self::for_source(schema.as_ref()),
			_ => Self::Runtime,
		}
	}
}

/// Who applies the schema-derived behaviours to the rows a scan reads.
// Constructed or read only by the operator-tree oracle, which test builds compile.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) enum RowEnforcement {
	/// The plan enforces: a `PermissionFilter` and the projection layers above
	/// this scan apply the table permission, computed fields and field
	/// permissions, so rows leave the scan as stored, and
	/// [`ResolvedTableContext::select_permission`] answers `Allow` whatever
	/// the table's `SELECT` clause says. `select_permission` is carried for
	/// the one reader that is not the plan: a top-k scan (KNN) applies the
	/// guard to its candidates so that k counts rows the caller may see, and
	/// its context carries the compiled guard, set by
	/// [`ResolvedTableContext::with_candidate_guard`].
	Planned {
		select_permission: PhysicalPermission,
		/// What the table's field definitions read, for deciding whether the
		/// scan may decode only part of each record.
		field_reads: DefinedFieldReads,
	},
	/// The scan enforces per row: the analyzer did not resolve this source, so
	/// nothing above the scan applies what the schema declares.
	Runtime {
		select_permission: PhysicalPermission,
		/// The table's whole field state; scans narrow it to their projection
		/// with [`ResolvedTableContext::field_state_for_projection`].
		field_state: Arc<FieldState>,
	},
}

/// Whether a table's field definitions evaluate anything against a row beyond
/// the fields a statement names: a computed field, which may read any stored
/// field, or a field-level `SELECT` permission, which reads the field it
/// guards.
// Constructed or read only by the operator-tree oracle, which test builds compile.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DefinedFieldReads {
	/// The table defines a computed field.
	pub(crate) computed: bool,
	/// A field's `SELECT` permission is not `FULL`.
	pub(crate) restricted: bool,
}

impl DefinedFieldReads {
	#[cfg(test)]
	/// What `fields`, a table's field definitions, read.
	pub(crate) fn of(fields: &[FieldDefinition]) -> Self {
		Self {
			computed: fields.iter().any(|fd| fd.computed.is_some()),
			restricted: fields.iter().any(|fd| !matches!(fd.select_permission, Permission::Full)),
		}
	}
}

/// Plan-time resolved table metadata, in place of runtime catalog lookups.
///
/// Carries the table definition and the [`RowEnforcement`] the scan was
/// planned under. With one of these, a scan's `execute()` performs no async
/// metadata resolution; only the synchronous session gates remain.
// Constructed or read only by the operator-tree oracle, which test builds compile.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
pub(crate) struct ResolvedTableContext {
	/// The resolved table definition (used for ns_id/db_id/table_id).
	pub table_def: Arc<TableDefinition>,
	/// Who enforces the schema-derived behaviours on the rows.
	pub enforcement: RowEnforcement,
}

impl ResolvedTableContext {
	/// The table permission this scan applies to every row it reads.
	///
	/// `Allow` when the session is not permission-checked, and whenever the
	/// plan enforces — including for a `SELECT` clause of `NONE`. Denying
	/// there would read as a short circuit of the filter above, but what that
	/// filter carries is not always the `SELECT` clause: the row source of a
	/// mutation is resolved under the statement's own clause, so a table a
	/// caller may delete from but not read has no filter above this scan and
	/// every row it needs has to reach it.
	pub(crate) fn select_permission(&self, check_perms: bool) -> PhysicalPermission {
		if !check_perms {
			return PhysicalPermission::Allow;
		}
		match &self.enforcement {
			RowEnforcement::Runtime {
				select_permission,
				..
			} => select_permission.clone(),
			RowEnforcement::Planned {
				..
			} => PhysicalPermission::Allow,
		}
	}

	#[cfg(test)]
	/// The table permission a top-k scan applies to its candidates, in either
	/// mode: the k neighbours it returns have to be k the caller may see, and a
	/// filter above the scan cannot widen a result already cut to k. `Allow`
	/// when the session is not permission-checked. A planned context carries
	/// the guard only once [`Self::with_candidate_guard`] has compiled it.
	pub(crate) fn candidate_permission(&self, check_perms: bool) -> PhysicalPermission {
		if !check_perms {
			return PhysicalPermission::Allow;
		}
		match &self.enforcement {
			RowEnforcement::Planned {
				select_permission,
				..
			}
			| RowEnforcement::Runtime {
				select_permission,
				..
			} => select_permission.clone(),
		}
	}

	#[cfg(test)]
	/// This context with the table's conditional `SELECT` guard compiled for a
	/// top-k scan's candidates. A planned context otherwise carries no
	/// compiled guard — the plan's filter judges it — so a top-k scan planned
	/// under the plan's enforcement asks for it here; a runtime context and a
	/// planned one whose permission is unconditional are returned unchanged.
	pub(crate) async fn with_candidate_guard(self, planner: &Planner<'_>) -> Result<Self, Error> {
		let field_reads = match (&self.enforcement, &self.table_def.permissions.select) {
			(
				RowEnforcement::Planned {
					select_permission: PhysicalPermission::Allow,
					field_reads,
				},
				Permission::Specific(_),
			) => *field_reads,
			_ => return Ok(self),
		};
		let select_permission =
			table_select_permission_physical(Some(&self.table_def), planner).await?;
		Ok(Self {
			table_def: self.table_def,
			enforcement: RowEnforcement::Planned {
				select_permission,
				field_reads,
			},
		})
	}

	/// The field state this scan applies, narrowed to `needed_fields`. Empty
	/// when the plan enforces.
	pub(crate) fn field_state_for_projection(
		&self,
		needed_fields: Option<&HashSet<String>>,
	) -> FieldState {
		match &self.enforcement {
			RowEnforcement::Planned {
				..
			} => FieldState::empty(),
			RowEnforcement::Runtime {
				field_state,
				..
			} => filter_field_state_for_projection(field_state, needed_fields),
		}
	}

	#[cfg(test)]
	/// Whether a scan of this table may decode only part of each record; see
	/// [`super::common::projected_decode_applies`], which a runtime context
	/// answers from its field state and a planned one from the
	/// [`DefinedFieldReads`] it resolved at plan time.
	pub(crate) fn projected_decode_applies(
		&self,
		select_permission: &PhysicalPermission,
		check_perms: bool,
	) -> bool {
		match &self.enforcement {
			RowEnforcement::Planned {
				field_reads,
				..
			} => {
				!field_reads.computed
					&& (!check_perms
						|| (matches!(select_permission, PhysicalPermission::Allow)
							&& !field_reads.restricted))
			}
			RowEnforcement::Runtime {
				field_state,
				..
			} => {
				super::common::projected_decode_applies(select_permission, check_perms, field_state)
			}
		}
	}

	#[cfg(test)]
	/// The table's whole field state, unnarrowed by any projection — what a
	/// plan-time eligibility check reads. Empty when the plan enforces.
	pub(crate) fn field_state(&self) -> Cow<'_, FieldState> {
		match &self.enforcement {
			RowEnforcement::Planned {
				..
			} => Cow::Owned(FieldState::empty()),
			RowEnforcement::Runtime {
				field_state,
				..
			} => Cow::Borrowed(field_state),
		}
	}
}

#[cfg(test)]
/// Resolve table context at plan time.
///
/// Reads the table definition as of `version` — the stamp the scan will read
/// its rows at, `None` for the transaction's current state. Under
/// [`EnforcementMode::Runtime`] it compiles the table's `SELECT` permission,
/// reads the field definitions at that stamp and builds the field state;
/// under [`EnforcementMode::Planned`] the plan's nodes enforce, so only
/// whether the permission is `NONE` is kept (see [`RowEnforcement::Planned`]).
/// Returns `None` if the table does not exist at that version.
///
/// The field state is compiled with field-level permissions included, since
/// the planner has no auth context; the scan skips their evaluation at runtime
/// when the session is not permission-checked.
///
/// `planner` provides the transaction, ns/db context, and the
/// [`crate::exec::planner::CycleGuard`] used to break compile-time cycles
/// for self-referential permissions / computed fields. The caller (the
/// only legitimate one is `try_resolve_table_ctx`) is responsible for
/// pushing `(ns_id, db_id, table_name)` onto the guard *before* invoking
/// this function.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_table_context(
	planner: &Planner<'_>,
	ns: &str,
	db: &str,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table_name: &TableName,
	mode: EnforcementMode,
	version: Option<u64>,
) -> Result<Option<ResolvedTableContext>, Error> {
	use crate::catalog::providers::TableProvider;

	let txn = planner.txn().ok_or_else(|| {
		EngineError::Internal("resolve_table_context requires a planner with txn".into())
	})?;

	// Look up table definition
	let table_def = match txn
		.get_tb_by_name(ns, db, table_name, version)
		.await
		.map_err(|e| EngineError::Internal(e.to_string()))?
	{
		Some(def) => def,
		None => return Ok(None),
	};

	let enforcement = match mode {
		EnforcementMode::Planned => {
			let fields = txn
				.all_tb_fields(ns_id, db_id, table_name, version)
				.await
				.map_err(|e| EngineError::Internal(e.to_string()))?;
			RowEnforcement::Planned {
				select_permission: planned_select_permission(&table_def),
				field_reads: DefinedFieldReads::of(&fields),
			}
		}
		EnforcementMode::Runtime => {
			// Pre-compile the SELECT permission at plan time. Inner subqueries
			// inherit the planner's `CycleGuard` via `planner.physical_expr(...)`
			// → SELECT planning → `try_resolve_table_ctx` → guard check. A
			// self-referential permission like `WHERE (SELECT FROM same_table)
			// != NONE` will fall back to runtime resolution for the inner
			// subtree only; the outer `table_def` here is fully resolved.
			let select_permission =
				table_select_permission_physical(Some(&table_def), planner).await?;
			let field_state =
				build_field_state_raw(planner, ns_id, db_id, table_name, true, version)
					.await
					.map_err(|cf| match cf {
						crate::expr::ControlFlow::Err(e) => EngineError::Internal(e.to_string()),
						_ => EngineError::Internal(
							"Unexpected control flow in field state resolution".into(),
						),
					})?;
			RowEnforcement::Runtime {
				select_permission,
				field_state: Arc::new(field_state),
			}
		}
	};

	Ok(Some(ResolvedTableContext {
		table_def,
		enforcement,
	}))
}

/// The table `SELECT` permission a scan carries under
/// [`EnforcementMode::Planned`], for the readers that are not the plan: `Deny`
/// for a permission of `NONE` and `Allow` otherwise, the plan judging a
/// conditional guard. Read off the definition the scan runs with, whether
/// resolved when the plan was made or when it runs.
///
/// This is not what the scan applies to the rows it reads — see
/// [`ResolvedTableContext::select_permission`], which admits every row under
/// this mode.
pub(crate) fn planned_select_permission(table_def: &TableDefinition) -> PhysicalPermission {
	match table_def.permissions.select {
		Permission::None => PhysicalPermission::Deny,
		Permission::Full | Permission::Specific(_) => PhysicalPermission::Allow,
	}
}

/// Fixtures shared by the scan operators' enforcement tests.
///
/// Each scan asserts the same contract: planned under
/// [`EnforcementMode::Planned`] it yields the stored record — the restricted
/// field present, the computed field absent — and planned under
/// [`EnforcementMode::Runtime`] it reduces the record for a permission-checked
/// caller. The helpers here build both contexts for a table from a live
/// database context, the way the planner would, so each scan's test states only
/// how that scan is constructed.
#[cfg(test)]
pub(crate) mod test_support {
	use std::sync::Arc;

	use super::*;
	use crate::dbs::Session;
	use crate::exec::ExecutionContext;
	use crate::exec::index::access_path::IndexRef;
	use crate::exec::operators::scan::pipeline::build_field_state;
	use crate::exec::permission::table_select_permission_physical_runtime;
	use crate::val::Value;

	/// A table with a stored field the anonymous caller may not select, a
	/// computed field, and a plain indexed field. The one stored record reads
	/// as `{ id: t:1, n: 1, secret: 's' }` from storage.
	pub(crate) const RESTRICTED_TABLE: &str = "
		DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
		DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;
		DEFINE FIELD label ON t COMPUTED 'computed';
		DEFINE FIELD n ON t TYPE int;
		DEFINE INDEX n_idx ON t FIELDS n;
		CREATE t:1 SET n = 1, secret = 's';
	";

	/// An anonymous session on `test`/`test`: permission-checked when server
	/// auth is enabled.
	pub(crate) fn anonymous() -> Session {
		Session::default().with_ns("test").with_db("test")
	}

	/// Both contexts for `table`, resolved against `ctx`'s transaction.
	pub(crate) async fn contexts(
		ctx: &ExecutionContext,
		table: &str,
	) -> (ResolvedTableContext, ResolvedTableContext) {
		let table = TableName::from(table);
		let db_ctx = ctx.database().expect("a database context");
		let table_def = db_ctx
			.get_table_def(&table, None)
			.await
			.expect("catalog read")
			.expect("the table is defined");
		let select_permission = table_select_permission_physical_runtime(Some(&table_def), ctx)
			.await
			.expect("the permission compiles");
		let field_state = build_field_state(ctx, &table, true, None).await.expect("field state");
		let planned = ResolvedTableContext {
			table_def: Arc::clone(&table_def),
			enforcement: RowEnforcement::Planned {
				select_permission: select_permission.clone(),
				field_reads: DefinedFieldReads {
					computed: !field_state.computed_fields.is_empty(),
					restricted: !field_state.field_permissions.is_empty(),
				},
			},
		};
		let runtime = ResolvedTableContext {
			table_def,
			enforcement: RowEnforcement::Runtime {
				select_permission,
				field_state: Arc::new(field_state),
			},
		};
		(planned, runtime)
	}

	/// The index `name` on `table`, as the access-path chooser hands it to a
	/// scan.
	pub(crate) async fn index_ref(ctx: &ExecutionContext, table: &str, name: &str) -> IndexRef {
		let indexes = ctx
			.database()
			.expect("a database context")
			.get_table_indexes(&TableName::from(table), None)
			.await
			.expect("catalog read");
		let idx =
			indexes.iter().position(|ix| ix.name.as_str() == name).expect("the index is defined");
		IndexRef::new(&indexes, idx)
	}

	/// A field of an object row, `NONE` when absent.
	pub(crate) fn field(row: &Value, name: &str) -> Value {
		match row {
			Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
			_ => Value::None,
		}
	}

	/// Assert `rows` is the one stored record of [`RESTRICTED_TABLE`], as
	/// stored: the restricted field present, the computed field absent.
	pub(crate) fn assert_as_stored(rows: &[Value]) {
		assert_eq!(rows.len(), 1, "one stored record, got {rows:?}");
		assert_eq!(field(&rows[0], "secret"), Value::from("s"), "restricted field kept: {rows:?}");
		assert_eq!(field(&rows[0], "label"), Value::None, "computed field not derived: {rows:?}");
	}

	/// Assert `rows` is the one stored record of [`RESTRICTED_TABLE`], reduced
	/// for a checked caller: the restricted field cut, the computed field
	/// derived.
	pub(crate) fn assert_reduced(rows: &[Value]) {
		assert_eq!(rows.len(), 1, "one stored record, got {rows:?}");
		assert_eq!(field(&rows[0], "secret"), Value::None, "restricted field cut: {rows:?}");
		assert_eq!(
			field(&rows[0], "label"),
			Value::from("computed"),
			"computed field derived: {rows:?}"
		);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::TestDb;
	use crate::expr::logical::SourceRef;
	use crate::kvs::TransactionType;

	fn source(schema: Option<Arc<Schema>>) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table("t".into()),
			schema,
			with: None,
			for_update: false,
		})
	}

	#[test]
	fn a_resolved_source_is_planned_and_an_unresolved_one_is_runtime() {
		let schema = Arc::new(Schema::open());
		assert_eq!(EnforcementMode::for_source(Some(&schema)), EnforcementMode::Planned);
		assert_eq!(EnforcementMode::for_source(None), EnforcementMode::Runtime);

		assert_eq!(
			EnforcementMode::of_sources(&[source(Some(Arc::clone(&schema)))]),
			EnforcementMode::Planned
		);
		assert_eq!(EnforcementMode::of_sources(&[source(None)]), EnforcementMode::Runtime);
		// Several sources are combined above the scans; each is planned on its own.
		assert_eq!(
			EnforcementMode::of_sources(&[source(Some(Arc::clone(&schema))), source(Some(schema))]),
			EnforcementMode::Runtime
		);
		assert_eq!(EnforcementMode::of_sources(&[]), EnforcementMode::Runtime);
	}

	#[tokio::test]
	async fn a_planned_context_hands_the_scan_nothing_to_enforce() {
		let db = TestDb::new_with_auth(test_support::RESTRICTED_TABLE).await;
		let ctx = db.exec_ctx_as(&test_support::anonymous(), TransactionType::Read).await;
		let (planned, runtime) = test_support::contexts(&ctx, "t").await;

		assert!(matches!(planned.select_permission(true), PhysicalPermission::Allow));
		assert!(planned.field_state_for_projection(None).computed_fields.is_empty());
		assert!(planned.field_state_for_projection(None).field_permissions.is_empty());
		assert!(planned.field_state().computed_fields.is_empty());

		assert_eq!(runtime.field_state_for_projection(None).computed_fields.len(), 1);
		assert_eq!(runtime.field_state_for_projection(None).field_permissions.len(), 1);
		assert_eq!(runtime.field_state().computed_fields.len(), 1);
	}

	/// The field reads a planned context records come from the table's field
	/// definitions, and close the projection gate exactly when the table
	/// defines a computed field or, for a permission-checked caller, a
	/// restricted one.
	#[tokio::test]
	async fn a_planned_context_records_what_the_field_definitions_read() {
		use crate::catalog::providers::TableProvider;

		let db = TestDb::new(
			"DEFINE TABLE plain SCHEMALESS;
			 DEFINE FIELD n ON plain TYPE int;
			 DEFINE TABLE computed SCHEMALESS;
			 DEFINE FIELD c ON computed COMPUTED 1;
			 DEFINE TABLE restricted SCHEMALESS;
			 DEFINE FIELD secret ON restricted TYPE string PERMISSIONS FOR select NONE;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let db_ctx = ctx.database().expect("a database context");
		let reads = |table: &'static str| {
			let db_ctx = db_ctx.clone();
			let ctx = ctx.clone();
			async move {
				let table_def = db_ctx
					.get_table_def(&TableName::from(table), None)
					.await
					.expect("catalog read")
					.expect("the table is defined");
				let fields = ctx
					.txn()
					.all_tb_fields(
						table_def.namespace_id,
						table_def.database_id,
						&TableName::from(table),
						None,
					)
					.await
					.expect("field definitions");
				let field_reads = DefinedFieldReads::of(&fields);
				let planned = ResolvedTableContext {
					table_def,
					enforcement: RowEnforcement::Planned {
						select_permission: PhysicalPermission::Allow,
						field_reads,
					},
				};
				(field_reads, planned)
			}
		};
		let allow = PhysicalPermission::Allow;

		let (plain, ctx_plain) = reads("plain").await;
		assert!(!plain.computed && !plain.restricted);
		assert!(ctx_plain.projected_decode_applies(&allow, true));

		let (computed, ctx_computed) = reads("computed").await;
		assert!(computed.computed && !computed.restricted);
		assert!(!ctx_computed.projected_decode_applies(&allow, false));

		let (restricted, ctx_restricted) = reads("restricted").await;
		assert!(!restricted.computed && restricted.restricted);
		assert!(ctx_restricted.projected_decode_applies(&allow, false));
		assert!(!ctx_restricted.projected_decode_applies(&allow, true));
	}

	#[tokio::test]
	async fn the_table_permission_is_kept_for_candidates_and_for_none() {
		let db = TestDb::new("DEFINE TABLE t;").await;
		let ctx = db.exec_ctx().await;
		let conditional = PhysicalPermission::Conditional(
			crate::exec::operators::test_util::physical_expr("flag = true", &ctx).await,
		);
		let table_def = ctx
			.database()
			.expect("a database context")
			.get_table_def(&TableName::from("t"), None)
			.await
			.expect("catalog read")
			.expect("the table is defined");
		let planned = |select_permission: PhysicalPermission| ResolvedTableContext {
			table_def: Arc::clone(&table_def),
			enforcement: RowEnforcement::Planned {
				select_permission,
				field_reads: DefinedFieldReads::default(),
			},
		};

		// A conditional permission is the filter's above the scan, not the scan's.
		let ctx_cond = planned(conditional.clone());
		assert!(matches!(ctx_cond.select_permission(true), PhysicalPermission::Allow));
		// A top-k scan applies it to its candidates regardless.
		assert!(matches!(ctx_cond.candidate_permission(true), PhysicalPermission::Conditional(_)));
		// Neither applies when the session is not checked.
		assert!(matches!(ctx_cond.candidate_permission(false), PhysicalPermission::Allow));
		assert!(matches!(ctx_cond.select_permission(false), PhysicalPermission::Allow));

		// `NONE` is the plan's to apply too: the filter above a planned scan
		// is not always the `SELECT` clause, so the scan admits every row and
		// carries the permission only for a top-k scan's candidates.
		let ctx_none = planned(PhysicalPermission::Deny);
		assert!(matches!(ctx_none.select_permission(true), PhysicalPermission::Allow));
		assert!(matches!(ctx_none.candidate_permission(true), PhysicalPermission::Deny));
	}

	/// A planned context does not compile a conditional table guard — the
	/// plan's filter judges it — until a top-k scan asks for its candidates'
	/// guard; a `NONE` permission needs no compilation to answer either way.
	#[tokio::test]
	async fn a_planned_context_compiles_the_guard_only_for_candidates() {
		let db = TestDb::new(
			"DEFINE TABLE guarded SCHEMALESS PERMISSIONS FOR select WHERE flag = true;
			 DEFINE TABLE hidden SCHEMALESS PERMISSIONS FOR select NONE;
			 DEFINE TABLE open SCHEMALESS PERMISSIONS FULL;",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let planner = Planner::with_txn(
			ctx.ctx(),
			ctx.function_registry(),
			ctx.try_txn().expect("a transaction"),
			Some("test".to_owned()),
			Some("test".to_owned()),
		);
		let db_ctx = ctx.database().expect("a database context");
		let (ns_id, db_id) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
		let resolve = |table: &'static str, mode| {
			let planner = &planner;
			async move {
				resolve_table_context(
					planner,
					"test",
					"test",
					ns_id,
					db_id,
					&TableName::from(table),
					mode,
					None,
				)
				.await
				.expect("catalog read")
				.expect("the table is defined")
			}
		};

		let guarded = resolve("guarded", EnforcementMode::Planned).await;
		assert!(matches!(guarded.select_permission(true), PhysicalPermission::Allow));
		assert!(matches!(guarded.candidate_permission(true), PhysicalPermission::Allow));
		let guarded = guarded.with_candidate_guard(&planner).await.expect("the guard compiles");
		assert!(matches!(guarded.select_permission(true), PhysicalPermission::Allow));
		assert!(matches!(guarded.candidate_permission(true), PhysicalPermission::Conditional(_)));

		let hidden = resolve("hidden", EnforcementMode::Planned).await;
		assert!(matches!(hidden.select_permission(true), PhysicalPermission::Allow));
		let hidden = hidden.with_candidate_guard(&planner).await.expect("nothing to compile");
		assert!(matches!(hidden.candidate_permission(true), PhysicalPermission::Deny));

		let open = resolve("open", EnforcementMode::Planned).await;
		let open = open.with_candidate_guard(&planner).await.expect("nothing to compile");
		assert!(matches!(open.candidate_permission(true), PhysicalPermission::Allow));

		// Under runtime enforcement the scan applies the guard itself, so it
		// is compiled up front.
		let runtime = resolve("guarded", EnforcementMode::Runtime).await;
		assert!(matches!(runtime.select_permission(true), PhysicalPermission::Conditional(_)));
	}
}
