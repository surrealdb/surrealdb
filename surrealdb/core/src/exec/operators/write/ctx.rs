//! What a write resolves once per statement per table, and every record's
//! stages then share.
//!
//! Resolution reads the catalog and compiles physical expressions, so it must
//! not happen per record. A statement that writes to more than one table — an
//! `INSERT` whose rows name their own — holds one of these per table.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::Mutex;

use super::clauses::WriteClauses;
use super::scope::WriteScope;
use crate::catalog::providers::TableProvider;
use crate::catalog::{
	DatabaseDefinition, Error as CatalogError, EventDefinition, FieldDefinition, IndexDefinition,
	NamespaceDefinition, PermissionKind, SubscriptionDefinition, TableDefinition,
};
use crate::doc::{FieldEvalOrder, NsDbCtx, NsDbTbMutCtx};
use crate::exec::ExecutionContext;
use crate::exec::permission::{PhysicalIndexEntry, PhysicalPermission, PhysicalViewKind};
use crate::exec::table_view::TableViews;
use crate::expr::computed_deps::ComputedDeps;
use crate::expr::output::Output;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::kvs::Transaction;
use crate::val::TableName;

/// A table's index definitions with their expressions compiled, parallel to
/// the definitions and in the same order; `None` at an index the catalog has
/// marked for removal, which is never maintained and so never compiled.
pub(crate) type CompiledIndexes = Arc<[Option<Arc<PhysicalIndexEntry>>]>;

/// Whether the statement's table must already exist.
///
/// `UPDATE` and `DELETE` require it, and name the missing table. The four
/// creating forms do not: the table is defined on demand, as writing the first
/// record of a schemaless table has always done.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableExistence {
	/// The table must exist; a missing one is the statement's failure.
	Required,
	/// A missing table is defined by this write.
	DefinedOnDemand,
}

/// Resolved once per statement per table, shared by every record.
#[derive(Debug)]
pub(crate) struct WriteCtx {
	/// The namespace the record belongs to. Held as the definition rather than
	/// the name because every storage key the stages write is keyed on its id.
	pub(crate) ns: Arc<NamespaceDefinition>,
	/// The database the record belongs to, for the same reason.
	pub(crate) db: Arc<DatabaseDefinition>,
	/// The table being written.
	pub(crate) table: Arc<TableDefinition>,
	/// The table's field definitions, in the order the catalog holds them.
	pub(crate) fields: Arc<[FieldDefinition]>,
	/// The event bodies this write triggers.
	pub(crate) events: Arc<[EventDefinition]>,
	/// The foreign materialised views this write maintains.
	pub(crate) views: Arc<[TableDefinition]>,
	/// The indexes this write maintains.
	pub(crate) indexes: Arc<[IndexDefinition]>,
	/// The live-query subscriptions this write notifies.
	pub(crate) lives: Arc<[SubscriptionDefinition]>,
	/// The VM's units of `lives`, one for each.
	pub(crate) live_units: crate::exec::vm::live::LiveUnitList,

	/// The order the field clauses are applied in, so a clause that reads
	/// another field of the same record sees the written value rather than the
	/// prior one. Carries the cycle a table's clauses form, when they form one;
	/// the record is rejected rather than the table.
	pub(crate) field_order: Arc<FieldEvalOrder>,
	/// Which same-table fields each computed field reads, keyed by field name.
	pub(crate) computed_deps: HashMap<String, ComputedDeps>,
	/// Index of the `id` field in `fields`, so id coercion reads its kind and
	/// default without rescanning the list per record.
	pub(crate) id_field: Option<usize>,

	/// The context the stages evaluate against, and the only handle to the
	/// transaction. No operator begins, commits or cancels one; a write
	/// obtains it from here and from nowhere else.
	pub(crate) exec: ExecutionContext,

	/// The statement's `RETURN` clause, dispatched by `output_write`.
	pub(crate) output: Option<Output>,

	/// The statement's own compiled clauses, shared by every record and by
	/// every table this statement writes to. A stage is addressed by a `Copy`
	/// id that can carry no expressions, so this is how a stage reaches the
	/// `WHERE`, the data clause, or `ON DUPLICATE KEY UPDATE`.
	pub(crate) clauses: Arc<WriteClauses>,

	/// The post-image projection the caller is shown: the table's
	/// `PERMISSIONS FOR select`, its per-field select guards, and its
	/// `COMPUTED` bodies.
	///
	/// Behind a mutex because a resolver memoises what it has resolved and
	/// [`WriteStage::apply`](super::stage::WriteStage::apply) takes `&WriteCtx`.
	/// It is never contended: at most one record is in flight per transaction.
	pub(crate) view_checked: Mutex<TableViews>,
	/// The same machinery with checks bypassed, for the images a stage derives
	/// rather than shows: the pre-mutation snapshot the data clause reads, and
	/// the scratch copy a permission predicate is judged against.
	///
	/// A separate resolver because whether a view's reads are checked is fixed
	/// for a resolver's lifetime — it decides whether the views it builds carry
	/// any field guards at all — so one resolver cannot answer both. It is
	/// built by [`TableViews::unchecked`], which fixes the bypass up front:
	/// a plain `TableViews::new()` here would resolve `check_perms` from
	/// whatever context the first stage happened to hand it, and the two fields
	/// would name a distinction nothing enforced.
	///
	/// Neither resolver can serve the per-subscriber live-query reduction,
	/// which needs one view per subscription auth; that stage owns its own.
	pub(crate) view_unchecked: Mutex<TableViews>,

	/// The foreign views this table maintains, with their clauses compiled —
	/// on first use, and once for the statement.
	///
	/// Behind a mutex for the reason the view resolvers above are, and keyed
	/// by the view table because one source table may feed several views.
	/// `views` holds the definitions; this holds what the write evaluates.
	pub(crate) views_compiled: Mutex<Vec<(TableName, Arc<PhysicalViewKind>)>>,

	/// The table's write `PERMISSIONS` clauses, compiled — on first use, and
	/// once for the statement.
	///
	/// Keyed by the clause, of which there are at most three: `create`,
	/// `update` and `delete`. A clause is a function of the table definition
	/// and not of the record, so compiling it per record would plan the same
	/// expression once for every row a statement writes. The `select` clause
	/// is not here — [`Self::view_checked`] holds its compiled form, so the
	/// gate on the projected image and the projection itself cannot disagree.
	pub(crate) permissions_compiled: Mutex<Vec<(PermissionKind, Arc<PhysicalPermission>)>>,

	/// The table's index definitions, with their columns and `COUNT` guards
	/// compiled — on first use, and once for the statement.
	///
	/// Parallel to [`Self::indexes`] and in the same order, so an index's
	/// entries are read positionally rather than searched for. `None` at a
	/// position the catalog has marked for removal: such an index is never
	/// maintained, and compiling its clauses could fail a write over an
	/// expression it would not have run.
	///
	/// An index's expressions are a function of its definition and not of the
	/// record, and every index the table carries is visited for every record
	/// the stage processes, so the whole set is compiled in one pass rather
	/// than one index at a time. Behind a mutex for the reason the view
	/// resolvers above are.
	pub(crate) indexes_compiled: Mutex<Option<CompiledIndexes>>,

	/// The statement scope that resolved this context, which is how a stage
	/// reaches what one statement owns across tables and nesting levels — the
	/// other tables' contexts, and the compiled plans of the statements the
	/// write performs on its own behalf.
	///
	/// Weak because the scope owns the contexts. Set by
	/// [`WriteScope::write_ctx_for`], the only production construction site;
	/// [`Self::resolve`] leaves it empty, so a context a test builds directly
	/// has no scope and a stage that needs one says so rather than appearing
	/// to work.
	pub(crate) scope: Weak<WriteScope>,
	/// The table's catalog version when the context was resolved, and how
	/// many catalog stamps the transaction had made then: a context resolved
	/// before a change to its table's catalog no longer stands
	/// ([`Self::stands`]).
	pub(crate) resolved_at: (Option<uuid::Uuid>, u64),
}

impl WriteCtx {
	/// Resolve the write context for one table.
	///
	/// Reads the per-table definitions a write needs: fields, events, foreign
	/// views, indexes and subscriptions, plus the field evaluation order
	/// derived from them.
	///
	/// It does **not** apply the statement-level table permission gate, which
	/// drops a target silently when the caller holds no permission on it —
	/// before any record is built, and therefore before any
	/// stage list runs. Nothing here covers it and no `StageId` can, because it
	/// is not a per-record step: it is owed by whatever builds the source, and
	/// is owed before a write operator is first planned.
	pub(crate) async fn resolve(
		exec: &ExecutionContext,
		table: &TableName,
		existence: TableExistence,
		output: Option<Output>,
		clauses: Arc<WriteClauses>,
	) -> Result<Self, ControlFlow> {
		let db_ctx = exec.database().context("a write requires a database context")?;
		let parent = NsDbCtx {
			ns: Arc::clone(&db_ctx.ns_ctx.ns),
			db: Arc::clone(&db_ctx.db),
		};
		let version = exec.version_stamp();
		let tb = match existence {
			TableExistence::Required => {
				db_ctx.get_table_def(table, version).await.map_err(ControlFlow::Err)?.ok_or_else(
					|| {
						ControlFlow::Err(anyhow::anyhow!(CatalogError::TbNotFound {
							name: table.to_owned(),
						}))
					},
				)?
			}
			TableExistence::DefinedOnDemand => exec
				.txn()
				.get_or_add_tb(Some(exec.ctx()), &parent.ns.name, &parent.db.name, table, version)
				.await
				.map_err(ControlFlow::Err)?,
		};
		let txn = exec.txn();
		let stamps = txn.catalog_stamps();
		let catalog_version = txn
			.catalog_version(parent.ns.namespace_id, parent.db.database_id, table)
			.await
			.map_err(ControlFlow::Err)?;
		let catalog = NsDbTbMutCtx::load(exec.ctx(), &parent, tb, table, version)
			.await
			.map_err(ControlFlow::Err)?;
		Ok(Self {
			ns: catalog.ns,
			db: catalog.db,
			table: catalog.tb,
			fields: catalog.fields,
			events: catalog.events,
			views: catalog.tables,
			indexes: catalog.indexes,
			lives: catalog.lives,
			live_units: catalog.live_units,
			field_order: catalog.field_order,
			computed_deps: catalog.computed_deps,
			id_field: catalog.id_field_idx,
			exec: exec.clone(),
			output,
			clauses,
			view_checked: Mutex::new(TableViews::new()),
			view_unchecked: Mutex::new(TableViews::unchecked()),
			views_compiled: Mutex::new(Vec::new()),
			indexes_compiled: Mutex::new(None),
			permissions_compiled: Mutex::new(Vec::new()),
			scope: Weak::new(),
			resolved_at: (catalog_version, stamps),
		})
	}

	/// This context's catalog reads, with `exec`, `output` and `clauses` in
	/// place of its own and nothing compiled yet.
	///
	/// Sound only while the catalog this context read is the one `exec`
	/// reads: the caller guarantees that nothing has changed it since, and
	/// that `exec` reads it at the same version, namespace and database.
	pub(crate) fn rebound(
		&self,
		exec: &ExecutionContext,
		output: Option<Output>,
		clauses: Arc<WriteClauses>,
	) -> Self {
		Self {
			ns: Arc::clone(&self.ns),
			db: Arc::clone(&self.db),
			table: Arc::clone(&self.table),
			fields: Arc::clone(&self.fields),
			events: Arc::clone(&self.events),
			views: Arc::clone(&self.views),
			indexes: Arc::clone(&self.indexes),
			lives: Arc::clone(&self.lives),
			live_units: Arc::clone(&self.live_units),
			field_order: Arc::clone(&self.field_order),
			computed_deps: self.computed_deps.clone(),
			id_field: self.id_field,
			exec: exec.clone(),
			output,
			clauses,
			view_checked: Mutex::new(TableViews::new()),
			view_unchecked: Mutex::new(TableViews::unchecked()),
			views_compiled: Mutex::new(Vec::new()),
			indexes_compiled: Mutex::new(None),
			permissions_compiled: Mutex::new(Vec::new()),
			scope: Weak::new(),
			resolved_at: self.resolved_at,
		}
	}

	/// Whether the table's catalog is the one this context read: the
	/// transaction has stamped no catalog version since, or none of this
	/// table's. The version is read through the transaction's cache, a seam
	/// of the run-time seal ([`crate::exec::compiles::seam`]).
	pub(crate) async fn stands(&self) -> Result<bool, ControlFlow> {
		let txn = self.exec.txn();
		let (version, stamps) = self.resolved_at;
		if txn.catalog_stamps() == stamps {
			return Ok(true);
		}
		let now = crate::exec::vm::descriptor::object(txn.catalog_version(
			self.ns.namespace_id,
			self.db.database_id,
			&self.table.name,
		))
		.await
		.map_err(ControlFlow::Err)?;
		Ok(now == version)
	}

	/// The transaction the stages write through.
	///
	/// Read from the execution context on every call rather than cached beside
	/// it, so there is one handle and no way for the two to disagree.
	pub(crate) fn txn(&self) -> Arc<Transaction> {
		self.exec.txn()
	}

	/// The context the stages evaluate expressions against.
	pub(crate) fn exec(&self) -> &ExecutionContext {
		&self.exec
	}

	/// The statement scope this context was resolved by.
	///
	/// Infallible while a stage runs: the operator's stream holds the scope for
	/// its whole life, and every context a record's stages are handed came from
	/// it. An empty one therefore means a context built outside a scope — a
	/// test harness — has reached a stage that needs one, which is a broken
	/// invariant rather than a condition to recover from.
	pub(crate) fn scope(&self) -> Result<Arc<WriteScope>, ControlFlow> {
		self.scope.upgrade().ok_or_else(|| {
			ControlFlow::Err(anyhow::anyhow!(crate::err::Error::unreachable(
				"a write stage asked for its statement scope, which is not set"
			)))
		})
	}
}
