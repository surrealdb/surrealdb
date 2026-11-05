//! Descriptors: what a running program learns of the catalog, loaded when it
//! needs it.
//!
//! A program holds no catalog assumption. What its plan saw of the catalog
//! shaped the program, and nothing more; every catalog fact the run acts on —
//! a table's existence and definition, its fields, its indexes, the field
//! state its reads apply, its select permission, the ids of the namespace and
//! database, a parameter's or a function's definition, another catalog
//! object a builtin names — is loaded through this module when the run needs
//! it, whether the plan named the object or only data does.
//!
//! This module is the only code that reads the catalog while a program runs
//! ([`crate::exec::compiles`]): each load is a seam of the run-time seal. A
//! load reads through the transaction's cache, so it sees every catalog
//! write the transaction has made, its own statements' included, and a load
//! made after an instruction that may write the catalog is current without
//! being told.
//!
//! A table's descriptor ([`TableDescriptor`]) is read afresh per load, from
//! the transaction cache, so a step that loads it where it needs it —
//! when its pipeline opens a source, when a write resolves its table — sees
//! the catalog as the statements before it left it, a schema statement's
//! writes and those of a call whose effects are unknown included. What is
//! compiled from a table's definitions — the field state of its reads and its
//! select permission — is part of the table's read and is served by the
//! compile cache ([`super::compile_cache`]) per (table, catalog version,
//! build) and frame: the class of the principal, whether its permissions are
//! suppressed and whether field permissions apply ([`from_cache`]). It is
//! compiled over the transaction, so the statements its permissions and
//! computed fields hold are planned with it, and checked against what else
//! its compilation read when it is served. A table whose catalog was never
//! stamped with a version, and a read at a `VERSION` stamp, name no version
//! to key by: theirs is compiled with no transaction and kept for the
//! statement.
//!
//! A schema statement reads the catalog it writes, as its data
//! ([`schema`]).

use std::sync::Arc;

use anyhow::Context as _;
use surrealdb_strand::TableName;

use super::compile_cache::{BuildId, ObjectVersion, UnitKey, UnitObject};
use super::program::UnitFrame;
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{
	DatabaseDefinition, DatabaseId, FieldDefinition, IndexDefinition, NamespaceDefinition,
	NamespaceId, TableDefinition, TableLayouts,
};
use crate::exec::operators::scan::pipeline::FieldState;
use crate::exec::permission::PhysicalPermission;
use crate::exec::{DatabaseContext, ExecutionContext};
use crate::expr::{ControlFlow, FlowResult};
use crate::kvs::RowGate;

/// A table's catalog as a load found it; see the module.
pub(crate) struct TableDescriptor {
	pub(crate) name: TableName,
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	/// The `VERSION` stamp the load read at; `None` for the current state.
	pub(crate) stamp: Option<u64>,
	/// `None` where the catalog holds no such table.
	pub(crate) definition: Option<Arc<TableDefinition>>,
	/// The table's field definitions, including the virtual definitions of
	/// the components its record id names.
	pub(crate) fields: Arc<[FieldDefinition]>,
	/// The generations the table's rows are stored under, as of `stamp`, for
	/// the table whether or not it is defined: rows remain under a removed
	/// table's root until a range delete reaches them. A scan checks each row
	/// it reads against these before it reads any field of the row
	/// ([`Self::row_gate`]).
	pub(crate) layouts: Arc<TableLayouts>,
	/// The transaction's layout epoch ([`crate::kvs::Transaction::layout_epoch`])
	/// read before `layouts` were loaded. A gate made from the descriptor holds
	/// it, so a current gate takes the transaction's layouts again once the
	/// epoch has moved since the load ([`RowGate::refresh`]), whether or not the
	/// descriptor itself is served again.
	pub(crate) layouts_epoch: u64,
}

impl TableDescriptor {
	/// The check of one reader of this table's rows at the descriptor's
	/// `VERSION` stamp. It refuses layouts that belong to another table before
	/// the first row is read.
	pub(crate) fn row_gate(&self) -> anyhow::Result<RowGate> {
		RowGate::new(
			Arc::clone(&self.layouts),
			self.ns,
			self.db,
			&self.name,
			self.stamp,
			self.layouts_epoch,
		)
	}

	/// The migration steps of the table's rows, when its ring carries any.
	pub(crate) fn replay(&self) -> Option<crate::exec::migrate::Replay> {
		crate::exec::migrate::Replay::of(&self.layouts, self.ns, self.db)
	}

	/// The definition, or the error of a table that is not defined.
	pub(crate) fn defined(&self) -> anyhow::Result<&Arc<TableDefinition>> {
		self.definition.as_ref().ok_or_else(|| {
			anyhow::Error::new(crate::catalog::Error::TbNotFound {
				name: self.name.clone(),
			})
		})
	}
}

/// The descriptor of `table` in the database `ctx` runs in, read at the
/// `VERSION` stamp `ctx` reads at.
pub(crate) fn table<'a>(
	ctx: &'a ExecutionContext,
	table: &'a TableName,
) -> impl std::future::Future<Output = anyhow::Result<Arc<TableDescriptor>>> + Send + 'a {
	table_at(ctx, table, ctx.version_stamp())
}

/// The descriptor of `table` in the database `ctx` runs in, as of `stamp`.
pub(crate) fn table_at<'a>(
	ctx: &'a ExecutionContext,
	table: &'a TableName,
	stamp: Option<u64>,
) -> impl std::future::Future<Output = anyhow::Result<Arc<TableDescriptor>>> + Send + 'a {
	crate::exec::compiles::seam(async move { load_table(ctx.database()?, table, stamp).await })
}

/// The descriptors a statement's run loads of the same tables again and
/// again, kept per distinct table.
///
/// Descriptors are read through the seam ([`table_at`]), and one is served
/// again only while the transaction is the same and has stamped no table
/// catalog version since the read, so a catalog write the transaction makes
/// between two loads is seen by the second. A speculating transaction is
/// never served from the memo and nothing is stored from it. It covers
/// current-state reads only (no `VERSION` stamp). It holds at most one
/// descriptor per distinct table.
#[derive(Default)]
pub(crate) struct TableMemo {
	/// The address of the transaction `entries` were read in; `0` for none.
	txn: usize,
	/// The transaction's `catalog_stamps` when `entries` were read.
	stamps: u64,
	entries: std::collections::HashMap<TableName, Arc<TableDescriptor>>,
}

impl TableMemo {
	/// The current descriptor of `table` in the database `ctx` runs in.
	pub(crate) async fn current(
		&mut self,
		ctx: &ExecutionContext,
		table: &TableName,
	) -> anyhow::Result<Arc<TableDescriptor>> {
		let txn = ctx.txn();
		if txn.speculating() {
			return table_at(ctx, table, None).await;
		}
		let (at, stamps) = (Arc::as_ptr(&txn) as usize, txn.catalog_stamps());
		if self.txn != at || self.stamps != stamps {
			self.entries.clear();
			self.txn = at;
			self.stamps = stamps;
		}
		if let Some(known) = self.entries.get(table) {
			return Ok(Arc::clone(known));
		}
		let loaded = table_at(ctx, table, None).await?;
		self.entries.insert(table.clone(), Arc::clone(&loaded));
		Ok(loaded)
	}

	/// Let go of every descriptor held.
	pub(crate) fn clear(&mut self) {
		self.txn = 0;
		self.stamps = 0;
		self.entries.clear();
	}
}

async fn load_table(
	db_ctx: &DatabaseContext,
	table: &TableName,
	stamp: Option<u64>,
) -> anyhow::Result<Arc<TableDescriptor>> {
	let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let txn = db_ctx.txn();
	let definition = txn.get_tb(ns, db, table, stamp).await?;
	let fields = match &definition {
		Some(_) => txn.all_tb_fields(ns, db, table, stamp).await?,
		None => Arc::from([]),
	};
	// Only a storage error fails the load. A generation this build cannot read
	// is reported as a refused generation in `layouts`.
	let layouts_epoch = txn.layout_epoch();
	let layouts = txn.table_layouts(ns, db, table, stamp).await?;
	Ok(Arc::new(TableDescriptor {
		name: table.clone(),
		ns,
		db,
		stamp,
		definition,
		fields,
		layouts,
		layouts_epoch,
	}))
}

/// The definition of `table` in the database of `db_ctx`, as of `stamp`;
/// `None` where it is not defined. This is the load
/// [`DatabaseContext::get_table_def`] makes on a miss in its own cache.
pub(crate) fn definition<'a>(
	db_ctx: &'a DatabaseContext,
	table: &'a TableName,
	stamp: Option<u64>,
) -> impl std::future::Future<Output = anyhow::Result<Option<Arc<TableDefinition>>>> + Send + 'a {
	crate::exec::compiles::seam(async move {
		let txn = db_ctx.txn();
		txn.get_tb_by_name(&db_ctx.ns_ctx.ns.name, &db_ctx.db.name, table, stamp).await
	})
}

/// The frame what is compiled from `table`'s definitions for `ctx` is kept
/// under in the compile cache: the class of the principal `ctx` runs under,
/// whether its permissions are suppressed, and whether field permissions
/// apply.
fn frame_of(db_ctx: &DatabaseContext, ctx: &ExecutionContext, checked: bool) -> UnitFrame {
	let root = ctx.root();
	UnitFrame {
		class_digest: UnitFrame::digest(
			ctx.ctx().auth_enabled(),
			Some(&root.auth),
			Some(db_ctx.ns_name()),
			Some(db_ctx.db_name()),
		),
		ns: Some(db_ctx.ns_name().to_owned()),
		db: Some(db_ctx.db_name().to_owned()),
		perms_suppressed: root.perms_suppressed,
		checked,
	}
}

/// The key `object`, compiled from `table`'s definitions, is kept under in
/// the compile cache, at the catalog version the transaction reads the table
/// at; `None` for a read at a `VERSION` stamp and for a table whose catalog
/// was never stamped, which name no version to key by.
async fn key_of(
	ctx: &ExecutionContext,
	db_ctx: &DatabaseContext,
	table: &TableName,
	object: impl FnOnce(TableName, Option<u64>) -> UnitObject,
) -> FlowResult<UnitKey> {
	let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let stamp = ctx.version_stamp();
	let stamped = match stamp {
		Some(_) => None,
		None => crate::exec::compiles::seam(ctx.txn().catalog_version(ns, db, table))
			.await
			.map_err(ControlFlow::Err)?,
	};
	// A read at a `VERSION` stamp, and a table no catalog write has stamped,
	// name no catalog version: they are keyed by the content of the
	// definitions the read finds, at its stamp.
	let version = match stamped {
		Some(version) => ObjectVersion::Stamped(version),
		None => {
			use std::hash::{Hash, Hasher};

			use surrealdb_types::ToSql;
			let descriptor = self::table(ctx, table).await.map_err(ControlFlow::Err)?;
			let mut hasher = std::hash::DefaultHasher::new();
			descriptor.definition.as_deref().hash(&mut hasher);
			for field in descriptor.fields.iter() {
				field.to_sql().hash(&mut hasher);
			}
			ObjectVersion::Content(hasher.finish())
		}
	};
	Ok(UnitKey {
		ns: db_ctx.ns_name().to_owned(),
		db: db_ctx.db_name().to_owned(),
		ids: (ns, db),
		object: object(table.clone(), stamp),
		version,
		build: BuildId::current(ctx.ctx().get_capabilities()),
	})
}

/// `compile`, what is compiled from `table`'s definitions for `ctx` and the
/// frame `checked` names, served by the compile cache under `object`: the
/// unit kept for the table's catalog version and the frame where what else
/// it depends on holds, and otherwise compiled on the miss, with a planner over the
/// transaction ([`unit_planner`]), and kept. A read at a `VERSION` stamp and a
/// table whose catalog was never stamped are compiled with no transaction, and
/// kept for the statement by `statement`.
///
/// The cache keeps only compilations the run requests. These are the only
/// compiles made after a query arrives ([`crate::exec::compiles`]).
async fn from_cache<'a, V: Clone + Send + Sync + 'static>(
	ctx: &'a ExecutionContext,
	table: &'a TableName,
	checked: bool,
	object: impl FnOnce(TableName, Option<u64>) -> UnitObject,
	cache: impl FnOnce(&super::units::CompiledUnits) -> &super::compile_cache::UnitCache<V>,
	compile: impl for<'p> FnOnce(
		&'p crate::exec::planner::Planner<'a>,
	) -> crate::exec::BoxFut<'p, FlowResult<V>>
	+ Send,
) -> FlowResult<Option<V>> {
	let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let Some(units) = ctx.ctx().compiled_units() else {
		// A context that keeps no units compiles for the read alone, over the
		// transaction all the same.
		return crate::exec::compiles::serving_fault(async {
			let planner = unit_planner(ctx, db_ctx);
			let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
			let _entered = planner.cycle_guard.try_enter(ns, db, table.clone());
			compile(&planner).await.map(Some)
		})
		.await;
	};
	let key = key_of(ctx, db_ctx, table, object).await?;
	let frame = frame_of(db_ctx, ctx, checked);
	let txn = ctx.txn();
	let served = crate::exec::compiles::seam(cache(units).get_or_compile(
		&key,
		&frame,
		&txn,
		ctx.ctx(),
		// A miss compiles under the serving-fault guard, as every compile-cache fill does.
		|| {
			crate::exec::compiles::serving_fault(async {
				let planner = unit_planner(ctx, db_ctx);
				let _entered = planner.cycle_guard.try_enter(key.ids.0, key.ids.1, table.clone());
				let recording = txn.record_catalog_reads();
				let compiled = compile(&planner).await;
				let reads = recording.finish();
				let value = compiled?;
				let deps = match crate::exec::catalog_deps::CatalogDeps::resolve(&txn, &reads).await
				{
					Some(deps) => super::units::ContextDeps::resolve(
						&txn,
						&reads,
						ctx.ctx(),
						Default::default(),
					)
					.await
					.map(|context| {
						(deps, context.without(crate::exec::planner::UNIT_RUNTIME_PARAMS))
					}),
					None => None,
				};
				Ok::<_, ControlFlow>((value, deps))
			})
		},
	))
	.await?;
	Ok(Some(served))
}

/// The planner what is compiled from a table's definitions for `ctx` is
/// compiled with: over the transaction `ctx` runs in, in its database, for
/// its principal and permission contract, at the depth of a statement, with
/// the session's own values read when the code runs. Its statements are
/// planned with it, so none is left to be planned where it is evaluated.
pub(crate) fn unit_planner<'a>(
	ctx: &'a ExecutionContext,
	db_ctx: &DatabaseContext,
) -> crate::exec::planner::Planner<'a> {
	// The definitions compiled here are a table's clauses, which run in a
	// privileged frame: they are planned against the session's own values and
	// the database's, never a caller's bindings.
	let frame = crate::ctx::Context::new_isolated(ctx.ctx()).freeze();
	crate::exec::planner::Planner::with_txn(
		&frame,
		ctx.function_registry(),
		ctx.txn(),
		Some(db_ctx.ns_name().to_owned()),
		Some(db_ctx.db_name().to_owned()),
	)
	.with_auth(Arc::clone(&ctx.root().auth))
	.with_perms_suppressed(ctx.root().perms_suppressed)
	.at_stamp(ctx.version_stamp())
	.unit_root()
}

/// The field state of reads of `table` in `ctx`, with field permissions
/// applied where `checked`: what [`crate::exec::operators::scan::pipeline::build_field_state`]
/// answers before it narrows it to a projection.
///
/// Part of the table's read, served by the compile cache per (table, catalog
/// version, build) and frame ([`from_cache`]), its permission and
/// computed-field statements planned with it; compiled once per statement,
/// with no transaction, for a table that names no version.
pub(crate) async fn field_state(
	ctx: &ExecutionContext,
	table: &TableName,
	checked: bool,
) -> FlowResult<Arc<FieldState>> {
	let cached = from_cache(
		ctx,
		table,
		checked,
		UnitObject::FieldState,
		super::units::CompiledUnits::field_states,
		|planner| {
			Box::pin(async move {
				let descriptor = self::table(ctx, table).await.map_err(ControlFlow::Err)?;
				crate::exec::operators::scan::pipeline::compile_field_state(
					planner,
					&descriptor.name,
					&descriptor.fields,
					checked,
				)
				.await
				.map(Arc::new)
			})
		},
	)
	.await?;
	cached.ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
			"a field state the compile cache answered nothing for".to_owned(),
		)))
	})
}

/// The `SELECT` permission of `table` in `ctx`, compiled: what
/// [`crate::exec::operators::fetch::table_select_permission`] answers. Served
/// as the field state is ([`field_state`]).
pub(crate) async fn select_permission(
	ctx: &ExecutionContext,
	table: &TableName,
) -> FlowResult<PhysicalPermission> {
	let cached = from_cache(
		ctx,
		table,
		true,
		UnitObject::SelectPermission,
		super::units::CompiledUnits::select_permissions,
		|planner| {
			Box::pin(async move {
				let descriptor = self::table(ctx, table).await.map_err(ControlFlow::Err)?;
				crate::exec::permission::table_select_permission_physical(
					descriptor.definition.as_deref(),
					planner,
				)
				.await
				.context("Failed to convert permission")
				.map_err(ControlFlow::Err)
			})
		},
	)
	.await?;
	cached.ok_or_else(|| {
		ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
			"a SELECT permission the compile cache answered nothing for".to_owned(),
		)))
	})
}

/// The index `name` of `descriptor`'s table, as of the stamp it was read at;
/// `None` where the table defines no such index.
pub(crate) async fn index(
	ctx: &ExecutionContext,
	descriptor: &TableDescriptor,
	name: &str,
) -> anyhow::Result<Option<Arc<IndexDefinition>>> {
	let txn = ctx.txn();
	crate::exec::compiles::seam(txn.get_tb_index(
		descriptor.ns,
		descriptor.db,
		&descriptor.name,
		name,
		descriptor.stamp,
	))
	.await
}

/// The namespace and database named `ns` and `db`, as of `stamp`; `None`
/// where either is not defined.
pub(crate) async fn database(
	ctx: &crate::ctx::FrozenContext,
	ns: &str,
	db: &str,
	stamp: Option<u64>,
) -> anyhow::Result<Option<(Arc<NamespaceDefinition>, Arc<DatabaseDefinition>)>> {
	let txn = ctx.tx();
	crate::exec::compiles::seam(async {
		let Some(db_def) = txn.get_db_by_name(ns, db, stamp).await? else {
			return Ok(None);
		};
		let Some(ns_def) = txn.get_ns_by_name(ns, stamp).await? else {
			return Ok(None);
		};
		Ok(Some((ns_def, db_def)))
	})
	.await
}

/// `read`, a load of a catalog object other than a table — a parameter, a
/// function, an analyzer, a sequence, a bucket, a model, a module, an API, a
/// config, the ids of a namespace and database — that the run, a builtin it
/// calls, or a write it makes names. Each is served as a table's descriptor is,
/// by (object, version).
pub(crate) fn object<'a, F: std::future::Future + Send + 'a>(
	read: F,
) -> impl std::future::Future<Output = F::Output> + Send + 'a {
	crate::exec::compiles::seam(read)
}

/// `work`, the resolution of a write's context for one table: its
/// definition, fields, events, views, indexes and live subscriptions, and
/// what is compiled from them. The table's write unit, to be served from the
/// compile cache by (table, version, build).
pub(crate) fn write_unit<'a, F: std::future::Future + Send + 'a>(
	work: F,
) -> impl std::future::Future<Output = F::Output> + Send + 'a {
	crate::exec::compiles::seam(work)
}

/// `work`, a schema statement's step, which reads the catalog it writes as
/// its data: the definitions it checks the new one against, and the ids it
/// allocates.
pub(crate) fn schema<'a, F: std::future::Future + Send + 'a>(
	work: F,
) -> impl std::future::Future<Output = F::Output> + Send + 'a {
	crate::exec::compiles::seam(work)
}
