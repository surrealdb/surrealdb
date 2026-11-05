//! The node's compiled live-subscription units.
//!
//! A live subscription's work on a record — the table's permission and field
//! view for the subscriber, its `WHERE`, its projection — is a unit of
//! bytecode of its own ([`LiveUnit`]), compiled for the subscriber's
//! principal against the catalog of the table it watches
//! ([`compile`]), and held by the node keyed by the subscription's
//! id and the table's catalog version. A held unit is reused only while every
//! other catalog entry its compilation read — a function its clauses call, a
//! parameter they read — is as it was read ([`CatalogDeps::hold`]); a unit
//! whose reads cannot be named is not held. A write's context carries the units of
//! its table's subscriptions at the version it read, and its instructions only
//! look them up. A unit is compiled as a bare `LIVE` statement arrives, in a
//! read transaction before the statement's write transaction opens, and is held
//! when the subscription registers if the table's catalog version is unchanged
//! ([`arrive`]); a `LIVE` inside a transaction block or a
//! caller-owned transaction compiles nothing as it registers. A unit is also
//! compiled when a write to its table is planned, and, for a subscription the
//! node holds no unit for at that version, in a fault as the write's context is
//! resolved ([`ensure`]).
//!
//! A subscription served nothing has no unit: its session has expired, it
//! holds no principal, or its stored text does not compile. A subscription
//! whose clauses do not lower at arrival is refused as it registers; one that stops
//! lowering after it registered — a definition it reads changed — has no
//! unit either. That refusal is logged as a warning and held like a unit,
//! under the same key and the same catalog reads, so a write to its table
//! does not compile it again until what it read changes. A subscription with
//! no unit is not notified.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use surrealdb_strand::TableName;
use uuid::Uuid;

use super::program::Program;
use super::write::LoweredSubscription;
use super::{Lowered, NotCovered, NotCoveredKind, PlanSession};
use crate::catalog::{DatabaseId, NamespaceId, SubscriptionDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exec::catalog_deps::CatalogDeps;
use crate::exec::ir::SubscriptionRequest;
use crate::exec::ir::unit::SUBSCRIBER_NAMES;
use crate::exec::operators::write::stages::lives::{Subscriber, subscriber};
use crate::exec::planner::Planner;
use crate::kvs::Transaction;

/// A live subscription's work compiled for the VM, for the subscription's
/// principal.
#[derive(Debug)]
pub(crate) struct LiveUnit {
	pub(in crate::exec::vm) program: Arc<Program>,
	pub(in crate::exec::vm) plan: LoweredSubscription,
}

/// The units of a table's subscriptions, one for each in the order the table
/// lists them: `None` for a subscription with no unit.
pub(crate) type LiveUnitList = Arc<[Option<Arc<LiveUnit>>]>;

/// How many units a node holds before it forgets them all, to be compiled
/// again as their tables are next loaded.
const CAPACITY: usize = 4096;

/// A node's compiled units, each under the subscription id and the catalog
/// version of its table it was compiled against, with what else its
/// compilation read of the catalog: `None` for a subscription whose clauses
/// did not lower.
type Units = HashMap<(Uuid, Option<Uuid>), (Option<Arc<LiveUnit>>, Arc<CatalogDeps>)>;

/// The units this node has compiled, by subscription id and the catalog
/// version of its table each was compiled against.
#[derive(Default)]
pub(crate) struct LiveUnits {
	units: Mutex<Units>,
}

impl std::fmt::Debug for LiveUnits {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("LiveUnits").finish_non_exhaustive()
	}
}

impl LiveUnits {
	/// What is held under `key` — the unit, or `None` for a subscription whose
	/// clauses did not lower — where every catalog entry its compilation read
	/// is, as `txn` reads it, in the state it was compiled against; `None`
	/// where nothing is held.
	pub(super) async fn get(
		&self,
		key: &(Uuid, Option<Uuid>),
		txn: &Transaction,
	) -> Option<Option<Arc<LiveUnit>>> {
		let (unit, deps) = {
			let units = self.units.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			units.get(key).cloned()?
		};
		deps.hold(txn).await.then_some(unit)
	}

	/// Hold `unit` under `key`, `None` for a subscription whose clauses did not
	/// lower, while the catalog reads `deps` hold.
	pub(super) fn insert(
		&self,
		key: (Uuid, Option<Uuid>),
		unit: Option<Arc<LiveUnit>>,
		deps: CatalogDeps,
	) {
		let mut units = self.units.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		if units.len() >= CAPACITY
			&& !units.contains_key(&key)
			&& let Some(victim) = units.keys().next().copied()
		{
			units.remove(&victim);
		}
		units.insert(key, (unit, Arc::new(deps)));
	}
}

#[cfg(test)]
thread_local! {
	/// Units compiled, for tests that must know a write compiled none.
	pub(in crate::exec::vm) static COMPILED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// Units compiled in a writable transaction, for tests that must know a
	/// write transaction compiled none.
	pub(in crate::exec::vm) static COMPILED_WRITABLE: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	/// Whether every compile is refused, for tests of a subscription whose
	/// clauses do not lower.
	pub(in crate::exec::vm) static REFUSE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The units of `lives`, the subscriptions table `table` of `ns`/`db` holds, at
/// the catalog version `ctx`'s transaction reads the table at, one for each:
/// `None` for a subscription that has no unit. A subscription the node holds no
/// unit for is compiled here, in a fault ([`ensure`]). The list has one entry
/// for every subscription in `lives`.
pub(crate) async fn units_for(
	ctx: &FrozenContext,
	ns: &crate::catalog::NamespaceDefinition,
	db: &crate::catalog::DatabaseDefinition,
	table: &TableName,
	lives: &[SubscriptionDefinition],
) -> anyhow::Result<LiveUnitList> {
	if lives.is_empty() {
		return Ok(Arc::from(Vec::new()));
	}
	let txn = ctx.tx();
	let version = txn.catalog_version(ns.namespace_id, db.database_id, table).await?;
	let mut units = Vec::with_capacity(lives.len());
	for subscription in lives {
		units.push(
			crate::exec::vm::host::out_of_line(|| {
				ensure(
					ctx,
					&txn,
					(ns.name.as_str(), db.name.as_str()),
					table,
					version,
					subscription,
				)
			})
			.await?,
		);
	}
	Ok(Arc::from(units))
}

/// One subscription's unit as compiled when its `LIVE` statement arrived,
/// with the catalog version of the table it was compiled against.
/// [`registered`] holds it only for that same subscription, table and version.
pub(crate) struct Arrived {
	subscription: Uuid,
	table: TableName,
	version: Option<Uuid>,
	outcome: Lowered<LiveUnit>,
	deps: Option<CatalogDeps>,
}

/// Compile the unit of `subscription` for table `table` of `ns`/`db` in `txn`,
/// as its `LIVE` statement arrives and before the transaction that registers
/// it opens. `txn` is a read transaction, so the compile reads the catalog
/// without a registration's cached state cleared and holds nothing the
/// registration writes. Arrival is query arrival, not a fault, so this
/// compiles outside [`crate::exec::compiles::serving_fault`].
///
/// `None` for a subscription served nothing, a node that holds no units, or a
/// catalog version that cannot be read; registration then compiles nothing
/// and raises its own errors. A refusal is carried in the outcome for
/// [`registered`] to raise.
pub(crate) async fn arrive(
	ctx: &FrozenContext,
	txn: &Arc<Transaction>,
	(ns, db): (NamespaceId, DatabaseId),
	(ns_name, db_name): (&str, &str),
	table: &TableName,
	subscription: &SubscriptionDefinition,
) -> Option<Arrived> {
	ctx.live_units()?;
	subscriber(subscription)?;
	let version = txn.catalog_version(ns, db, table).await.ok()?;
	let (outcome, deps) =
		Box::pin(compile_kept(ctx, txn, ns_name, db_name, table, subscription)).await;
	Some(Arrived {
		subscription: subscription.id,
		table: table.clone(),
		version,
		outcome,
		deps,
	})
}

/// Register `subscription`, just written to table `table` of `ns`/`db` in
/// `ctx`'s transaction, with the unit compiled as its statement arrived
/// ([`arrive`]). Nothing is compiled here: the transaction is a write
/// transaction.
///
/// The unit is held at the table's catalog version only if that version is the
/// one it was compiled against. Where there is no arrival, it names another
/// subscription or table, or the table's catalog version has moved, nothing is
/// held and the first write to the table compiles the unit in a fault. A
/// subscription whose clauses did not lower is an error, so the registration
/// fails and the client is told rather than never notified, as is an invariant
/// the lowering broke.
pub(crate) async fn registered(
	ctx: &FrozenContext,
	(ns, db): (NamespaceId, DatabaseId),
	table: &TableName,
	subscription: &SubscriptionDefinition,
	arrived: Option<Arrived>,
) -> anyhow::Result<()> {
	let Some(node) = ctx.live_units() else {
		return Ok(());
	};
	let Some(arrived) = arrived else {
		return Ok(());
	};
	if arrived.subscription != subscription.id || arrived.table != *table {
		return Ok(());
	}
	let version = ctx.tx().catalog_version(ns, db, table).await?;
	if version != arrived.version {
		return Ok(());
	}
	match arrived.outcome {
		Ok(unit) => {
			if let Some(deps) = arrived.deps {
				node.insert((subscription.id, arrived.version), Some(Arc::new(unit)), deps);
			}
			Ok(())
		}
		Err(refused) if refused.kind == NotCoveredKind::Internal => Err(internal(refused)),
		Err(refused) => Err(anyhow::Error::new(crate::exec::Error::Thrown(format!(
			"LIVE query cannot be served: its clauses do not compile: {}",
			refused.reason
		)))),
	}
}

/// The unit of `subscription` on `table` at catalog version `version`, for a
/// write that reads the table in `txn`: the node's unit where it holds one,
/// and otherwise one compiled at that point, in a fault, and kept unless `txn`
/// speculates.
///
/// `None` for a subscription served nothing, or whose clauses do not lower.
/// The latter is logged and held as refused under the same key and catalog
/// reads a unit would be, so the writes after it do not compile it again
/// while what it read holds. An error is an invariant the lowering broke,
/// which fails the write rather than one subscriber's notification.
pub(crate) async fn ensure(
	ctx: &FrozenContext,
	txn: &Arc<Transaction>,
	(ns, db): (&str, &str),
	table: &TableName,
	version: Option<Uuid>,
	subscription: &SubscriptionDefinition,
) -> anyhow::Result<Option<Arc<LiveUnit>>> {
	if subscriber(subscription).is_none() {
		return Ok(None);
	}
	let key = (subscription.id, version);
	let node = ctx.live_units();
	if let Some(node) = node.as_ref()
		&& let Some(held) = node.get(&key, txn).await
	{
		return Ok(held);
	}
	let compiled = crate::exec::compiles::serving_fault(Box::pin(compile_kept(
		ctx,
		txn,
		ns,
		db,
		table,
		subscription,
	)))
	.await;
	match compiled {
		(Ok(unit), deps) => {
			let unit = Arc::new(unit);
			if let (Some(node), Some(deps)) = (node, deps) {
				node.insert(key, Some(Arc::clone(&unit)), deps);
			}
			Ok(Some(unit))
		}
		(Err(refused), deps) => {
			refusal(subscription, refused)?;
			if let (Some(node), Some(deps)) = (node, deps) {
				node.insert(key, None, deps);
			}
			Ok(None)
		}
	}
}

/// What a refusal of a registered `subscription`'s work means: `Ok`, logged
/// as a warning, for clauses that fail to lower after registration — the subscription is not
/// notified, and the writes to its table go on — and an error for an
/// invariant the lowering broke.
fn refusal(subscription: &SubscriptionDefinition, refused: NotCovered) -> anyhow::Result<()> {
	if refused.kind == NotCoveredKind::Internal {
		return Err(internal(refused));
	}
	tracing::warn!(
		target: "surrealdb::core::exec::vm::lives",
		subscription_id = %subscription.id,
		reason = %refused.reason,
		"LIVE subscription is not notified: its clauses do not lower",
	);
	Ok(())
}

/// The error of `refused`, an invariant the lowering broke.
fn internal(refused: NotCovered) -> anyhow::Error {
	anyhow::Error::new(crate::err::Error::Engine(crate::err::EngineError::Internal(refused.reason)))
}

/// The unit of `subscription` on `table` ([`compile`]), or its refusal, with
/// the state of every catalog entry its compilation read where the outcome
/// may be held: `None` where `txn` speculates, where the reads cannot be
/// named, or where the refusal is an invariant the lowering broke.
async fn compile_kept(
	ctx: &FrozenContext,
	txn: &Arc<Transaction>,
	ns: &str,
	db: &str,
	table: &TableName,
	subscription: &SubscriptionDefinition,
) -> (Lowered<LiveUnit>, Option<CatalogDeps>) {
	let recording = txn.record_catalog_reads();
	let unit = compile(ctx, txn, ns, db, table, subscription).await;
	let reads = recording.finish();
	let deps = match &unit {
		Err(refused) if refused.kind == NotCoveredKind::Internal => None,
		_ if txn.speculating() => None,
		_ => CatalogDeps::resolve(txn, &reads).await,
	};
	(unit, deps)
}

/// The unit of `subscription` on `table`, compiled with a planner of its
/// subscriber's: its principal, the database it watches, the catalog `txn`
/// reads, and every parameter its frame binds left to the frame.
pub(crate) async fn compile(
	ctx: &FrozenContext,
	txn: &Arc<Transaction>,
	ns: &str,
	db: &str,
	table: &TableName,
	subscription: &SubscriptionDefinition,
) -> Lowered<LiveUnit> {
	#[cfg(test)]
	{
		COMPILED.with(|compiled| compiled.set(compiled.get() + 1));
		if txn.writeable() {
			COMPILED_WRITABLE.with(|compiled| compiled.set(compiled.get() + 1));
		}
		if REFUSE.with(|refuse| refuse.get()) {
			return Err(NotCovered::unsupported("refused for a test"));
		}
	}
	let Some(Subscriber {
		compiled,
		session,
		auth,
	}) = subscriber(subscription)
	else {
		return Err(NotCovered::unsupported("a subscription served nothing has no unit"));
	};
	let shadowed: Vec<String> = SUBSCRIBER_NAMES
		.iter()
		.map(|name| (*name).to_owned())
		.chain(subscription.vars.keys().cloned())
		.collect();
	let planner = Planner::with_txn(
		ctx,
		ctx.function_registry(),
		Arc::clone(txn),
		Some(ns.to_owned()),
		Some(db.to_owned()),
	)
	.with_auth(Arc::clone(&auth))
	.with_perms_suppressed(false)
	.shadowing(shadowed.iter().cloned());
	let mut opt = Options::new(&ctx.config.exec);
	opt.set_ns(Some(Arc::from(ns)));
	opt.set_db(Some(Arc::from(db)));
	let plan_session = PlanSession::new(opt.with_auth(auth), Arc::new(session.clone()));
	let request = SubscriptionRequest {
		table,
		compiled,
		shadowed,
	};
	super::compile::compile_subscription(&planner, &plan_session, &request).await
}
