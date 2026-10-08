//! Lowering a live subscription's work for the node's unit cache
//! ([`super::live`]).
//!
//! A subscription's unit is compiled for the subscriber's principal against
//! the catalog of the table it watches. Units are compiled:
//!
//! - as a bare `LIVE` statement arrives, in a read transaction before its write transaction opens
//!   ([`arrive`]), and held when it registers if the table's catalog version is unchanged
//!   ([`registered`]);
//! - when a write to a table is planned against a catalog version the node holds no unit for
//!   ([`prepare`]);
//! - in a fault, when a write's context is resolved and the node holds none ([`ensure`]).
//!
//! A unit a running program needs is therefore always either held or compiled
//! in a fault. A unit is held only with the catalog state its compilation read
//! ([`compile_kept`]), and reused only while that state holds. A subscription
//! whose clauses do not lower at arrival fails to register; one that stops
//! lowering later is held as refused the same way a unit is held
//! ([`refusal`]). A registration compiles nothing, so a `LIVE` inside a
//! transaction block or a caller-owned transaction holds no unit until the
//! first write to its table.

use std::sync::Arc;

use surrealdb_strand::TableName;
use uuid::Uuid;

use super::NotCovered;
use super::live::LiveUnit;
use super::lower::Lowered;
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, SubscriptionDefinition};
use crate::ctx::FrozenContext;
use crate::exec::catalog_deps::CatalogDeps;
use crate::exec::operators::write::stages::lives::{Subscriber, subscriber};
use crate::exec::planner::Planner;
use crate::kvs::Transaction;

/// The parameters a subscription's frame binds, per record or from its
/// registration, which its unit must read from the frame and never fold.
const FRAME_PARAMS: [&str; 8] =
	["before", "after", "value", "event", "auth", "session", "access", "token"];

/// Compile the units of the subscriptions table `table` holds as `planner`'s
/// transaction reads it, for a write that `planner` is planning, which each
/// is held at the table's catalog version: those the node does not hold at
/// that version already. Planning is not a fault, so this compiles outside
/// [`crate::exec::compiles::serving_fault`].
///
/// A subscription registered after the plan was made is compiled as it
/// registers; one this node loads from another's registration is compiled
/// here, as the first write to its table on this node is planned.
pub(crate) async fn prepare(planner: &Planner<'_>, table: &TableName) {
	let (Some(txn), Some((ns, db)), Some(ns_name), Some(db_name)) = (
		planner.txn().cloned(),
		crate::exec::vm::host::out_of_line(|| planner.ns_db_ids()).await,
		planner.ns(),
		planner.db(),
	) else {
		return;
	};
	let ctx = planner.frozen_ctx();
	let Some(node) = ctx.live_units() else {
		return;
	};
	let (Ok(lives), Ok(version)) =
		(txn.all_tb_lives(ns, db, table, None).await, txn.catalog_version(ns, db, table).await)
	else {
		return;
	};
	for subscription in lives.iter() {
		if subscriber(subscription).is_none() {
			continue;
		}
		let key = (subscription.id, version);
		if node.get(&key, &txn).await.is_some() {
			continue;
		}
		match Box::pin(compile_kept(ctx, &txn, ns_name, db_name, table, subscription)).await {
			(Ok(unit), deps) => {
				if let Some(deps) = deps {
					node.insert(key, Some(Arc::new(unit)), deps);
				}
			}
			// An invariant the lowering broke is not held: the write that finds
			// no unit compiles it again, and fails there.
			(Err(refused), deps) => {
				if refusal(subscription, refused).is_ok()
					&& let Some(deps) = deps
				{
					node.insert(key, None, deps);
				}
			}
		}
	}
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
		Err(refused) if refused.internal => Err(internal(refused)),
		Err(refused) => Err(anyhow::Error::new(crate::exec::Error::Thrown(format!(
			"LIVE query cannot be served: its clauses do not compile: {}",
			refused.reason
		)))),
	}
}

/// The unit of `subscription` on `table` at catalog version `version`, for a
/// write that reads the table in `txn`: the node's unit where it holds one,
/// and otherwise one compiled now, in a fault, and kept unless `txn`
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
/// as a warning, for clauses that no longer lower — the subscription is not
/// notified, and the writes to its table go on — and an error for an
/// invariant the lowering broke.
fn refusal(subscription: &SubscriptionDefinition, refused: NotCovered) -> anyhow::Result<()> {
	if refused.internal {
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
		Err(refused) if refused.internal => None,
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
		super::live::COMPILED.with(|compiled| compiled.set(compiled.get() + 1));
		if txn.writeable() {
			super::live::COMPILED_WRITABLE.with(|compiled| compiled.set(compiled.get() + 1));
		}
		if super::live::REFUSE.with(|refuse| refuse.get()) {
			return Err(NotCovered::new("refused for a test"));
		}
	}
	let Some(Subscriber {
		compiled,
		auth,
		..
	}) = subscriber(subscription)
	else {
		return Err(NotCovered::new("a subscription served nothing has no unit"));
	};
	let shadowed = FRAME_PARAMS
		.iter()
		.map(|name| (*name).to_owned())
		.chain(subscription.vars.keys().cloned())
		.collect::<Vec<_>>();
	let planner = Planner::with_txn(
		ctx,
		ctx.function_registry(),
		Arc::clone(txn),
		Some(ns.to_owned()),
		Some(db.to_owned()),
	)
	.with_auth(auth)
	.with_perms_suppressed(false)
	.shadowing(shadowed);
	super::lower::lower_subscription(&planner, table.clone(), compiled).await
}
