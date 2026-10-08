//! The node's compiled live-subscription units.
//!
//! A live subscription's work on a record — the table's permission and field
//! view for the subscriber, its `WHERE`, its projection — is a unit of
//! bytecode of its own ([`LiveUnit`]), compiled for the subscriber's
//! principal against the catalog of the table it watches
//! ([`super::lower_live`]), and held by the node keyed by the subscription's
//! id and the table's catalog version. A held unit is reused only while every
//! other catalog entry its compilation read — a function its clauses call, a
//! parameter they read — is as it was read ([`CatalogDeps::hold`]); a unit
//! whose reads cannot be named is not held. A write's context carries the units of
//! its table's subscriptions at the version it read, and its instructions only
//! look them up. A unit is compiled as a bare `LIVE` statement arrives, in a
//! read transaction before the statement's write transaction opens, and is held
//! when the subscription registers if the table's catalog version is unchanged
//! ([`super::lower_live::arrive`]); a `LIVE` inside a transaction block or a
//! caller-owned transaction compiles nothing as it registers. A unit is also
//! compiled when a write to its table is planned, and, for a subscription the
//! node holds no unit for at that version, in a fault as the write's context is
//! resolved ([`super::lower_live::ensure`]).
//!
//! A subscription served nothing has no unit: its session has expired, it
//! holds no principal, or its stored text no longer compiles. A subscription
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
use crate::catalog::SubscriptionDefinition;
use crate::ctx::FrozenContext;
use crate::exec::catalog_deps::CatalogDeps;
use crate::kvs::Transaction;

/// A live subscription's work compiled for the VM: the program whose host
/// pipelines the plan names, for the subscription's principal.
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
		if units.len() >= CAPACITY {
			units.clear();
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
/// `None` for a subscription with none. A subscription the node holds no unit
/// for is compiled here, in a fault ([`super::lower_live::ensure`]), so the
/// list has one entry for every subscription in `lives`.
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
				super::lower_live::ensure(
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
