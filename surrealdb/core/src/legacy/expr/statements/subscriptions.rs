use std::sync::Arc;

use anyhow::Result;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::catalog::{self, DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::dbs::RoutedNotification;
use crate::kvs::Transaction;
use crate::types::{PublicAction, PublicNotification, PublicValue};
use crate::val::TableName;

/// Queue a `KILLED` for every subscription on a table, to be sent if and when
/// the enclosing transaction commits.
///
/// The removal is about to destroy the `lq`/`lv` keys these clients are waiting
/// on, so each is owed the one notification that tells it to stop waiting.
/// Queued rather than sent, so a statement that is later rolled back does not
/// tear down subscriptions whose rows still exist.
///
/// Failing to enumerate is logged, not propagated. The notification is a
/// courtesy owed to some other client, while the removal is what this user
/// asked for; refusing a destructive statement because a third party's row is
/// unreadable inverts the blast radius, and that row is only reachable by
/// completing the removal being attempted.
pub(crate) async fn kill_table_subscriptions_where(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	owned_by: &dyn Fn(&catalog::SubscriptionDefinition) -> bool,
) -> Result<()> {
	// Embedded datastores and most of the test harness have no broker.
	let Some(sender) = ctx.broker() else {
		return Ok(());
	};
	let lvs = match txn.all_tb_lives(ns, db, tb, None).await {
		Ok(lvs) => lvs,
		Err(e) => {
			warn!(
				target: "surrealdb::core::expr",
				table = %tb,
				error = %e,
				"Could not read the subscriptions on a table being removed; its \
				 subscribers will not be told that it is gone"
			);
			return Ok(());
		}
	};
	for lv in lvs.iter().filter(|lv| owned_by(lv)) {
		txn.register_live_query_kill_after_commit(
			Arc::clone(sender),
			RoutedNotification::new(
				lv.node,
				PublicNotification::new(
					lv.id.into(),
					None,
					PublicAction::Killed,
					PublicValue::None,
					PublicValue::None,
				),
			),
		)
		.await;
	}
	Ok(())
}

/// Every subscription on a table; see [`kill_table_subscriptions_where`].
pub(crate) async fn kill_table_subscriptions(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> Result<()> {
	kill_table_subscriptions_where(ctx, txn, ns, db, tb, &|_| true).await
}

/// See [`kill_table_subscriptions_where`]. Removing a database destroys every
/// table's keys with it.
pub(crate) async fn kill_database_subscriptions(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
) -> Result<()> {
	kill_database_subscriptions_where(ctx, txn, ns, db, &|_| true).await
}

/// Every subscription in a database that `owned_by` selects.
pub(crate) async fn kill_database_subscriptions_where(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	owned_by: &dyn Fn(&catalog::SubscriptionDefinition) -> bool,
) -> Result<()> {
	for tb in txn.all_tb(ns, db, None).await?.iter() {
		kill_table_subscriptions_where(ctx, txn, ns, db, &tb.name, owned_by).await?;
	}
	Ok(())
}

/// Remove every subscription in a database that runs as `actor`, and tell its
/// client.
///
/// Unlike the helpers above, this one deletes the rows as well as notifying.
/// Those exist because a statement is about to destroy the keys anyway and the
/// subscriber is owed the news; here nothing else removes anything, so
/// revocation has to do it. A subscription captures its `Auth` at `LIVE` time
/// and `doc::lives` replays it verbatim on every notification, so without this
/// `REMOVE USER u` leaves `u`'s subscriptions streaming rows to a still-open
/// socket, evaluated under `u`'s permissions.
///
/// Scoped to one database because that is where the `lq` rows live. A root- or
/// namespace-level principal can hold subscriptions in any database, so those
/// callers sweep each one.
pub(crate) async fn kill_principal_subscriptions(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	actor: &str,
) -> Result<()> {
	for tb in txn.all_tb(ns, db, None).await?.iter() {
		let lvs = match txn.all_tb_lives(ns, db, &tb.name, None).await {
			Ok(lvs) => lvs,
			Err(e) => {
				warn!(
					target: "surrealdb::core::expr",
					table = %tb.name,
					error = %e,
					"Could not read the subscriptions on a table while revoking a principal; 					 any it owns there keep running"
				);
				continue;
			}
		};
		let mut removed_any = false;
		for lv in lvs.iter().filter(|lv| lv.auth.as_ref().is_some_and(|a| a.id() == actor)) {
			txn.clr_key(&crate::key::table::lq::Lq {
				prefix: crate::key::database::all::DatabaseRoot {
					ns,
					db,
				},
				tb: std::borrow::Cow::Borrowed(&tb.name),
				lq: lv.id,
			})
			.await?;
			txn.clr_key(&crate::key::node::lq::Lq {
				nd: lv.node,
				lq: lv.id,
			})
			.await?;
			removed_any = true;
			if let Some(sender) = ctx.broker() {
				txn.register_live_query_kill_after_commit(
					Arc::clone(sender),
					RoutedNotification::new(
						lv.node,
						PublicNotification::new(
							lv.id.into(),
							None,
							PublicAction::Killed,
							PublicValue::None,
							PublicValue::None,
						),
					),
				)
				.await;
			}
		}
		if removed_any {
			// Writers cache the subscriber list against this stamp, so it has
			// to move in the same transaction as the deletions.
			txn.bump_table_lives_cache(ns, db, &tb.name).await?;
		}
	}
	Ok(())
}

/// See [`kill_table_subscriptions_where`].
///
/// This reaches only databases the catalog still lists. A database removed but
/// not yet reclaimed keeps its subscription rows under a prefix `all_db` no
/// longer yields, so those subscribers were already notified by the
/// `REMOVE DATABASE` that orphaned them.
pub(crate) async fn kill_namespace_subscriptions(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
) -> Result<()> {
	for db in txn.all_db(ns, None).await?.iter() {
		kill_database_subscriptions(ctx, txn, ns, db.database_id).await?;
	}
	Ok(())
}

/// See [`kill_principal_subscriptions`]. A namespace-level principal can hold
/// subscriptions in any database of that namespace.
pub(crate) async fn kill_namespace_principal_subscriptions(
	ctx: &FrozenContext,
	txn: &Transaction,
	ns: NamespaceId,
	actor: &str,
) -> Result<()> {
	for db in txn.all_db(ns, None).await?.iter() {
		kill_principal_subscriptions(ctx, txn, ns, db.database_id, actor).await?;
	}
	Ok(())
}

/// See [`kill_principal_subscriptions`]. A root principal can hold
/// subscriptions anywhere.
pub(crate) async fn kill_root_principal_subscriptions(
	ctx: &FrozenContext,
	txn: &Transaction,
	actor: &str,
) -> Result<()> {
	for ns in txn.all_ns(None).await?.iter() {
		kill_namespace_principal_subscriptions(ctx, txn, ns.namespace_id, actor).await?;
	}
	Ok(())
}
