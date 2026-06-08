use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::SubscriptionDefinition;
use crate::ctx::FrozenContext;
use crate::dbs::{Options, RoutedNotification};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Expr, FlowResultExt as _};
use crate::iam::Error as IamError;
use crate::types::{PublicAction, PublicNotification, PublicValue};
use crate::val::{Uuid, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Expr,
}

impl KillStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "KillStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Is realtime enabled?
		ctx.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Resolve live query id
		let lid = match stk
			.run(|stk| self.id.compute(stk, ctx, opt, None))
			.await
			.catch_return()?
			.cast_to::<Uuid>()
		{
			Err(_) => {
				bail!(Error::KillStatement {
					value: self.id.to_sql(),
				})
			}
			Ok(id) => id,
		};
		// Get the Node ID
		let nid = ctx.node_id();
		// Get the LIVE ID
		let lid = lid.0;
		// Get the transaction
		let txn = ctx.tx();
		// Fetch the live query key
		let key = crate::key::node::lq::new(nid, lid);
		// Fetch the live query key if it exists
		match txn.get(&key, None).await? {
			Some(live) => {
				// Verify that the requesting user is the owner of this live query.
				// Root-level users may kill any live query; all other users may only
				// kill live queries they themselves created.
				if ctx.auth_enabled() && !opt.auth.is_root() {
					let table_key = crate::key::table::lq::new(live.ns, live.db, &live.tb, lid);
					let subscription: Option<SubscriptionDefinition> =
						txn.get(&table_key, None).await?;
					if let Some(sub) = subscription {
						// For live queries created before auth tracking was introduced
						// (sub.auth is None), we have no ownership information and
						// cannot verify the caller is the original owner. Fail closed:
						// only root may kill legacy live queries without auth metadata.
						//
						// Compare only user identity (id + level), not the full Auth
						// snapshot. Roles are excluded so that a legitimate owner whose
						// roles were changed by an admin after the LIVE query was created
						// can still kill their own live query.
						let is_owner = sub.auth.as_ref().is_some_and(|live_auth| {
							live_auth.id() == opt.auth.id() && live_auth.level() == opt.auth.level()
						});
						if !is_owner {
							bail!(Error::IamError(IamError::NotAllowed {
								actor: opt.auth.id().to_string(),
								action: "KILL".to_string(),
								resource: lid.to_string(),
							}));
						}
					} else {
						// Deny when the subscription record is absent — fail closed
						// to prevent ownership bypass via a missing or corrupted entry.
						bail!(Error::IamError(IamError::NotAllowed {
							actor: opt.auth.id().to_string(),
							action: "KILL".to_string(),
							resource: lid.to_string(),
						}));
					}
				}
				// Delete the node live query
				let key = crate::key::node::lq::new(nid, lid);
				txn.clr(&key).await?;
				// Delete the table live query
				let key = crate::key::table::lq::new(live.ns, live.db, &live.tb, lid);
				txn.clr(&key).await?;
				// Refresh the table cache for lives
				if let Some(cache) = ctx.get_cache() {
					cache.set_live_queries_version(live.ns, live.db, &live.tb);
				}
				// Clear the cache
				txn.clear_cache();
			}
			None => {
				bail!(Error::KillStatement {
					value: self.id.to_sql(),
				});
			}
		}
		if let Some(sender) = ctx.broker() {
			sender
				.send(RoutedNotification::new(
					nid,
					PublicNotification::new(
						lid.into(),
						None,
						PublicAction::Killed,
						PublicValue::None,
						PublicValue::None,
					),
				))
				.await;
		}
		// Return the query id
		Ok(Value::None)
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::sync::Arc;

	use anyhow::Result;
	use surrealdb_types::vars;

	use crate::catalog::providers::CatalogProvider;
	use crate::channel::Receiver;
	use crate::dbs::{Capabilities, Session};
	use crate::iam::{Actor, Auth, Level, Role};
	use crate::kvs::Datastore;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::types::{PublicNotification, PublicRecordId, PublicRecordIdKey, PublicValue};

	async fn new_ds_with_auth() -> Result<(Receiver<PublicNotification>, Datastore)> {
		let (send, recv) = crate::channel::bounded(1000);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(true)
			.with_notify(send)
			.build_with_path("memory")
			.await?;
		Ok((recv, ds))
	}

	async fn new_ds_no_auth() -> Result<(Receiver<PublicNotification>, Datastore)> {
		let (send, recv) = crate::channel::bounded(1000);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(false)
			.with_notify(send)
			.build_with_path("memory")
			.await?;
		Ok((recv, ds))
	}

	/// Build a database-level session with an explicit user `id` and `role`.
	///
	/// Unlike [`Session::for_level`], this gives each user a distinct identity
	/// so tests can verify that ownership is checked by user ID rather than
	/// just by role.
	fn db_session(ns: &str, db: &str, id: &str, role: Role) -> Session {
		let auth =
			Auth::new(Actor::new(id.into(), vec![role], Level::Database(ns.into(), db.into())));
		Session {
			au: Arc::new(auth),
			ns: Some(ns.to_string()),
			db: Some(db.to_string()),
			rt: true,
			..Default::default()
		}
	}

	async fn setup_table(ds: &Datastore, ns: &str, db: &str, tb: &str) {
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(&format!("DEFINE TABLE {tb}"), &ses, None).await.unwrap();
	}

	async fn start_live_query(ds: &Datastore, ses: &Session, tb: &str) -> PublicValue {
		let stmt = format!("LIVE SELECT * FROM {tb}");
		let mut res = ds.execute(&stmt, ses, None).await.unwrap();
		res.remove(0).result.unwrap()
	}

	async fn kill_live_query(
		ds: &Datastore,
		ses: &Session,
		lid: &PublicValue,
	) -> anyhow::Result<()> {
		let mut res = ds.execute("KILL $uuid", ses, Some(vars!("uuid": lid.clone()))).await?;
		res.remove(0).result.map_err(anyhow::Error::from)?;
		Ok(())
	}

	/// A user who created a live query can kill it.
	#[tokio::test]
	async fn test_kill_own_live_query_succeeds() {
		let (_, ds) = new_ds_with_auth().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_table(&ds, ns, db, tb).await;

		let ses = Session::for_level(Level::Database(ns.to_string(), db.to_string()), Role::Owner)
			.with_rt(true);

		let lid = start_live_query(&ds, &ses, tb).await;
		kill_live_query(&ds, &ses, &lid)
			.await
			.expect("owner should be able to kill their own live query");
	}

	/// A user can kill their own live query even if their roles changed after the LIVE
	/// query was created.  This covers the case where an admin runs `DEFINE USER ... ROLES`
	/// between the LIVE and the KILL: the stored auth snapshot has the old roles, but
	/// the current session carries the new roles.  Only identity (id + level) is
	/// compared, so the role difference must not block the owner.
	#[tokio::test]
	async fn test_kill_own_live_query_after_role_change_succeeds() {
		let (_, ds) = new_ds_with_auth().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_table(&ds, ns, db, tb).await;

		let alice_owner = db_session(ns, db, "alice", Role::Owner);
		let lid = start_live_query(&ds, &alice_owner, tb).await;

		// Simulate an admin changing alice's role from Owner → Editor.
		let alice_editor = db_session(ns, db, "alice", Role::Editor);
		kill_live_query(&ds, &alice_editor, &lid)
			.await
			.expect("user should be able to kill their own live query even after a role change");
	}

	/// A database-level user cannot kill a live query created by a different database user.
	///
	/// Both users have the *same* role here to ensure the check is driven by identity
	/// (user ID + level), not by role difference.  Previously `Session::for_level`
	/// hard-coded `id = "system_auth"` for all system sessions, making the two users
	/// indistinguishable by identity.  We use `db_session` to give each user a
	/// distinct ID so the test genuinely exercises identity-based ownership.
	#[tokio::test]
	async fn test_kill_other_db_users_live_query_fails() {
		let (_, ds) = new_ds_with_auth().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_table(&ds, ns, db, tb).await;

		let alice_ses = db_session(ns, db, "alice", Role::Owner);
		let bob_ses = db_session(ns, db, "bob", Role::Owner);

		let lid = start_live_query(&ds, &alice_ses, tb).await;
		assert!(
			kill_live_query(&ds, &bob_ses, &lid).await.is_err(),
			"a different db user should not be able to kill another user's live query"
		);
	}

	/// A record user cannot kill a root user's live query.
	#[tokio::test]
	async fn test_record_user_cannot_kill_root_live_query() {
		let (_, ds) = new_ds_with_auth().await.unwrap();
		let (ns, db, tb, ac) = ("test", "test", "person", "user");
		setup_table(&ds, ns, db, tb).await;

		let root_ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		let record_ses = Session::for_record(
			ns,
			db,
			ac,
			PublicValue::RecordId(PublicRecordId {
				table: "person".to_string().into(),
				key: PublicRecordIdKey::String("alice".to_string()),
			}),
		)
		.with_rt(true);

		let lid = start_live_query(&ds, &root_ses, tb).await;
		assert!(
			kill_live_query(&ds, &record_ses, &lid).await.is_err(),
			"a record user should not be able to kill a root user's live query"
		);
	}

	/// A root user can kill any user's live query.
	#[tokio::test]
	async fn test_root_can_kill_any_live_query() {
		let (_, ds) = new_ds_with_auth().await.unwrap();
		let (ns, db, tb, ac) = ("test", "test", "person", "user");
		setup_table(&ds, ns, db, tb).await;

		let record_ses = Session::for_record(
			ns,
			db,
			ac,
			PublicValue::RecordId(PublicRecordId {
				table: "person".to_string().into(),
				key: PublicRecordIdKey::String("alice".to_string()),
			}),
		)
		.with_rt(true);
		let root_ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);

		let lid = start_live_query(&ds, &record_ses, tb).await;
		kill_live_query(&ds, &root_ses, &lid)
			.await
			.expect("a root user should be able to kill any live query");
	}

	/// When authentication is disabled, any user can kill any live query (backwards compat).
	///
	/// Use `db_session` with distinct IDs (alice/bob) so the two sessions are genuinely
	/// different identities.  With the identity-based ownership check in place, if a
	/// regression caused auth-disabled sessions to enter the ownership block, `is_owner`
	/// would return `false` (alice ≠ bob) and the test would correctly fail.  Using
	/// `Session::for_level` for both would give them the same `id="system_auth"`, making
	/// `is_owner` true and masking the regression.
	#[tokio::test]
	async fn test_kill_any_live_query_when_auth_disabled() {
		let (_, ds) = new_ds_no_auth().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_table(&ds, ns, db, tb).await;

		let ses_a = db_session(ns, db, "alice", Role::Owner);
		let ses_b = db_session(ns, db, "bob", Role::Editor);

		let lid = start_live_query(&ds, &ses_a, tb).await;
		kill_live_query(&ds, &ses_b, &lid)
			.await
			.expect("with auth disabled, any user should be able to kill any live query");
	}

	/// A non-root user cannot kill a live query whose subscription record has auth: None
	/// (legacy queries created before auth tracking was introduced, e.g. v3.0.0 and betas).
	/// Without ownership metadata we cannot verify the caller is the creator, so we fail closed.
	#[tokio::test]
	async fn test_kill_legacy_live_query_auth_none_fails_for_non_root() {
		let (_, ds) = new_ds_with_auth().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_table(&ds, ns, db, tb).await;

		let ses = Session::for_level(Level::Database(ns.to_string(), db.to_string()), Role::Owner)
			.with_rt(true);

		let lid = start_live_query(&ds, &ses, tb).await;
		let live_uuid = match &lid {
			PublicValue::Uuid(u) => (*u).into_inner(),
			_ => panic!("expected uuid"),
		};

		// Simulate a legacy live query by clearing auth on the stored SubscriptionDefinition.
		{
			let txn = ds.transaction(Write, Optimistic).await.unwrap();
			let db_def = txn.ensure_ns_db(None, ns, db).await.unwrap();
			let tb_name = crate::val::TableName::from(tb);
			let key = crate::key::table::lq::new(
				db_def.namespace_id,
				db_def.database_id,
				&tb_name,
				live_uuid,
			);
			let mut sub: crate::catalog::SubscriptionDefinition =
				txn.get(&key, None).await.unwrap().expect("subscription must exist");
			sub.auth = None;
			txn.set(&key, &sub).await.unwrap();
			txn.commit().await.unwrap();
		}

		assert!(
			kill_live_query(&ds, &ses, &lid).await.is_err(),
			"non-root user should not be able to kill a legacy live query with auth: None (fail closed)"
		);
	}

	/// Root can kill a live query whose subscription record has auth: None (legacy queries).
	/// Root bypasses ownership checks entirely.
	#[tokio::test]
	async fn test_root_can_kill_legacy_live_query_auth_none() {
		let (_, ds) = new_ds_with_auth().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_table(&ds, ns, db, tb).await;

		let db_ses =
			Session::for_level(Level::Database(ns.to_string(), db.to_string()), Role::Owner)
				.with_rt(true);
		let root_ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);

		let lid = start_live_query(&ds, &db_ses, tb).await;
		let live_uuid = match &lid {
			PublicValue::Uuid(u) => (*u).into_inner(),
			_ => panic!("expected uuid"),
		};

		// Simulate a legacy live query by clearing auth on the stored SubscriptionDefinition.
		{
			let txn = ds.transaction(Write, Optimistic).await.unwrap();
			let db_def = txn.ensure_ns_db(None, ns, db).await.unwrap();
			let tb_name = crate::val::TableName::from(tb);
			let key = crate::key::table::lq::new(
				db_def.namespace_id,
				db_def.database_id,
				&tb_name,
				live_uuid,
			);
			let mut sub: crate::catalog::SubscriptionDefinition =
				txn.get(&key, None).await.unwrap().expect("subscription must exist");
			sub.auth = None;
			txn.set(&key, &sub).await.unwrap();
			txn.commit().await.unwrap();
		}

		kill_live_query(&ds, &root_ses, &lid)
			.await
			.expect("root should be able to kill a legacy live query with auth: None");
	}

	/// When the table-level subscription record is missing, KILL must fail closed for
	/// non-root users rather than silently skipping the ownership check.
	///
	/// Use `db_session` with distinct IDs (alice/bob) for consistency with the rest of
	/// the ownership-related tests in this module.  The `else` branch is an unconditional
	/// `bail!` that ignores caller identity, so production behaviour is not affected, but
	/// using genuinely distinct principals future-proofs the test against any weakening
	/// of that branch.
	#[tokio::test]
	async fn test_kill_without_subscription_record_fails_closed() {
		let (_, ds) = new_ds_with_auth().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_table(&ds, ns, db, tb).await;

		let owner_ses = db_session(ns, db, "alice", Role::Owner);
		let other_ses = db_session(ns, db, "bob", Role::Editor);

		let lid = start_live_query(&ds, &owner_ses, tb).await;
		let live_uuid = match &lid {
			PublicValue::Uuid(u) => (*u).into_inner(),
			_ => panic!("expected uuid"),
		};

		// Delete the table-level subscription record to simulate data corruption or a
		// concurrent cleanup race, then verify that KILL is denied for a non-root user.
		{
			let txn = ds.transaction(Write, Optimistic).await.unwrap();
			let db_def = txn.ensure_ns_db(None, ns, db).await.unwrap();
			let tb_name = crate::val::TableName::from(tb);
			let key = crate::key::table::lq::new(
				db_def.namespace_id,
				db_def.database_id,
				&tb_name,
				live_uuid,
			);
			txn.clr(&key).await.unwrap();
			txn.commit().await.unwrap();
		}

		assert!(
			kill_live_query(&ds, &other_ses, &lid).await.is_err(),
			"KILL should fail closed when the subscription record is absent"
		);
	}
}
