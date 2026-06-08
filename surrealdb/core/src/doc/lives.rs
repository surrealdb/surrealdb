use std::collections::BTreeSet;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_channel::Sender;
use chrono::Utc;
use futures::future::try_join_all;
use reblessive::TreeStack;
use reblessive::tree::Stk;
use tracing::instrument;

use super::IgnoreError;
use crate::catalog::{Permission, SubscriptionDefinition, SubscriptionFields};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{MessageBroker, Options, RoutedNotification};
use crate::doc::{Action, CursorDoc, Document};
use crate::err::Error;
use crate::expr::FlowResultExt as _;
use crate::expr::paths::{AC, ID, RD, TK};
use crate::kvs::Transaction;
use crate::types::{PublicAction, PublicNotification};
use crate::val::{Value, convert_value_to_public_value};

impl Document {
	/// Processes any LIVE SELECT statements which
	/// have been defined for the table which this
	/// record belongs to. This functions loops
	/// through the live queries and processes them
	/// all within the currently running transaction.
	pub(super) async fn process_table_lives(
		&mut self,
		_stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		action: Action,
	) -> Result<()> {
		// Check import
		if opt.import {
			return Ok(());
		}

		// Check if we can send notifications
		if ctx.broker().is_none() {
			// no sender, so nothing to do.
			return Ok(());
		};

		// Check if changed
		if !self.is_modified() {
			return Ok(());
		}

		// Get all live queries for this table
		let live_subscriptions = self.doc_ctx.lv()?;

		// If there are no live queries, we can skip the rest of the function
		if live_subscriptions.is_empty() {
			return Ok(());
		}

		// Get the event action
		let (met, is_delete): (Arc<Value>, _) = match action {
			Action::Delete => (Value::from("DELETE").into(), true),
			Action::Create => (Value::from("CREATE").into(), false),
			Action::Update => (Value::from("UPDATE").into(), false),
		};

		// Get the current and initial docs
		// These are only used for EVENTS, so they should not be reduced
		let initial = self.initial.doc.as_arc();
		let current = self.current.doc.as_arc();

		// Move self to a shared reference
		let doc: &Self = &*self;

		let mut tasks = Vec::with_capacity(live_subscriptions.len());
		// Loop through all index statements
		for live_subscription in live_subscriptions.iter() {
			// We need to create a new options which we will
			// use for processing this LIVE query statement.
			// This ensures that we are using the auth data
			// of the user who created the LIVE query.
			let lqopt = opt.new_with_perms(true);
			let (met, current, initial) =
				(Arc::clone(&met), Arc::clone(&current), Arc::clone(&initial));
			tasks.push(async move {
				let mut stack = TreeStack::new();
				stack
					.enter(|stk| {
						doc.lq_compute(
							stk,
							ctx,
							live_subscription.clone(),
							lqopt,
							ctx.tx(),
							(met, initial, current),
							is_delete,
						)
					})
					.finish()
					.await
			});
		}
		// Run the tasks concurrently
		try_join_all(tasks).await?;
		// Carry on
		Ok(())
	}

	/// SECURITY: this function is dispatched from `process_table_lives`
	/// inside `try_join_all`, so any `Err(...)` propagated from here will
	/// abort the triggering write across every concurrent subscription.
	/// Per-subscription failures (WHERE / projection / FETCH / reduce /
	/// computed-fields / public-value conversion) MUST be downgraded to
	/// `Ok(())` with a `tracing::debug!` rather than `?`-propagated.
	///
	/// The only `?` escapes intentionally left here are
	/// `sender.should_emit(...)?` (a broker-level error that already
	/// fails the entire write today) and the `Error::unreachable` for a
	/// missing record id (truly unreachable — LIVE notifications run
	/// after `process_record`).
	///
	/// Adding a new `?` to this function without an explicit comment is
	/// almost certainly a regression of the subscriber-isolation
	/// guarantee — please don't.
	#[allow(clippy::too_many_arguments, reason = "live-query dispatch shape")]
	async fn lq_compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		live_subscription: SubscriptionDefinition,
		opt: Options,
		tx: Arc<Transaction>,
		(met, initial, current): (Arc<Value>, Arc<Value>, Arc<Value>),
		is_delete: bool,
	) -> Result<()> {
		// Ensure that a session exists on the LIVE query
		let sess = match live_subscription.session.as_ref() {
			Some(v) => v,
			None => return Ok(()),
		};
		// Skip notification if the session that created this LIVE query has
		// expired. `session["exp"]` is a unix timestamp set by
		// `DURATION FOR SESSION`; absent means no expiry. We coerce via
		// `Number::to_int` so a Float or Decimal value (e.g. from a JS
		// client that serialised the timestamp as a float) still counts as
		// an expiry. This mirrors the per-request check in
		// `Datastore::execute` without requiring the originating Session
		// object to remain in memory beyond the RPC connection lifetime.
		if let Value::Object(ref session_obj) = *sess
			&& let Some(Value::Number(exp)) = session_obj.get("exp")
			&& Utc::now().timestamp() > (*exp).to_int()
		{
			return Ok(());
		}
		// Ensure that auth info exists on the LIVE query
		let auth = match live_subscription.auth.clone() {
			Some(v) => v,
			None => return Ok(()),
		};
		let opt = opt.with_auth(auth.into());

		let Some(sender) = ctx.broker() else {
			return Ok(());
		};

		// Get the record id of this document
		let rid = self
			.id
			.clone()
			.ok_or_else(|| {
				Error::unreachable("Processing live query for record without a Record ID")
			})
			.map_err(anyhow::Error::new)?;

		// We need to create a new context which we will
		// use for processing this LIVE query statement.
		// This ensures that we are using the session
		// of the user who created the LIVE query.
		let mut ctx = Context::background(ctx);
		// Set the current transaction on the new LIVE
		// query context to prevent unreachable behaviour
		// and ensure that queries can be executed.
		ctx.set_transaction(tx);
		// SECURITY: captured user variables MUST be added first so the
		// trusted LIVE / session params below overwrite any user-named
		// `$value` / `$before` / `$after` / `$event` / `$session` /
		// `$auth` / `$access` / `$token`. Otherwise a subscriber could
		// shadow the real document context that table permission
		// expressions read.
		ctx.add_values(live_subscription.vars.clone());
		// Add the session params to this LIVE query, so
		// that queries can use these within field
		// projections and WHERE clauses.
		ctx.add_value("access", sess.pick(AC.as_ref()).into());
		ctx.add_value("auth", sess.pick(RD.as_ref()).into());
		ctx.add_value("token", sess.pick(TK.as_ref()).into());
		ctx.add_value("session", sess.clone().into());
		// Add $before, $after, $value, and $event params
		// to this LIVE query so the user can use these
		// within field projections and WHERE clauses.
		ctx.add_value("event", met);
		ctx.add_value("value", Arc::clone(&current));
		ctx.add_value("after", current);
		ctx.add_value("before", initial);
		// Freeze the context
		let ctx = ctx.freeze();

		// Get the document to check against and to return based on lq context.
		// `prepare_live_doc` clones+reduces the source, populates computed
		// fields under the LIVE owner's auth, then re-cuts computed fields
		// that the owner does not have permission to read. It returns
		// `None` if any step fails so the notification is skipped rather
		// than the triggering write being aborted.
		let source = if is_delete {
			&self.initial
		} else {
			&self.current
		};
		let Some(doc) = self.prepare_live_doc(stk, &ctx, &opt, source).await else {
			return Ok(());
		};

		// First of all, let's check to see if the WHERE
		// clause of the LIVE query is matched by this
		// document. If it is then we can continue.
		match self.lq_check(stk, &ctx, &opt, &live_subscription, &doc).await {
			Err(IgnoreError::Ignore) => return Ok(()),
			Err(IgnoreError::Error(e)) => {
				tracing::debug!(
					target: "surrealdb::core::doc::lives",
					subscription_id = %live_subscription.id,
					error = %e,
					"LIVE notification skipped: WHERE clause evaluation failed",
				);
				return Ok(());
			}
			Ok(_) => (),
		}
		// Secondly, let's check to see if any PERMISSIONS
		// clause for this table allows this document to
		// be viewed by the user who created this LIVE
		// query. If it does, then we can continue.
		match self.lq_allow(stk, &ctx, &opt, is_delete).await {
			Err(IgnoreError::Ignore) => return Ok(()),
			Err(IgnoreError::Error(e)) => {
				tracing::debug!(
					target: "surrealdb::core::doc::lives",
					subscription_id = %live_subscription.id,
					error = %e,
					"LIVE notification skipped: table SELECT permission evaluation failed",
				);
				return Ok(());
			}
			Ok(_) => (),
		}
		if !sender.should_emit(*ctx.node_id().as_bytes(), *live_subscription.node.as_bytes())? {
			return Ok(());
		}
		// Let's check what type of statement
		// caused this LIVE query to run, and obtain
		// the relevant result.
		let (action, mut result) = match live_subscription.fields {
			SubscriptionFields::Diff => {
				// DIFF mode: return JSON patch operations instead of full document
				if is_delete {
					// For DELETE: diff the prepared (reduced + computed-field
					// filtered) initial view against None, mirroring how the
					// CREATE and UPDATE arms below use `doc`. Today the
					// `Value::Object → Value::None` case in `Value::diff`
					// collapses to a single `Replace { path: "", value: None }`
					// op, so no individual field names would leak even if we
					// used `self.initial` here. Using `doc` is defense in
					// depth: any future change that emits per-field ops for
					// this case (e.g. partial-delete semantics) inherits the
					// LIVE owner's SELECT permissions automatically.
					let operations = doc.doc.as_ref().diff(&Value::None);
					let result = Value::Array(
						operations.into_iter().map(|op| Value::Object(op.into_object())).collect(),
					);
					(PublicAction::Delete, result)
				} else if self.is_new() {
					// For CREATE: compute diff from empty object to current document
					let operations = Value::None.diff(doc.doc.as_ref());
					let result = Value::Array(
						operations.into_iter().map(|op| Value::Object(op.into_object())).collect(),
					);
					(PublicAction::Create, result)
				} else {
					// For UPDATE: prepare the LHS the same way the RHS was
					// prepared in `prepare_live_doc` (reduce + computed
					// fields + filter computed-field select permissions).
					// Without matching preparation on both sides, the diff
					// would emit spurious `Remove` / `Replace` ops for
					// restricted stored fields or COMPUTED fields. Any
					// evaluation error skips this notification rather than
					// aborting the triggering write.
					let Some(previous) =
						self.prepare_live_doc(stk, &ctx, &opt, &self.initial).await
					else {
						return Ok(());
					};
					let operations = previous.doc.as_ref().diff(doc.doc.as_ref());
					let result = Value::Array(
						operations.into_iter().map(|op| Value::Object(op.into_object())).collect(),
					);
					(PublicAction::Update, result)
				}
			}
			SubscriptionFields::Select(x) => {
				// Evaluate the projection. Any error (type mismatch, THROW,
				// BREAK, CONTINUE, closure result, etc.) skips this
				// notification without aborting the write — the projection
				// belongs to the LIVE query, not the triggering statement.
				let result =
					match x.compute(stk, &ctx, &opt, Some(&doc)).await.map_err(IgnoreError::from) {
						Err(IgnoreError::Ignore) => return Ok(()),
						Err(IgnoreError::Error(e)) => {
							tracing::debug!(
								target: "surrealdb::core::doc::lives",
								subscription_id = %live_subscription.id,
								error = %e,
								"LIVE notification skipped: projection evaluation failed",
							);
							return Ok(());
						}
						Ok(x) => x,
					};
				let action = if is_delete {
					PublicAction::Delete
				} else if self.is_new() {
					PublicAction::Create
				} else {
					PublicAction::Update
				};
				(action, result)
			}
		};

		// Process any potential `FETCH` clause on the live statement.
		// Any evaluation error (invalid function arguments, unsupported
		// expressions, etc.) skips this notification rather than
		// aborting the triggering write transaction.
		if let Some(fetchs) = live_subscription.fetch {
			let mut idioms = BTreeSet::new();
			for fetch in fetchs.iter() {
				if let Err(e) = fetch.compute(stk, &ctx, &opt, &mut idioms).await {
					tracing::debug!(
						target: "surrealdb::core::doc::lives",
						subscription_id = %live_subscription.id,
						error = %e,
						"LIVE notification skipped: FETCH expression evaluation failed",
					);
					return Ok(());
				}
			}
			for i in &idioms {
				if let Err(e) = stk.run(|stk| result.fetch(stk, &ctx, &opt, &i.0)).await {
					tracing::debug!(
						target: "surrealdb::core::doc::lives",
						subscription_id = %live_subscription.id,
						error = %e,
						"LIVE notification skipped: FETCH path resolution failed",
					);
					return Ok(());
				}
			}
		}

		// Extract the session ID from the session value
		let session_id = match sess.pick(ID.as_ref()) {
			Value::Uuid(uuid) => Some(uuid.into()),
			Value::String(s) => s.parse::<crate::val::Uuid>().ok().map(|uuid| uuid.into()),
			_ => None,
		};

		// Convert values to the public wire format. A conversion error
		// (e.g. a closure-valued projection that cannot be serialised)
		// skips this notification rather than aborting the triggering
		// write transaction.
		let rid_public = match convert_value_to_public_value(Value::RecordId(rid.as_ref().clone()))
		{
			Ok(v) => v,
			Err(e) => {
				tracing::debug!(
					target: "surrealdb::core::doc::lives",
					subscription_id = %live_subscription.id,
					error = %e,
					"LIVE notification skipped: record id could not be converted to public value",
				);
				return Ok(());
			}
		};
		let result_public = match convert_value_to_public_value(result) {
			Ok(v) => v,
			Err(e) => {
				tracing::debug!(
					target: "surrealdb::core::doc::lives",
					subscription_id = %live_subscription.id,
					error = %e,
					"LIVE notification skipped: result could not be converted to public value",
				);
				return Ok(());
			}
		};
		let notification = PublicNotification::new(
			live_subscription.id.into(),
			session_id,
			action,
			rid_public,
			result_public,
		);

		// Send the notification
		sender.send(RoutedNotification::new(live_subscription.node, notification)).await;

		Ok(())
	}

	/// Prepare an owned, reduced, computed, and computed-field-permission
	/// filtered view of `source` for a single LIVE subscription. Used for
	/// both the full-document and the DIFF UPDATE LHS payloads.
	///
	/// Returns `None` when any step fails — the caller should skip the
	/// notification rather than abort the triggering write. Every failure
	/// arm emits a `tracing::debug!` so the cause can still be
	/// correlated in traces.
	#[instrument(level = "trace", target = "surrealdb::core::doc::lives", skip_all)]
	async fn prepare_live_doc(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		source: &CursorDoc,
	) -> Option<CursorDoc> {
		// We need an owned clone here because computed-field evaluation
		// mutates the doc, and the outcome can differ per subscription.
		// `reduce_to_owned` collapses the reduction-required / clone-only
		// branches into one call. Both `reduce_to_owned` and
		// `computed_fields_inner` run under the LIVE owner's auth, which
		// may differ from the writer's; any evaluation error skips this
		// notification.
		let mut doc = match self.reduce_to_owned(stk, ctx, opt, source).await {
			Ok(d) => d,
			Err(e) => {
				tracing::debug!(
					target: "surrealdb::core::doc::lives",
					error = %e,
					"LIVE notification skipped: reduce_to_owned failed",
				);
				return None;
			}
		};
		if let Ok(rid) = self.id() {
			let fields = match self.doc_ctx.fd() {
				Ok(f) => f,
				Err(e) => {
					tracing::debug!(
						target: "surrealdb::core::doc::lives",
						error = %e,
						"LIVE notification skipped: field definitions unavailable",
					);
					return None;
				}
			};
			// Live-query notifications evaluate every computed field
			// for now. A future refinement can pass the closure of
			// fields the subscription's projection / WHERE actually
			// reads, mirroring what SELECT does, but the savings on
			// live tables are usually small and the bookkeeping less
			// obvious. Pass `None` to keep behaviour conservative.
			if let Err(e) =
				Document::computed_fields_inner(stk, ctx, opt, rid.as_ref(), fields, &mut doc, None)
					.await
			{
				tracing::debug!(
					target: "surrealdb::core::doc::lives",
					error = %e,
					"LIVE notification skipped: computed_fields_inner failed",
				);
				return None;
			}
			// SECURITY: `reduce_to_owned` runs before computed fields
			// are populated, so it can't filter them; apply the
			// computed-field `FOR select` permissions now so a
			// subscriber without permission to read a computed field
			// never receives its value in the LIVE notification.
			if let Err(e) = self.filter_computed_field_permissions(stk, ctx, opt, &mut doc).await {
				tracing::debug!(
					target: "surrealdb::core::doc::lives",
					error = %e,
					"LIVE notification skipped: computed-field permission filtering failed",
				);
				return None;
			}
		}
		Some(doc)
	}

	/// Check the WHERE clause for a LIVE query
	async fn lq_check(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		live_subscription: &SubscriptionDefinition,
		doc: &CursorDoc,
	) -> Result<(), IgnoreError> {
		// Check where condition
		if let Some(cond) = live_subscription.cond.as_ref() {
			// Check if the expression is truthy
			if !stk
				.run(|stk| cond.compute(stk, ctx, opt, Some(doc)))
				.await
				.catch_return()?
				.is_truthy()
			{
				// Ignore this document
				return Err(IgnoreError::Ignore);
			}
		}
		// Carry on
		Ok(())
	}
	/// Check any PERMISSIONS for a LIVE query
	async fn lq_allow(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		is_delete: bool,
	) -> Result<(), IgnoreError> {
		// Should we run permissions checks?
		// Live queries are always
		if ctx.check_perms(opt, crate::iam::Action::View)? {
			// Get the table
			let tb = self.doc_ctx.tb()?;
			// Process the table permissions
			match &tb.permissions.select {
				Permission::None => return Err(IgnoreError::Ignore),
				Permission::Full => return Ok(()),
				Permission::Specific(e) => {
					// Retrieve the document to check permissions against
					let doc = if is_delete {
						&self.initial
					} else {
						&self.current
					};

					// Disable permissions
					let opt = &opt.new_with_perms(false);
					// Process the PERMISSION clause
					if !stk
						.run(|stk| e.compute(stk, ctx, opt, Some(doc)))
						.await
						.catch_return()
						.is_ok_and(|x| x.is_truthy())
					{
						return Err(IgnoreError::Ignore);
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}

#[derive(Clone, Debug)]
pub(crate) struct DefaultBroker {
	sender: Sender<RoutedNotification>,
	delivery: Arc<dyn MessageBroker>,
}

impl DefaultBroker {
	pub(crate) fn new(
		sender: Sender<RoutedNotification>,
		delivery: Arc<dyn MessageBroker>,
	) -> Arc<Self> {
		Arc::new(Self {
			sender,
			delivery,
		})
	}
}
impl MessageBroker for DefaultBroker {
	fn should_emit(&self, node_id: [u8; 16], target_node: [u8; 16]) -> Result<bool> {
		self.delivery.should_emit(node_id, target_node)
	}

	fn send(&self, item: RoutedNotification) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
		Box::pin(async move {
			// If there is an error, we can just ignore it,
			// as it means that the channel was closed.
			let _ = self.sender.send(item).await;
		})
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use anyhow::Result;
	use chrono::Utc;

	use crate::catalog::providers::CatalogProvider;
	use crate::channel::Receiver;
	use crate::dbs::{Capabilities, Session};
	use crate::kvs::Datastore;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::types::{
		PublicAction, PublicNotification, PublicRecordId, PublicRecordIdKey, PublicValue,
	};

	async fn new_ds_with_broker() -> Result<(Receiver<PublicNotification>, Datastore)> {
		let (send, recv) = crate::channel::bounded(1000);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_auth(false)
			.with_notify(send)
			.build_with_path("memory")
			.await?;
		Ok((recv, ds))
	}

	async fn setup_ns_db_table(ds: &Datastore, ns: &str, db: &str, tb: &str) {
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();
		let ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(&format!("DEFINE TABLE {tb}"), &ses, None).await.unwrap();
	}

	fn record_user_session(ns: &str, db: &str, user_key: &str) -> Session {
		Session::for_record(
			ns,
			db,
			"user",
			PublicValue::RecordId(PublicRecordId {
				table: "user".to_string().into(),
				key: PublicRecordIdKey::String(user_key.to_string()),
			}),
		)
		.with_rt(true)
	}

	/// SECURITY (#101): the trusted LIVE `$value` / `$before` / `$after` /
	/// `$event` bindings must NOT be shadowed by a user-captured `$value` in
	/// `live_subscription.vars`. A user can `LET $value = { ok: true }`
	/// before registering a LIVE query whose WHERE references `$value.ok`;
	/// if the user binding wins, the WHERE will match every notification
	/// regardless of the real document. The fix adds user vars to the
	/// context BEFORE the trusted bindings so the trusted ones overwrite.
	#[tokio::test]
	async fn test_live_user_value_does_not_shadow_real_document() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_ns_db_table(&ds, ns, db, tb).await;

		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute(
			"LET $value = { ok: true }; LIVE SELECT * FROM person WHERE $value.ok = true",
			&ses,
			None,
		)
		.await
		.unwrap();

		let ses_write = Session::owner().with_ns(ns).with_db(db);
		let mut res = ds
			.execute("CREATE person:42 SET ok = false", &ses_write, None)
			.await
			.expect("execute should not return an Err");
		res.remove(0).result.expect("CREATE should succeed");

		// With the fix the system `$value` (the real document, ok=false) wins, so
		// the WHERE clause evaluates to false and no notification fires.
		let result =
			tokio::time::timeout(tokio::time::Duration::from_millis(300), recv.recv()).await;
		assert!(
			result.is_err(),
			"no notification should fire when the real document does not match the WHERE",
		);
	}

	/// SECURITY: a LIVE query whose originating session has expired via TTL
	/// must not receive notifications after the TTL passes. We set `exp` to
	/// the current integer second so the session is technically still valid
	/// at registration time, then sleep ≥1.1s so the integer second counter
	/// has advanced past `exp`.
	#[tokio::test]
	async fn test_live_expired_session_suppresses_notification() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_ns_db_table(&ds, ns, db, tb).await;

		let mut live_ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		live_ses.exp = Some(Utc::now().timestamp());
		ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live_ses, None).await.unwrap();
		while recv.try_recv().is_ok() {}

		tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

		let owner_ses = Session::owner().with_ns(ns).with_db(db);
		let res = ds.execute(&format!("CREATE {tb}"), &owner_ses, None).await.unwrap();
		assert!(res[0].result.is_ok(), "CREATE must succeed regardless of LIVE query TTL");

		let spurious =
			tokio::time::timeout(tokio::time::Duration::from_millis(200), recv.recv()).await;
		assert!(
			spurious.is_err(),
			"no notification must be sent after the LIVE query's session TTL has expired",
		);
	}

	/// Sanity check companion to the expiry test: a LIVE query with a
	/// non-expiring session receives notifications normally.
	#[tokio::test]
	async fn test_live_active_session_sends_notification() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_ns_db_table(&ds, ns, db, tb).await;

		let live_ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute(&format!("LIVE SELECT * FROM {tb}"), &live_ses, None).await.unwrap();
		while recv.try_recv().is_ok() {}

		let owner_ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(&format!("CREATE {tb}"), &owner_ses, None).await.unwrap();

		let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
			.await
			.expect("notification should arrive within timeout")
			.expect("channel should not be closed");
		assert_eq!(notif.action, PublicAction::Create);
	}

	/// SECURITY (#120): `reduce_to_owned` runs *before* computed-field
	/// evaluation, so a computed field marked `PERMISSIONS FOR select NONE`
	/// would never be touched by the table-side reduction. The CREATE
	/// notification delivered to a record subscriber must not contain the
	/// computed field's value — that's what
	/// `filter_computed_field_permissions` enforces.
	#[tokio::test]
	async fn test_live_create_does_not_leak_restricted_computed_field() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();

		let owner_ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE person PERMISSIONS FOR select FULL; \
			 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE FIELD secret ON person TYPE string; \
			 DEFINE FIELD derived ON person TYPE string \
			     COMPUTED string::concat('derived_', secret) \
			     PERMISSIONS FOR select NONE",
			&owner_ses,
			None,
		)
		.await
		.unwrap();

		let live_ses = record_user_session(ns, db, "alice");
		ds.execute("LIVE SELECT * FROM person", &live_ses, None).await.unwrap();
		while recv.try_recv().is_ok() {}

		ds.execute("CREATE person:1 SET secret = 'shh'", &owner_ses, None).await.unwrap();

		let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
			.await
			.expect("notification should arrive within timeout")
			.expect("channel should not be closed");
		assert_eq!(notif.action, PublicAction::Create);
		let PublicValue::Object(obj) = notif.result else {
			panic!("CREATE result should be an object, got: {:?}", notif.result);
		};
		assert!(
			!obj.contains_key("derived"),
			"CREATE notification must not include the restricted computed field; got: {obj:?}",
		);
	}

	/// SECURITY (#120): for DELETE events `lq_compute` uses `&self.initial`
	/// (the pre-delete view) as the payload source; without
	/// `filter_computed_field_permissions` the DELETE notification would
	/// carry the unfiltered computed field from the pre-delete state.
	#[tokio::test]
	async fn test_live_delete_does_not_leak_restricted_computed_field() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();

		let owner_ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE person PERMISSIONS FOR select FULL; \
			 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE FIELD secret ON person TYPE string; \
			 DEFINE FIELD derived ON person TYPE string \
			     COMPUTED string::concat('derived_', secret) \
			     PERMISSIONS FOR select NONE",
			&owner_ses,
			None,
		)
		.await
		.unwrap();
		ds.execute("CREATE person:1 SET secret = 'shh'", &owner_ses, None).await.unwrap();

		let live_ses = record_user_session(ns, db, "alice");
		ds.execute("LIVE SELECT * FROM person", &live_ses, None).await.unwrap();
		while recv.try_recv().is_ok() {}

		ds.execute("DELETE person:1", &owner_ses, None).await.unwrap();

		let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
			.await
			.expect("notification should arrive within timeout")
			.expect("channel should not be closed");
		assert_eq!(notif.action, PublicAction::Delete);
		let PublicValue::Object(obj) = notif.result else {
			panic!("DELETE result should be an object, got: {:?}", notif.result);
		};
		assert!(
			!obj.contains_key("derived"),
			"DELETE notification must not include the restricted computed field; got: {obj:?}",
		);
	}

	/// SECURITY (#120): on `LIVE SELECT DIFF`, the LHS of the patch
	/// computation must be prepared the same way as the RHS (reduce +
	/// compute + filter). Otherwise a `Remove /derived` or
	/// `Replace /derived` op would leak the restricted computed field's
	/// existence or value through the diff payload.
	#[tokio::test]
	async fn test_live_diff_does_not_leak_restricted_computed_field() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();

		let owner_ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE person PERMISSIONS FOR select FULL; \
			 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE FIELD name ON person TYPE string; \
			 DEFINE FIELD derived ON person TYPE string \
			     COMPUTED string::concat('derived_', name) \
			     PERMISSIONS FOR select NONE",
			&owner_ses,
			None,
		)
		.await
		.unwrap();

		// Pre-create so the subsequent UPDATE produces an UPDATE notification
		// (not a CREATE) and exercises the LHS filtering path.
		ds.execute("CREATE person:1 SET name = 'foo'", &owner_ses, None).await.unwrap();

		let live_ses = record_user_session(ns, db, "alice");
		ds.execute("LIVE SELECT DIFF FROM person", &live_ses, None).await.unwrap();
		while recv.try_recv().is_ok() {}

		ds.execute("UPDATE person:1 SET name = 'bar'", &owner_ses, None).await.unwrap();

		let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
			.await
			.expect("notification should arrive within timeout")
			.expect("channel should not be closed");
		let PublicValue::Array(ops) = notif.result else {
			panic!("DIFF result should be a Value::Array, got: {:?}", notif.result);
		};
		for op in ops.iter() {
			let PublicValue::Object(obj) = op else {
				continue;
			};
			let path = obj.get("path");
			let targets_derived = path == Some(&PublicValue::String("/derived".to_string()));
			assert!(
				!targets_derived,
				"DIFF UPDATE must not reference the restricted computed field; ops: {ops:?}",
			);
		}
	}

	/// SECURITY (#120): conditional (`Specific`) field permissions on
	/// computed fields must also be enforced on LIVE delivery. A subscriber
	/// whose record satisfies the predicate sees the computed field; one
	/// whose record does not satisfy it must NOT see it.
	#[tokio::test]
	async fn test_live_conditional_permission_filters_computed_field() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();

		let owner_ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE doc PERMISSIONS FOR select FULL; \
			 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE FIELD owner ON doc TYPE record<user>; \
			 DEFINE FIELD name ON doc TYPE string; \
			 DEFINE FIELD mine ON doc TYPE string \
			     COMPUTED string::concat('hi_', name) \
			     PERMISSIONS FOR select WHERE owner = $auth",
			&owner_ses,
			None,
		)
		.await
		.unwrap();

		let live_ses = record_user_session(ns, db, "alice");
		ds.execute("LIVE SELECT * FROM doc", &live_ses, None).await.unwrap();
		while recv.try_recv().is_ok() {}

		// Record owned by the subscriber → `mine` is visible.
		ds.execute("CREATE doc:1 SET owner = user:alice, name = 'one'", &owner_ses, None)
			.await
			.unwrap();
		let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
			.await
			.expect("notification should arrive within timeout")
			.expect("channel should not be closed");
		let PublicValue::Object(obj) = notif.result else {
			panic!("CREATE result should be an object, got: {:?}", notif.result);
		};
		assert!(
			obj.contains_key("mine"),
			"CREATE for own record must include the conditional computed field; got: {obj:?}",
		);

		// Record owned by someone else → `mine` is hidden.
		ds.execute("CREATE doc:2 SET owner = user:bob, name = 'two'", &owner_ses, None)
			.await
			.unwrap();
		let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
			.await
			.expect("notification should arrive within timeout")
			.expect("channel should not be closed");
		let PublicValue::Object(obj) = notif.result else {
			panic!("CREATE result should be an object, got: {:?}", notif.result);
		};
		assert!(
			!obj.contains_key("mine"),
			"CREATE for someone else's record must NOT include the conditional computed field; got: {obj:?}",
		);
	}

	/// SECURITY: on `LIVE SELECT DIFF` with a non-Owner subscriber, no
	/// `Remove` op should appear for a stored field the subscriber cannot
	/// `SELECT`. Before the fix, `self.initial` (LHS of the diff) was not
	/// reduced under the subscriber's auth, so any field present in the
	/// initial but absent from the reduced RHS would surface as a spurious
	/// `Remove` op — leaking the field name to a subscriber with no SELECT
	/// permission on it.
	#[tokio::test]
	async fn test_live_diff_does_not_leak_restricted_field_name() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();

		let owner_ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE person PERMISSIONS FOR select FULL; \
			 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE FIELD name ON person TYPE string; \
			 DEFINE FIELD secret ON person TYPE string PERMISSIONS FOR select WHERE false",
			&owner_ses,
			None,
		)
		.await
		.unwrap();

		// Pre-create so the subsequent UPDATE is a diff rather than a CREATE.
		ds.execute("CREATE person:50 SET name = 'foo', secret = 'shh'", &owner_ses, None)
			.await
			.unwrap();

		let live_ses = record_user_session(ns, db, "alice");
		ds.execute("LIVE SELECT DIFF FROM person", &live_ses, None).await.unwrap();
		while recv.try_recv().is_ok() {}

		ds.execute("UPDATE person:50 SET name = 'bar'", &owner_ses, None).await.unwrap();

		let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
			.await
			.expect("notification should arrive within timeout")
			.expect("channel should not be closed");
		let PublicValue::Array(ops) = notif.result else {
			panic!("DIFF result should be a Value::Array, got: {:?}", notif.result);
		};
		for op in ops.iter() {
			let PublicValue::Object(obj) = op else {
				continue;
			};
			let is_remove = obj.get("op") == Some(&PublicValue::String("remove".to_string()));
			let targets_secret =
				obj.get("path") == Some(&PublicValue::String("/secret".to_string()));
			assert!(
				!(is_remove && targets_secret),
				"DIFF UPDATE must not leak a Remove op for the restricted /secret field; ops: {ops:?}",
			);
		}
	}

	/// CONTRACT (defense in depth): the DELETE arm of `LIVE SELECT DIFF`
	/// must not emit patch ops that reference fields the subscriber has
	/// no SELECT permission for. With today's `Value::diff` semantics
	/// (`Object → None` collapses to a single `Replace { path: "" }`)
	/// this is already true regardless of which source is diffed, but
	/// `lq_compute` uses the prepared view (`doc`) so that a future
	/// change to per-field diff semantics inherits the LIVE owner's
	/// permissions automatically. This test pins the invariant.
	#[tokio::test]
	async fn test_live_diff_delete_does_not_leak_restricted_field_name() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();

		let owner_ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE person PERMISSIONS FOR select FULL; \
			 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE FIELD name ON person TYPE string; \
			 DEFINE FIELD secret ON person TYPE string PERMISSIONS FOR select WHERE false; \
			 DEFINE FIELD derived ON person TYPE string \
			     COMPUTED string::concat('derived_', name) \
			     PERMISSIONS FOR select NONE",
			&owner_ses,
			None,
		)
		.await
		.unwrap();

		ds.execute("CREATE person:51 SET name = 'foo', secret = 'shh'", &owner_ses, None)
			.await
			.unwrap();

		let live_ses = record_user_session(ns, db, "alice");
		ds.execute("LIVE SELECT DIFF FROM person", &live_ses, None).await.unwrap();
		while recv.try_recv().is_ok() {}

		ds.execute("DELETE person:51", &owner_ses, None).await.unwrap();

		let notif = tokio::time::timeout(tokio::time::Duration::from_millis(500), recv.recv())
			.await
			.expect("notification should arrive within timeout")
			.expect("channel should not be closed");
		assert_eq!(notif.action, PublicAction::Delete);
		let PublicValue::Array(ops) = notif.result else {
			panic!("DIFF result should be a Value::Array, got: {:?}", notif.result);
		};
		for op in ops.iter() {
			let PublicValue::Object(obj) = op else {
				continue;
			};
			let path = obj.get("path");
			let targets_secret = path == Some(&PublicValue::String("/secret".to_string()));
			let targets_derived = path == Some(&PublicValue::String("/derived".to_string()));
			assert!(
				!targets_secret,
				"DIFF DELETE must not reference the restricted /secret field; ops: {ops:?}",
			);
			assert!(
				!targets_derived,
				"DIFF DELETE must not reference the restricted /derived computed field; ops: {ops:?}",
			);
		}
	}

	/// ISOLATION: a LIVE query whose WHERE clause errors per-document
	/// (`string::len(name)` when `name` is missing) must not abort the
	/// triggering CREATE. PR #214 chose silent ignore (no Action::Error
	/// notification), so we just confirm CREATE succeeds and no
	/// notification fires.
	#[tokio::test]
	async fn test_live_where_doc_dependent_error_does_not_abort_create() {
		let (recv, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_ns_db_table(&ds, ns, db, tb).await;

		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM person WHERE string::len(name) > 3", &ses, None)
			.await
			.unwrap();

		let ses_write = Session::owner().with_ns(ns).with_db(db);
		let mut res = ds
			.execute("CREATE person:1", &ses_write, None)
			.await
			.expect("execute should not return an Err");
		res.remove(0).result.expect("CREATE should succeed, not be aborted by LIVE WHERE error");

		let spurious =
			tokio::time::timeout(tokio::time::Duration::from_millis(200), recv.recv()).await;
		assert!(
			spurious.is_err(),
			"per-PR-214 design: WHERE errors silently skip the notification, no Action::Error",
		);
	}

	/// ISOLATION: a LIVE query whose SELECT projection always errors
	/// (`string::len(NONE)`) must not abort the triggering CREATE.
	#[tokio::test]
	async fn test_live_projection_type_error_does_not_abort_create() {
		let (_, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_ns_db_table(&ds, ns, db, tb).await;

		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT string::len(NONE) FROM person", &ses, None).await.unwrap();

		let ses_write = Session::owner().with_ns(ns).with_db(db);
		let mut res = ds
			.execute("CREATE person:2", &ses_write, None)
			.await
			.expect("execute should not return an Err");
		res.remove(0)
			.result
			.expect("CREATE should succeed, not be aborted by LIVE projection error");
	}

	/// ISOLATION: a LIVE query with a FETCH clause that always errors
	/// (`type::field(NONE)`) must not abort subsequent writes. Before the
	/// fix, this path was unprotected (raw `?` propagation).
	#[tokio::test]
	async fn test_live_fetch_error_does_not_abort_create() {
		let (_, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_ns_db_table(&ds, ns, db, tb).await;

		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM person FETCH type::field(NONE)", &ses, None).await.unwrap();

		let ses_write = Session::owner().with_ns(ns).with_db(db);
		let mut res = ds
			.execute("CREATE person:12", &ses_write, None)
			.await
			.expect("execute should not return an Err");
		res.remove(0).result.expect("CREATE should succeed despite invalid FETCH expression");
	}

	/// ISOLATION: a LIVE subscriber whose per-field SELECT permission
	/// always errors must not abort writes. `reduce_to_owned` runs under
	/// the LIVE owner's auth (here a record-access user), so an erroring
	/// field permission only affects this subscription — not the writer.
	#[tokio::test]
	async fn test_live_field_permission_error_does_not_abort_create() {
		let (_, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db) = ("test", "test");
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.ensure_ns_db(None, ns, db).await.unwrap();
		tx.commit().await.unwrap();

		let owner_ses = Session::owner().with_ns(ns).with_db(db);
		ds.execute(
			"DEFINE TABLE person; \
			 DEFINE ACCESS user ON DATABASE TYPE RECORD; \
			 DEFINE FIELD secret ON person PERMISSIONS FOR select WHERE string::len(NONE)",
			&owner_ses,
			None,
		)
		.await
		.unwrap();

		let live_ses = record_user_session(ns, db, "alice");
		ds.execute("LIVE SELECT * FROM person", &live_ses, None).await.unwrap();

		let ses_write = Session::owner().with_ns(ns).with_db(db);
		let mut res = ds
			.execute("CREATE person:20 SET secret = 'shh'", &ses_write, None)
			.await
			.expect("execute should not return an Err");
		res.remove(0)
			.result
			.expect("CREATE should succeed despite erroring field SELECT permission");
	}

	/// ISOLATION: multiple LIVE queries with document-dependent erroring
	/// WHERE clauses on the same table must not prevent CREATE, UPDATE, or
	/// DELETE from succeeding. This is the multi-subscriber stress test.
	#[tokio::test]
	async fn test_multiple_erroring_live_queries_do_not_abort_writes() {
		let (_, ds) = new_ds_with_broker().await.unwrap();
		let (ns, db, tb) = ("test", "test", "person");
		setup_ns_db_table(&ds, ns, db, tb).await;

		let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
		ds.execute("LIVE SELECT * FROM person WHERE string::len(name) > 3", &ses, None)
			.await
			.unwrap();
		ds.execute("LIVE SELECT * FROM person WHERE string::uppercase(name) = 'ALICE'", &ses, None)
			.await
			.unwrap();

		let ses_write = Session::owner().with_ns(ns).with_db(db);

		let mut res =
			ds.execute("CREATE person:3", &ses_write, None).await.expect("execute should not Err");
		res.remove(0).result.expect("CREATE should succeed");

		let mut res = ds
			.execute("UPDATE person:3 SET name = 'Alice'", &ses_write, None)
			.await
			.expect("execute should not Err");
		res.remove(0).result.expect("UPDATE should succeed");

		let mut res =
			ds.execute("DELETE person:3", &ses_write, None).await.expect("execute should not Err");
		res.remove(0).result.expect("DELETE should succeed");
	}
}
