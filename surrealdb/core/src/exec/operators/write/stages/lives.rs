//! The subscribers a completed write notifies.
//!
//! One stage, and one notification per subscription on the table, each
//! computed as, and for, a different principal. The computation is the
//! subscription's unit, which the write's context holds one entry for per
//! subscription (`crate::exec::vm::live`), run by
//! [`crate::exec::vm::notify_unit`]; this module holds the stage and the parts
//! of a notification that are not the unit's program: who is served, the frame
//! the unit runs in, and the envelope it is delivered in.
//!
//! # Subscriber isolation is a type here, not a convention
//!
//! A subscription's notification is a [`Notify`], not a `Result`. A
//! per-subscription failure — a `WHERE` that throws, a projection that cannot
//! be evaluated, a `FETCH` path that resolves to nothing, a value that cannot
//! be converted to the wire format — belongs to that subscription and not to
//! the statement that triggered it, so it has nowhere to go but a `debug!` and
//! a [`Notify::Skip`].
//!
//! Three failures do leave the loop, and none is one subscription's. The
//! missing record id is per record and is answered once, before any
//! subscription is considered. A context whose units do not match its
//! subscriptions is an invariant the write broke. The broker's `should_emit`
//! is asked only after the payload is built, so a write fails only for
//! subscriptions that have something to deliver.
//!
//! # The two-phase bind
//!
//! `$before` and `$after` are bound twice ([`subscriber_frame`]), and the
//! order is the security property. Phase one binds the *unreduced* records,
//! because the only expressions evaluated under it are the schema owner's own
//! field-permission predicates and `COMPUTED` bodies — a predicate reading the
//! view its own clause is about to cut from would decide on a document with
//! the field already missing. Phase two rebinds to the *reduced* views before
//! any subscriber-authored expression runs, so a subscriber denied a field can
//! neither read it through `LIVE SELECT $after.<field>` nor use
//! `WHERE $after.<field> = …` as a boolean oracle.
//!
//! # Sequentially
//!
//! Subscriptions run in order, not concurrently. Running them in order makes
//! delivery order deterministic and contends on nothing — the futures would
//! share one transaction handle either way.

// Entry points the IR lowering emits; no Rust call site reaches them directly.
#![allow(dead_code)]

use std::sync::Arc;

use chrono::Utc;

use crate::catalog::SubscriptionDefinition;
use crate::dbs::RoutedNotification;
use crate::doc::Action;
use crate::exec::ExecutionContext;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::expr::ControlFlow;
use crate::expr::paths::{AC, ID, RD, TK};
use crate::types::{PublicAction, PublicNotification};
use crate::val::{RecordId, Value, convert_value_to_public_value};

/// Send `notification` to `subscription`'s node, when this node emits for it.
///
/// The broker is asked only once the payload is built. Its error is the one
/// delivery failure that aborts the write, so it can only abort the write for
/// a subscription that has something to deliver.
pub(crate) async fn deliver(
	broker: &Arc<dyn crate::dbs::MessageBroker>,
	ctx: &WriteCtx,
	subscription: &SubscriptionDefinition,
	notification: PublicNotification,
) -> Result<(), ControlFlow> {
	let emit = broker
		.should_emit(*ctx.exec().root().ctx.node_id().as_bytes(), *subscription.node.as_bytes())
		.map_err(ControlFlow::Err)?;
	if emit {
		broker.send(RoutedNotification::new(subscription.node, notification)).await;
	}
	Ok(())
}

/// Whether a subscription is told about this record.
///
/// Deliberately not a `Result`: see the module documentation.
pub(crate) enum Notify {
	/// Nothing is sent, and the write is unaffected.
	Skip,
	/// The notification this subscription receives.
	Deliver(PublicNotification),
}

/// The record's two images and what happened to it, owned so nothing borrows
/// the row across an await.
#[derive(Clone)]
pub(crate) struct Images {
	/// The record before the write.
	pub(crate) initial: Arc<Value>,
	/// The record after it, which a delete has already blanked.
	pub(crate) current: Arc<Value>,
	/// What the order did to it.
	pub(crate) action: Action,
}

impl Images {
	/// The image the notification is about: what was there for a delete, what
	/// is there otherwise.
	pub(crate) fn subject(&self) -> &Arc<Value> {
		match self.action {
			Action::Delete => &self.initial,
			Action::Create | Action::Update => &self.current,
		}
	}

	/// The opposite image, which is absent for exactly a create and a delete.
	pub(crate) fn counterpart(&self) -> &Arc<Value> {
		match self.action {
			Action::Delete => &self.current,
			Action::Create | Action::Update => &self.initial,
		}
	}

	/// What the subscriber is told happened.
	pub(crate) fn public_action(&self) -> PublicAction {
		match self.action {
			Action::Delete => PublicAction::Delete,
			Action::Create => PublicAction::Create,
			Action::Update => PublicAction::Update,
		}
	}

	/// `$event`.
	pub(crate) fn event(&self) -> Value {
		match self.action {
			Action::Delete => Value::from("DELETE"),
			Action::Create => Value::from("CREATE"),
			Action::Update => Value::from("UPDATE"),
		}
	}
}

/// What a subscription holds that it must hold to be served at all: its
/// clauses as compiled from its stored text, the session it was registered
/// in, and the principal it runs as.
pub(crate) struct Subscriber<'a> {
	pub(crate) compiled: &'a crate::catalog::CompiledSubscription,
	pub(crate) session: &'a Value,
	pub(crate) auth: Arc<crate::iam::Auth>,
}

/// The parts of `subscription` it is served with, or `None` for one that is
/// served nothing: its stored text no longer compiles — it stays in the list
/// so teardown and `INFO` can see it — or it holds no session or principal,
/// or its session has expired.
pub(crate) fn subscriber(subscription: &SubscriptionDefinition) -> Option<Subscriber<'_>> {
	let compiled = subscription.query.compiled()?;
	let session = subscription.session.as_ref()?;
	// `session.exp` is the unix timestamp `DURATION FOR SESSION` set; absent
	// means no expiry. Read through `to_int` so a client that serialised it as
	// a float still counts as an expiry.
	if let Value::Object(object) = session
		&& let Some(Value::Number(expiry)) = object.get("exp")
		&& Utc::now().timestamp() > (*expiry).to_int()
	{
		return None;
	}
	let auth = Arc::new(subscription.auth.clone()?);
	Some(Subscriber {
		compiled,
		session,
		auth,
	})
}

/// The notification `subscription` receives about record `rid`, carrying
/// `result`, or a skip when either has no wire representation.
pub(crate) fn envelope(
	subscription: &SubscriptionDefinition,
	session: &Value,
	rid: &RecordId,
	action: PublicAction,
	result: Value,
) -> Notify {
	macro_rules! skip {
		($reason:expr, $err:expr) => {{
			tracing::debug!(
				target: "surrealdb::core::exec::write::lives",
				subscription_id = %subscription.id,
				error = %$err,
				reason = $reason,
				"LIVE notification skipped",
			);
			return Notify::Skip;
		}};
	}
	// A session id is a uuid, written either as one or as the text of one. Any
	// other value names no session, and a subscriber's own `$session` is
	// whatever the definition stored, so every other variant is reachable.
	let picked = session.pick(ID.as_ref());
	let session_id = if let Value::Uuid(uuid) = picked {
		Some(uuid.into())
	} else if let Value::String(text) = picked {
		text.parse::<crate::val::Uuid>().ok().map(|uuid| uuid.into())
	} else {
		None
	};
	let record = match convert_value_to_public_value(Value::RecordId(rid.clone())) {
		Ok(value) => value,
		Err(err) => skip!("the record id has no wire representation", err),
	};
	let result = match convert_value_to_public_value(result) {
		Ok(value) => value,
		Err(err) => skip!("the result has no wire representation", err),
	};
	Notify::Deliver(PublicNotification::new(
		subscription.id.into(),
		session_id,
		action,
		record,
		result,
	))
}

/// The context a subscription's work runs in, before its principal is applied.
///
/// The shadowing order is load-bearing: the subscription's own captured
/// variables go in first, so the trusted bindings below overwrite a captured
/// variable of the same name rather than the other way round.
/// A subscriber that could shadow `$value`, `$before`, `$after`, `$session`,
/// `$auth`, `$access` or `$token` would be feeding the table's own permission
/// expressions a document of its choosing.
pub(crate) fn subscriber_frame(
	ctx: &WriteCtx,
	subscription: &SubscriptionDefinition,
	session: &Value,
	images: &Images,
	documents: impl FnOnce(&Images) -> (Arc<Value>, Arc<Value>),
) -> ExecutionContext {
	let mut frame = ctx.exec().detached_context();
	frame.add_values(subscription.vars.clone());
	frame.add_value("access", Arc::new(session.pick(AC.as_ref())));
	frame.add_value("auth", Arc::new(session.pick(RD.as_ref())));
	frame.add_value("token", Arc::new(session.pick(TK.as_ref())));
	frame.add_value("session", Arc::new(session.clone()));
	frame.add_value("event", Arc::new(images.event()));
	let (before, after) = documents(images);
	frame.add_value("value", Arc::clone(&after));
	frame.add_value("after", after);
	frame.add_value("before", before);
	ctx.exec().with_new_ctx(frame.freeze())
}

/// The frozen context a frame carries, for handing back to
/// [`ExecutionContext::with_replaced_principal`].
pub(crate) fn deriving_frozen(frame: &ExecutionContext) -> crate::ctx::FrozenContext {
	Arc::clone(frame.ctx())
}
