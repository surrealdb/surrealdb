use std::sync::Arc;

use async_graphql::dynamic::{
	FieldValue, Subscription, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_stream::try_stream;
use surrealdb_types::ToSql;
use surrealdb_types::{Action as PublicAction, Notification as PublicNotification};
use tokio::sync::broadcast::{Receiver, Sender};
use uuid::Uuid;

use super::error::{GqlError, resolver_error};
use super::tables::CachedRecord;
use super::utils::execute_plan;
use crate::catalog::TableDefinition;
use crate::dbs::Session;
use crate::expr::plan::TopLevelExpr;
use crate::expr::statements::{KillStatement, LiveFields, LiveStatement};
use crate::expr::{Expr, Fields, Literal, LogicalPlan};
use crate::kvs::Datastore;
use crate::val::{RecordId, TableName, Value};

/// Context for GraphQL Subscription resolvers
pub(crate) type NotificationBroadcaster = Arc<Sender<PublicNotification>>;

pub(crate) fn process_subscriptions(tbs: &[TableDefinition]) -> Option<Subscription> {
	if tbs.is_empty() {
		return None;
	}

	let mut subscription = Subscription::new("Subscription");
	for tb in tbs {
		subscription = subscription.field(make_table_subscription_field(tb));
	}

	Some(subscription)
}

fn make_table_subscription_field(tb: &TableDefinition) -> SubscriptionField {
	let tb_name = tb.name.clone();
	let tb_name_str = tb_name.clone().into_string();

	SubscriptionField::new(tb_name_str.clone(), TypeRef::named(&tb_name_str), move |ctx| {
		let tb_name = tb_name.clone();
		SubscriptionFieldFuture::new(async move {
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;
			let broadcaster = ctx.data::<NotificationBroadcaster>().map_err(|_| {
				async_graphql::Error::new(
					"GraphQL subscriptions are not enabled on this server node",
				)
			})?;

			let live_sess = sess.as_ref().clone().with_rt(true);
			let live_id = start_table_live_query(&ds, &live_sess, &tb_name).await?;
			let mut receiver = broadcaster.subscribe();
			let cleanup = LiveQueryCleanup::new(ds.clone(), live_sess, live_id);

			Ok(try_stream! {
				let _cleanup = cleanup;
				loop {
					let notification = recv_notification(&mut receiver).await?;
					if notification.id.into_inner() != live_id {
						continue;
					}
					if matches!(notification.action, PublicAction::Killed) {
						break;
					}
					if let Some(value) = notification_to_field_value(notification) {
						yield value;
					}
				}
			})
		})
	})
	.description(format!("LIVE query notifications for `{}`", tb.name))
}

async fn recv_notification(
	receiver: &mut Receiver<PublicNotification>,
) -> Result<PublicNotification, async_graphql::Error> {
	loop {
		match receiver.recv().await {
			Ok(n) => return Ok(n),
			Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
			Err(tokio::sync::broadcast::error::RecvError::Closed) => {
				return Err(async_graphql::Error::new(
					"Live notification channel closed unexpectedly",
				));
			}
		}
	}
}

fn notification_to_field_value(notification: PublicNotification) -> Option<FieldValue<'static>> {
	let record: Value = notification.record.into();
	let result: Value = notification.result.into();

	let Value::Object(obj) = result else {
		return None;
	};

	let rid = extract_record_id(&obj, &record)?;
	Some(FieldValue::owned_any(CachedRecord {
		rid,
		version: None,
		data: obj,
	}))
}

fn extract_record_id(obj: &crate::val::Object, fallback: &Value) -> Option<RecordId> {
	match obj.get("id") {
		Some(Value::RecordId(rid)) => Some(rid.clone()),
		_ => match fallback {
			Value::RecordId(rid) => Some(rid.clone()),
			_ => None,
		},
	}
}

async fn start_table_live_query(
	ds: &Datastore,
	sess: &Session,
	table: &TableName,
) -> Result<Uuid, async_graphql::Error> {
	let stmt = LiveStatement {
		id: Uuid::new_v4(),
		node: Uuid::new_v4(),
		fields: LiveFields::Select(Fields::all()),
		what: Expr::Table(table.clone()),
		cond: None,
		fetch: None,
	};
	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Live(Box::new(stmt))],
	};
	let res = execute_plan(ds, sess, plan).await?;

	match res {
		Value::Uuid(id) => Ok(id.into()),
		value => {
			Err(resolver_error(format!("LIVE query did not return a UUID, got {}", value.to_sql()))
				.into())
		}
	}
}

async fn kill_live_query(ds: &Datastore, sess: &Session, live_id: Uuid) -> Result<(), GqlError> {
	let stmt = KillStatement {
		id: Expr::Literal(Literal::Uuid(live_id.into())),
	};
	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Kill(stmt)],
	};
	let _ = execute_plan(ds, sess, plan).await?;
	Ok(())
}

struct LiveQueryCleanup {
	ds: Arc<Datastore>,
	sess: Session,
	live_id: Uuid,
}

impl LiveQueryCleanup {
	fn new(ds: Arc<Datastore>, sess: Session, live_id: Uuid) -> Self {
		Self {
			ds,
			sess,
			live_id,
		}
	}
}

impl Drop for LiveQueryCleanup {
	fn drop(&mut self) {
		let ds = self.ds.clone();
		let sess = self.sess.clone();
		let live_id = self.live_id;
		tokio::spawn(async move {
			if let Err(err) = kill_live_query(&ds, &sess, live_id).await {
				trace!(?err, ?live_id, "failed to cleanup GraphQL live query");
			}
		});
	}
}
