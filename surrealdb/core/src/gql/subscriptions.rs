use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	FieldValue, InputValue, Subscription, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value as GqlValue};
use async_stream::try_stream;
use surrealdb_types::ToSql;
use surrealdb_types::{Action as PublicAction, Notification as PublicNotification};
use tokio::sync::broadcast::{Receiver, Sender};
use uuid::Uuid;

use super::error::{resolver_error, GqlError};
use super::tables::{filter_name_from_table, parse_filter_arg, CachedRecord};
use super::utils::execute_plan;
use super::utils::GqlValueUtils;
use crate::catalog::{FieldDefinition, TableDefinition};
use crate::dbs::Session;
use crate::expr::field::Selector;
use crate::expr::plan::TopLevelExpr;
use crate::expr::statements::{KillStatement, LiveFields, LiveStatement};
use crate::expr::{
	BinaryOperator, Cond, Expr, Fetch, Fetchs, Field, Fields, Idiom, Literal, LogicalPlan, Part,
};
use crate::kvs::Datastore;
use crate::val::{RecordId, TableName, Value};

/// Context for GraphQL Subscription resolvers
pub(crate) type NotificationBroadcaster = Arc<Sender<PublicNotification>>;

pub(crate) fn process_subscriptions(
	tbs: &[TableDefinition],
	table_fields: &HashMap<String, Arc<[FieldDefinition]>>,
) -> Option<Subscription> {
	if tbs.is_empty() {
		return None;
	}

	let mut subscription = Subscription::new("Subscription");
	for tb in tbs {
		let fds = table_fields
			.get(tb.name.as_str())
			.cloned()
			.unwrap_or_else(|| Arc::<[FieldDefinition]>::from([]));
		subscription = subscription.field(make_table_subscription_field(tb, fds));
	}

	Some(subscription)
}

fn make_table_subscription_field(
	tb: &TableDefinition,
	fds: Arc<[FieldDefinition]>,
) -> SubscriptionField {
	let tb_name = tb.name.clone();
	let tb_name_str = tb_name.clone().into_string();
	let table_filter_name = filter_name_from_table(&tb_name);
	let selectable_fields = selectable_top_level_fields(&fds);

	SubscriptionField::new(tb_name_str.clone(), TypeRef::named(&tb_name_str), move |ctx| {
		let tb_name = tb_name.clone();
		let fds = fds.clone();
		let selectable_fields = selectable_fields.clone();
		SubscriptionFieldFuture::new(async move {
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;
			let broadcaster = ctx.data::<NotificationBroadcaster>().map_err(|_| {
				async_graphql::Error::new(
					"GraphQL subscriptions are not enabled on this server node",
				)
			})?;
			let args = ctx.args.as_index_map();

			let live_sess = sess.as_ref().clone().with_rt(true);
			let fields = projected_live_fields(&ctx, &selectable_fields);
			let cond = parse_subscription_cond(args, &fds, &tb_name)?;
			let fetch = parse_fetch_arg(args)?;
			let live_id =
				start_table_live_query(&ds, &live_sess, &tb_name, fields, cond, fetch).await?;
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
	.argument(InputValue::new("id", TypeRef::named(TypeRef::ID)))
	.argument(InputValue::new("filter", TypeRef::named(&table_filter_name)))
	.argument(InputValue::new("where", TypeRef::named(&table_filter_name)))
	.argument(InputValue::new("fetch", TypeRef::named_nn_list(TypeRef::STRING)))
}

fn selectable_top_level_fields(fds: &[FieldDefinition]) -> HashSet<String> {
	let mut out = HashSet::new();
	out.insert("id".to_string());
	for fd in fds {
		if fd.name.0.len() != 1 {
			continue;
		}
		if let Some(Part::Field(name)) = fd.name.0.first() {
			out.insert(name.clone());
		}
	}
	out
}

fn projected_live_fields(
	ctx: &async_graphql::dynamic::ResolverContext<'_>,
	selectable_fields: &HashSet<String>,
) -> LiveFields {
	let mut selected = Vec::new();
	for field in ctx.field().selection_set() {
		let name = field.name();
		if name.starts_with("__") {
			continue;
		}
		if selectable_fields.contains(name) {
			selected.push(name.to_string());
		}
	}
	if !selected.iter().any(|x| x == "id") {
		selected.push("id".to_string());
	}
	selected.sort_unstable();
	selected.dedup();
	let projected = selected
		.into_iter()
		.map(|name| {
			Field::Single(Selector {
				expr: Expr::Idiom(Idiom::field(name)),
				alias: None,
			})
		})
		.collect();
	LiveFields::Select(Fields::Select(projected))
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

fn parse_subscription_cond(
	args: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	tb_name: &TableName,
) -> Result<Option<Cond>, async_graphql::Error> {
	let id_cond = parse_id_cond(args, tb_name)?;
	let where_cond = parse_filter_arg(args, fds, tb_name.as_str())
		.map_err(|e| async_graphql::Error::new(e.to_string()))?;
	Ok(combine_cond(id_cond, where_cond))
}

fn parse_id_cond(
	args: &IndexMap<Name, GqlValue>,
	tb_name: &TableName,
) -> Result<Option<Cond>, async_graphql::Error> {
	let Some(id_val) = args.get("id") else {
		return Ok(None);
	};
	if matches!(id_val, GqlValue::Null) {
		return Ok(None);
	}
	let Some(id_str) = id_val.as_string() else {
		return Err(async_graphql::Error::new("id must be a record ID string"));
	};
	let rid: RecordId = crate::syn::record_id(&id_str)
		.map_err(|_| async_graphql::Error::new(format!("Invalid record ID format: {id_str}")))?
		.into();
	if &rid.table != tb_name {
		return Err(async_graphql::Error::new(format!(
			"Record ID `{id_str}` does not belong to table `{tb_name}`"
		)));
	}
	Ok(Some(Cond(Expr::Binary {
		left: Box::new(Expr::Idiom(Idiom::field("id".to_string()))),
		op: BinaryOperator::Equal,
		right: Box::new(Value::RecordId(rid).into_literal()),
	})))
}

fn combine_cond(left: Option<Cond>, right: Option<Cond>) -> Option<Cond> {
	match (left, right) {
		(Some(left), Some(right)) => Some(Cond(Expr::Binary {
			left: Box::new(left.0),
			op: BinaryOperator::And,
			right: Box::new(right.0),
		})),
		(Some(left), None) => Some(left),
		(None, Some(right)) => Some(right),
		(None, None) => None,
	}
}

fn parse_fetch_arg(
	args: &IndexMap<Name, GqlValue>,
) -> Result<Option<Fetchs>, async_graphql::Error> {
	let Some(fetch_value) = args.get("fetch") else {
		return Ok(None);
	};
	if matches!(fetch_value, GqlValue::Null) {
		return Ok(None);
	}

	let values: Vec<String> = match fetch_value {
		GqlValue::List(items) => {
			let mut out = Vec::with_capacity(items.len());
			for item in items {
				let Some(path) = item.as_string() else {
					return Err(async_graphql::Error::new("fetch must be a list of strings"));
				};
				out.push(path);
			}
			out
		}
		GqlValue::String(path) => vec![path.clone()],
		_ => {
			return Err(async_graphql::Error::new("fetch must be a list of strings"));
		}
	};

	if values.is_empty() {
		return Ok(None);
	}

	let mut fetches = Vec::with_capacity(values.len());
	for path in values {
		let idiom = crate::syn::idiom(&path)
			.map_err(|_| async_graphql::Error::new(format!("Invalid fetch path: {path}")))?;
		fetches.push(Fetch(Expr::Idiom(idiom.into())));
	}

	Ok(Some(Fetchs::new(fetches)))
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
	fields: LiveFields,
	cond: Option<Cond>,
	fetch: Option<Fetchs>,
) -> Result<Uuid, async_graphql::Error> {
	let stmt = LiveStatement {
		id: Uuid::new_v4(),
		node: Uuid::new_v4(),
		fields,
		what: Expr::Table(table.clone()),
		cond,
		fetch,
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
