use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	FieldValue, InputValue, Subscription, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value as GraphqlValue};
use async_stream::try_stream;
use surrealdb_types::{Action as PublicAction, Notification as PublicNotification, ToSql};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::error::{GraphqlError, resolver_error};
use super::tables::{CachedRecord, filter_name_from_table, parse_filter_arg};
use super::utils::{GraphqlValueUtils, execute_plan};
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

/// Routes LIVE query notifications to their specific GraphQL subscribers.
///
/// Each GraphQL subscription registers its live query UUID and receives a
/// dedicated bounded mpsc channel. Notifications are dispatched in O(1) via
/// HashMap lookup. When a subscriber's channel is full the notification is
/// dropped (analogous to the "lagged" behaviour of broadcast channels).
pub struct NotificationRouter {
	routes: RwLock<HashMap<Uuid, mpsc::Sender<PublicNotification>>>,
	channel_capacity: usize,
}

impl NotificationRouter {
	pub fn new(channel_capacity: usize) -> Self {
		Self {
			routes: RwLock::new(HashMap::new()),
			channel_capacity: channel_capacity.max(1),
		}
	}

	fn subscribe(&self, live_id: Uuid) -> mpsc::Receiver<PublicNotification> {
		let (tx, rx) = mpsc::channel(self.channel_capacity);
		self.routes.write().unwrap_or_else(|e| e.into_inner()).insert(live_id, tx);
		rx
	}

	fn unsubscribe(&self, live_id: &Uuid) {
		self.routes.write().unwrap_or_else(|e| e.into_inner()).remove(live_id);
	}

	/// Route a notification to the matching subscriber, if any.
	///
	/// Clones the notification only when a matching subscriber exists.
	/// If the subscriber's channel is full the notification is dropped
	/// rather than blocking the dispatch loop.
	pub fn dispatch(&self, notification: &PublicNotification) {
		let routes = self.routes.read().unwrap_or_else(|e| e.into_inner());
		if let Some(sender) = routes.get(&notification.id) {
			match sender.try_send(notification.clone()) {
				Ok(()) => {}
				Err(mpsc::error::TrySendError::Full(_)) => {
					warn!(
						live_id = %notification.id,
						"GraphQL subscription channel full, notification dropped"
					);
				}
				Err(mpsc::error::TrySendError::Closed(_)) => {
					trace!(
						live_id = %notification.id,
						"GraphQL subscription channel closed, stale route"
					);
				}
			}
		}
	}

	pub fn has_subscribers(&self) -> bool {
		!self.routes.read().unwrap_or_else(|e| e.into_inner()).is_empty()
	}
}

pub(crate) fn process_subscriptions(
	tbs: &[TableDefinition],
	table_fields: &HashMap<TableName, Arc<[FieldDefinition]>>,
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
	// The subscription field and its payload type follow the table's GraphQL
	// name, so a `GRAPHQL_ALIAS` reaches them too (#7453).
	let gql_name: Arc<str> = Arc::from(super::naming::table_base_name(tb));
	let table_filter_name = filter_name_from_table(&gql_name);
	let selectable_fields = Arc::new(selectable_top_level_fields(&fds));

	SubscriptionField::new(gql_name.to_string(), TypeRef::named(gql_name.as_ref()), move |ctx| {
		let tb_name = tb_name.clone();
		let gql_name = Arc::clone(&gql_name);
		let fds = Arc::clone(&fds);
		let selectable_fields = Arc::clone(&selectable_fields);
		SubscriptionFieldFuture::new(async move {
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;
			let router = ctx.data::<Arc<NotificationRouter>>().map_err(|_| {
				async_graphql::Error::new(
					"GraphQL subscriptions are not enabled on this server node",
				)
			})?;
			let args = ctx.args.as_index_map();

			let live_sess = sess.as_ref().clone().with_rt(true);
			let fields = projected_live_fields(&ctx, &selectable_fields);
			let cond = parse_subscription_cond(args, &fds, &tb_name, &gql_name)?;
			let fetch = parse_fetch_arg(args)?;
			let live_id =
				start_table_live_query(ds, &live_sess, &tb_name, fields, cond, fetch).await?;
			let mut receiver = router.subscribe(live_id);
			let cleanup =
				LiveQueryCleanup::new(Arc::clone(ds), live_sess, live_id, Arc::clone(router));

			Ok(try_stream! {
				let _cleanup = cleanup;
				loop {
					let Some(notification) = receiver.recv().await else {
						break;
					};
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

/// The top-level fields a subscription can project, keyed by the name they are
/// selected under in GraphQL and valued by the SurrealQL field the LIVE query
/// has to read.
///
/// The two diverge as soon as a field carries a `GRAPHQL_ALIAS` (#7453), so the
/// mapping has to be built here rather than reconstructed from the selection
/// set. Only single-part idioms are projectable: a nested `parent.child` field
/// is reached by projecting `parent` whole.
fn selectable_top_level_fields(fds: &[FieldDefinition]) -> HashMap<String, String> {
	// Vec<(String, String)> with a linear scan would likely be slightly faster
	// for under ~50 fields. Opting for code clarity over an unknown performance
	// benefit with this cold code.
	let mut out = HashMap::new();
	out.insert("id".to_string(), "id".to_string());
	for fd in fds {
		if fd.name.0.len() != 1 {
			continue;
		}
		if let Some(Part::Field(name)) = fd.name.0.first() {
			out.insert(super::naming::field_graphql_name(fd), name.as_str().to_owned());
		}
	}
	out
}

fn projected_live_fields(
	ctx: &async_graphql::dynamic::ResolverContext<'_>,
	selectable_fields: &HashMap<String, String>,
) -> LiveFields {
	let selected =
		projected_storage_names(ctx.field().selection_set().map(|f| f.name()), selectable_fields);
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

/// Translate a subscription's selection set into the sorted list of SurrealQL
/// field names its LIVE query should project.
///
/// Selections carry *GraphQL* names, so an aliased field has to be mapped back
/// to its storage name before it reaches the projection — otherwise the value
/// is never fetched and the cached notification resolves it as missing (#7453).
/// Names that are not stored columns (introspection, relation fields, which
/// resolve by their own queries) are dropped, and `id` is always projected
/// because the notification payload is keyed by it.
fn projected_storage_names<'a>(
	selection: impl Iterator<Item = &'a str>,
	selectable_fields: &HashMap<String, String>,
) -> Vec<String> {
	let mut selected: Vec<String> = selection
		.filter(|name| !name.starts_with("__"))
		.filter_map(|name| selectable_fields.get(name).cloned())
		.collect();
	if !selected.iter().any(|x| x == "id") {
		selected.push("id".to_string());
	}
	selected.sort_unstable();
	selected.dedup();
	selected
}

fn parse_subscription_cond(
	args: &IndexMap<Name, GraphqlValue>,
	fds: &[FieldDefinition],
	tb_name: &TableName,
	gql_name: &str,
) -> Result<Option<Cond>, async_graphql::Error> {
	let id_cond = parse_id_cond(args, tb_name)?;
	// The `where` argument is typed by `_filter_<gql_name>`, so its enum scope
	// has to be the table's GraphQL name rather than the SurrealQL one (#7453).
	let where_cond = parse_filter_arg(args, fds, gql_name, &[])
		.map_err(|e| async_graphql::Error::new(e.to_string()))?;
	Ok(combine_cond(id_cond, where_cond))
}

fn parse_id_cond(
	args: &IndexMap<Name, GraphqlValue>,
	tb_name: &TableName,
) -> Result<Option<Cond>, async_graphql::Error> {
	let Some(id_val) = args.get("id") else {
		return Ok(None);
	};
	if matches!(id_val, GraphqlValue::Null) {
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
	args: &IndexMap<Name, GraphqlValue>,
) -> Result<Option<Fetchs>, async_graphql::Error> {
	let Some(fetch_value) = args.get("fetch") else {
		return Ok(None);
	};
	if matches!(fetch_value, GraphqlValue::Null) {
		return Ok(None);
	}

	let values: Vec<String> = match fetch_value {
		GraphqlValue::List(items) => {
			let mut out = Vec::with_capacity(items.len());
			for item in items {
				let Some(path) = item.as_string() else {
					return Err(async_graphql::Error::new("fetch must be a list of strings"));
				};
				out.push(path);
			}
			out
		}
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

async fn kill_live_query(
	ds: &Datastore,
	sess: &Session,
	live_id: Uuid,
) -> Result<(), GraphqlError> {
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
	router: Arc<NotificationRouter>,
}

impl LiveQueryCleanup {
	fn new(
		ds: Arc<Datastore>,
		sess: Session,
		live_id: Uuid,
		router: Arc<NotificationRouter>,
	) -> Self {
		Self {
			ds,
			sess,
			live_id,
			router,
		}
	}
}

impl Drop for LiveQueryCleanup {
	fn drop(&mut self) {
		self.router.unsubscribe(&self.live_id);
		let Ok(handle) = tokio::runtime::Handle::try_current() else {
			return;
		};
		let ds = Arc::clone(&self.ds);
		let sess = self.sess.clone();
		let live_id = self.live_id;
		handle.spawn(async move {
			if let Err(err) = kill_live_query(&ds, &sess, live_id).await {
				trace!(?err, ?live_id, "failed to cleanup GraphQL live query");
			}
		});
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A `DEFINE FIELD <name> ON <tb> …` entry with the given `GRAPHQL_ALIAS`.
	fn field(name: &str, alias: Option<&str>) -> FieldDefinition {
		FieldDefinition {
			name: Idiom(vec![Part::Field(name.into())]),
			graphql_alias: alias.map(str::to_owned),
			..Default::default()
		}
	}

	#[test]
	fn selectable_fields_are_keyed_by_the_graphql_name() {
		let map = selectable_top_level_fields(&[
			field("created_at", Some("createdAt")),
			field("label", None),
			// A nested sub-field is not projectable on its own; its parent is.
			FieldDefinition {
				name: Idiom(vec![Part::Field("price".into()), Part::Field("in_euro".into())]),
				..Default::default()
			},
		]);
		assert_eq!(map.get("createdAt").map(String::as_str), Some("created_at"));
		assert_eq!(map.get("label").map(String::as_str), Some("label"));
		assert_eq!(map.get("id").map(String::as_str), Some("id"));
		// The raw name of an aliased field is not selectable — the generated
		// Object type does not expose it under that name either.
		assert!(!map.contains_key("created_at"));
		assert!(!map.contains_key("in_euro"));
	}

	#[test]
	fn an_aliased_selection_projects_its_storage_name() {
		let map = selectable_top_level_fields(&[field("created_at", Some("createdAt"))]);
		// Selecting `createdAt` has to reach `created_at` in the LIVE query, or
		// the notification never carries the value the resolver looks up.
		assert_eq!(
			projected_storage_names(["createdAt"].into_iter(), &map),
			vec!["created_at".to_string(), "id".to_string()]
		);
	}

	#[test]
	fn id_is_always_projected_and_unknown_names_are_dropped() {
		let map = selectable_top_level_fields(&[field("label", None)]);
		assert_eq!(
			projected_storage_names(["__typename", "label", "uses_in", "label"].into_iter(), &map),
			vec!["id".to_string(), "label".to_string()]
		);
		// An explicit `id` selection must not be projected twice.
		assert_eq!(
			projected_storage_names(["id", "label"].into_iter(), &map),
			vec!["id".to_string(), "label".to_string()]
		);
	}
}
