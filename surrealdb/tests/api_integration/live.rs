#![allow(clippy::unwrap_used)]
#![cfg(any(
	feature = "protocol-ws",
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-tikv",
	feature = "kv-surrealkv",
))]

// Tests for running live queries
// Supported by the storage engines and the WS protocol

use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use surrealdb::method::QueryStream;
use surrealdb::opt::{Config, Resource};
use surrealdb::types::{Action, RecordId, SurrealValue, Value, object};
use surrealdb::{Notification, Result, Surreal};
use tokio::sync::RwLock;
use tracing::info;
use ulid::Ulid;

use super::CreateDb;
use crate::api_integration::ApiRecordId;

const LQ_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_NOTIFICATIONS: usize = 100;

pub async fn live_select_table(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table}")).await.unwrap();

		// Start listening
		let mut users = db.select(&table).live().await.unwrap();

		// Create a record
		let created: Option<ApiRecordId> = db.create(table).await.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, Some(notification.data.clone()));
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		// Update the record
		let _: Option<ApiRecordId> = db
			.update(&notification.data.id)
			.content(UpdateContent {
				field: "bar".to_string(),
			})
			.await
			.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be updated
		assert_eq!(notification.action, Action::Update);

		// Delete the record
		let _: Option<ApiRecordId> = db.delete(&notification.data.id).await.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> = users.next().await.unwrap().unwrap();
		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
	}

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table}")).await.unwrap();

		// Start listening
		let mut users = db.select(Resource::from(&table)).live().await.unwrap();

		// Create a record
		db.create(Resource::from(&table)).await.unwrap();
		// Pull the notification
		let notification =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);
	}

	drop(permit);
}

pub async fn live_select_record_id(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table}")).await.unwrap();

		let record_id = RecordId::new(table, "john");

		// Start listening
		let mut users = db.select(&record_id).live().await.unwrap();

		// Create a record
		let created: Option<ApiRecordId> = db.create(record_id).await.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, Some(notification.data.clone()));
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		// Update the record
		let _: Option<ApiRecordId> = db
			.update(&notification.data.id)
			.content(UpdateContent {
				field: "bar".to_string(),
			})
			.await
			.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be updated
		assert_eq!(notification.action, Action::Update);

		// Delete the record
		let _: Option<ApiRecordId> = db.delete(&notification.data.id).await.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
	}

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table}")).await.unwrap();

		let record_id = RecordId::new(table, "john");

		// Start listening
		let mut users = db.select(Resource::from(&record_id)).live().await.unwrap();

		// Create a record
		db.create(Resource::from(record_id)).await.unwrap();
		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);
	}

	drop(permit);
}

pub async fn live_select_record_ranges(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table}")).await.unwrap();

		// Start listening
		let mut users = db.select(&table).range("jane".."john").live().await.unwrap();

		// Create a record
		let created: Option<ApiRecordId> = db.create((table, "jane")).await.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, Some(notification.data.clone()));
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		// Update the record
		let _: Option<ApiRecordId> = db
			.update(&notification.data.id)
			.content(UpdateContent {
				field: "bar".to_string(),
			})
			.await
			.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be updated
		assert_eq!(notification.action, Action::Update);

		// Delete the record
		let _: Option<ApiRecordId> = db.delete(&notification.data.id).await.unwrap();

		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();

		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
	}

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table}")).await.unwrap();

		// Start listening
		let mut users =
			db.select(Resource::from(&table)).range("jane".."john").live().await.unwrap();

		// Create a record
		let created_value = db
			.create(Resource::from((table, "job")))
			.await
			.unwrap()
			.into_array()
			.unwrap()
			.remove(0)
			.into_object()
			.unwrap();

		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		// Delete the record
		let thing = match created_value.get("id").unwrap() {
			Value::RecordId(thing) => thing,
			_ => panic!("Expected a thing"),
		};
		db.query("DELETE $item").bind(("item", thing.clone())).await.unwrap();

		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();

		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
		let notification = match notification.data {
			Value::Object(notification) => notification,
			_ => panic!("Expected an object"),
		};
		assert_eq!(notification, created_value);
	}

	drop(permit);
}

pub async fn live_select_query(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();
	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table}")).await.unwrap();

		// Start listening
		info!("Starting live query");
		let users: QueryStream<Notification<ApiRecordId>> = db
			.query(format!("LIVE SELECT * FROM {table}"))
			.await
			.unwrap()
			.stream::<Notification<_>>(0)
			.unwrap();
		let users = Arc::new(RwLock::new(users));

		// Create a record
		info!("Creating record");
		let created: Option<ApiRecordId> = db.create(table).await.unwrap();
		// Pull the notification
		let notifications = receive_all_pending_notifications(Arc::clone(&users), LQ_TIMEOUT).await;
		// It should be newly created
		assert_eq!(
			notifications.iter().map(|n| n.action).collect::<Vec<_>>(),
			vec![Action::Create],
			"{:?}",
			notifications
		);
		// The returned record should match the created record
		assert_eq!(created, Some(notifications[0].data.clone()));

		// Update the record
		info!("Updating record");
		let _: Option<ApiRecordId> = db
			.update(&notifications[0].data.id)
			.content(UpdateContent {
				field: "bar".to_string(),
			})
			.await
			.unwrap();
		let notifications = receive_all_pending_notifications(Arc::clone(&users), LQ_TIMEOUT).await;

		// It should be updated
		assert_eq!(
			notifications.iter().map(|n| n.action).collect::<Vec<_>>(),
			[Action::Update],
			"{:?}",
			notifications
		);

		// Delete the record
		info!("Deleting record");
		let _: Option<ApiRecordId> = db.delete(&notifications[0].data.id).await.unwrap();
		// Pull the notification
		let notifications = receive_all_pending_notifications(Arc::clone(&users), LQ_TIMEOUT).await;
		// It should be deleted
		assert_eq!(
			notifications.iter().map(|n| n.action).collect::<Vec<_>>(),
			[Action::Delete],
			"{:?}",
			notifications
		);
	}

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL")).await.unwrap();

		// Start listening
		let mut users = db
			.query(format!("LIVE SELECT * FROM {table}"))
			.await
			.unwrap()
			.stream::<Value>(0)
			.unwrap();

		// Create a record
		db.create(Resource::from(&table)).await.unwrap();
		// Pull the notification
		let notification =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();

		// The returned record should be an object
		assert!(notification.data.is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);
	}

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL")).await.unwrap();

		// Start listening
		let mut users = db
			.query(format!("LIVE SELECT * FROM {table}"))
			.await
			.unwrap()
			.stream::<Notification<_>>(())
			.unwrap();

		// Create a record
		let created: Option<ApiRecordId> = db.create(table).await.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, Some(notification.data.clone()));
		// It should be newly created
		assert_eq!(notification.action, Action::Create, "{:?}", notification);

		// Update the record
		let _: Option<ApiRecordId> = db
			.update(&notification.data.id)
			.content(UpdateContent {
				field: "bar".to_string(),
			})
			.await
			.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be updated
		assert_eq!(notification.action, Action::Update, "{:?}", notification);

		// Delete the record
		let _: Option<ApiRecordId> = db.delete(&notification.data.id).await.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be deleted
		assert_eq!(notification.action, Action::Delete, "{:?}", notification);
	}

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL")).await.unwrap();

		// Start listening
		let mut users = db
			.query(format!("BEGIN; LIVE SELECT * FROM {table}; COMMIT"))
			.await
			.unwrap()
			.stream::<Value>(())
			.unwrap();

		// Create a record
		db.create(Resource::from(&table)).await.unwrap();
		// Pull the notification
		let notification =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);
	}

	drop(permit);
}

pub async fn live_query_delete_notifications(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	db.query("DEFINE TABLE bar".to_string()).await.unwrap().check().unwrap();

	let mut stream =
		db.query("LIVE SELECT field FROM bar").await.unwrap().stream::<Value>(0).unwrap();

	db.query("CREATE bar CONTENT { field: 'baz' }").await.unwrap().check().unwrap();
	let notification =
		tokio::time::timeout(LQ_TIMEOUT, stream.next()).await.unwrap().unwrap().unwrap();
	assert_eq!(notification.data, Value::Object(object! { field: "baz" }));
	assert_eq!(notification.action, Action::Create);

	db.query("UPDATE bar MERGE { data: 123 }").await.unwrap().check().unwrap();
	let notification =
		tokio::time::timeout(LQ_TIMEOUT, stream.next()).await.unwrap().unwrap().unwrap();
	assert_eq!(notification.data, Value::Object(object! { field: "baz" }));
	assert_eq!(notification.action, Action::Update);

	db.query("DELETE bar").await.unwrap().check().unwrap();
	let notification =
		tokio::time::timeout(LQ_TIMEOUT, stream.next()).await.unwrap().unwrap().unwrap();
	assert_eq!(notification.data, Value::Object(object! { field: "baz" }));
	assert_eq!(notification.action, Action::Delete);

	drop(permit);
}

#[derive(Debug, Clone, SurrealValue, PartialEq, PartialOrd)]
struct ApiRecordIdWithFetchedLink {
	id: RecordId,
	link: Option<ApiRecordId>,
}

#[derive(Debug, Clone, SurrealValue, PartialEq, PartialOrd)]
struct ApiRecordIdWithUnfetchedLink {
	id: RecordId,
	link: RecordId,
}

#[derive(Debug, Clone, SurrealValue, PartialEq, PartialOrd)]
struct LinkContent {
	link: RecordId,
}

#[derive(Debug, Clone, SurrealValue)]
struct UpdateContent {
	field: String,
}

pub async fn live_select_with_fetch(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	let table = format!("table_{}", Ulid::new());
	let linktb = format!("link_{}", Ulid::new());
	db.query(format!("DEFINE TABLE {table}")).await.unwrap();

	// Start listening
	let mut users = db
		.query(format!("LIVE SELECT * FROM {table} FETCH link"))
		.await
		.unwrap()
		.stream::<Notification<_>>(())
		.unwrap();

	let link: Option<ApiRecordId> = db.create(&linktb).await.unwrap();
	let linkone = link.unwrap().id;
	let link: Option<ApiRecordId> = db.create(&linktb).await.unwrap();
	let linktwo = link.unwrap().id;

	// Create a record
	let created: Option<ApiRecordIdWithUnfetchedLink> = db
		.create(table)
		.content(LinkContent {
			link: linkone.clone(),
		})
		.await
		.unwrap();
	// Pull the notification
	let notification: Notification<ApiRecordIdWithFetchedLink> =
		tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
	// // The returned record should match the created record
	assert_eq!(
		ApiRecordIdWithFetchedLink {
			id: created.unwrap().id,
			link: Some(ApiRecordId {
				id: linkone,
			}),
		},
		notification.data.clone()
	);
	// It should be newly created
	assert_eq!(notification.action, Action::Create);

	// Update the record
	let updated: Option<ApiRecordIdWithUnfetchedLink> = db
		.update(&notification.data.id)
		.content(LinkContent {
			link: linktwo.clone(),
		})
		.await
		.unwrap();
	// Pull the notification
	let notification: Notification<ApiRecordIdWithFetchedLink> =
		tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
	// The returned record should match the updated record
	assert_eq!(
		ApiRecordIdWithFetchedLink {
			id: updated.unwrap().id,
			link: Some(ApiRecordId {
				id: linktwo,
			}),
		},
		notification.data.clone()
	);
	// It should be updated
	assert_eq!(notification.action, Action::Update);

	// Delete the record
	let _: Option<ApiRecordIdWithUnfetchedLink> = db.delete(&notification.data.id).await.unwrap();
	// Pull the notification
	let notification: Notification<ApiRecordIdWithFetchedLink> =
		users.next().await.unwrap().unwrap();
	// It should be deleted
	assert_eq!(notification.action, Action::Delete);

	drop(permit);
}

async fn receive_all_pending_notifications<S: Stream<Item = Result<Notification<I>>> + Unpin, I>(
	stream: Arc<RwLock<S>>,
	timeout: Duration,
) -> Vec<Notification<I>> {
	let mut results = Vec::new();
	let we_expect_timeout = tokio::time::timeout(timeout, async {
		while let Some(notification) = stream.write().await.next().await {
			if results.len() >= MAX_NOTIFICATIONS {
				panic!("too many notification!")
			}
			results.push(notification.unwrap())
		}
	})
	.await;
	assert!(we_expect_timeout.is_err());
	results
}

/// The live queries registered on `table`, rendered for substring matching.
async fn registered_lives<C: surrealdb::Connection>(db: &Surreal<C>, table: &str) -> String {
	let mut res = db.query(format!("INFO FOR TABLE {table}")).await.unwrap();
	let info: Value = res.take(0).unwrap();
	format!("{info:?}")
}

/// Removing a database must end the streams subscribed beneath it.
///
/// `REMOVE TABLE` announces the subscriptions it destroys; a database removal
/// destroys the same ones without going through that statement.
pub async fn live_query_remove_database_terminates_stream(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	let namespace = format!("ns_{}", Ulid::new());
	let database = format!("db_{}", Ulid::new());
	db.use_ns(&namespace).use_db(&database).await.unwrap();

	let table = format!("table_{}", Ulid::new());
	db.query(format!("DEFINE TABLE {table}")).await.unwrap();

	let mut users = db.select(&table).live().await.unwrap();

	let _: Option<ApiRecordId> = db.create(&table).await.unwrap();
	let notification: Notification<ApiRecordId> =
		tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
	assert_eq!(notification.action, Action::Create);

	db.query(format!("REMOVE DATABASE {database}")).await.unwrap().check().unwrap();

	assert!(
		tokio::time::timeout(LQ_TIMEOUT, users.next())
			.await
			.expect("stream did not terminate after REMOVE DATABASE")
			.is_none(),
		"stream must end after the database it subscribed to is removed"
	);

	drop(permit);
}

/// Removing a namespace must end the streams subscribed beneath it.
pub async fn live_query_remove_namespace_terminates_stream(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	let namespace = format!("ns_{}", Ulid::new());
	let database = format!("db_{}", Ulid::new());
	db.use_ns(&namespace).use_db(&database).await.unwrap();

	let table = format!("table_{}", Ulid::new());
	db.query(format!("DEFINE TABLE {table}")).await.unwrap();

	let mut users = db.select(&table).live().await.unwrap();

	let _: Option<ApiRecordId> = db.create(&table).await.unwrap();
	let notification: Notification<ApiRecordId> =
		tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
	assert_eq!(notification.action, Action::Create);

	db.query(format!("REMOVE NAMESPACE {namespace}")).await.unwrap().check().unwrap();

	assert!(
		tokio::time::timeout(LQ_TIMEOUT, users.next())
			.await
			.expect("stream did not terminate after REMOVE NAMESPACE")
			.is_none(),
		"stream must end after the namespace it subscribed to is removed"
	);

	drop(permit);
}

/// A `KILL` run through `query()` must report its outcome.
///
/// The result used to be discarded, so killing an unknown live query returned
/// `Ok` with nothing in it and the caller had no way to tell it had failed.
pub async fn live_query_kill_reports_unknown_id(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	let unknown = uuid::Uuid::new_v4();
	let outcome = db.query(format!("KILL u'{unknown}'")).await.unwrap().check();
	assert!(outcome.is_err(), "killing an unregistered live query must surface an error");

	drop(permit);
}

/// Dropping a stream must retire its live query.
///
/// `Stream::drop` is the teardown path most callers actually use, and it is
/// fire-and-forget — the kill it issues runs detached and cannot report failure
/// to anyone. So assert the subscription is really gone rather than trusting
/// that it ran.
pub async fn live_query_stream_drop_retires_the_query(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	let table = format!("table_{}", Ulid::new());
	db.query(format!("DEFINE TABLE {table}")).await.unwrap();

	let live_id = {
		let mut users = db.select(&table).live().await.unwrap();

		let _: Option<ApiRecordId> = db.create(&table).await.unwrap();
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		assert_eq!(notification.action, Action::Create);

		let registered = registered_lives(&db, &table).await;
		assert!(
			registered.contains(&notification.query_id.to_string()),
			"live query should be registered while its stream is open: {registered}"
		);

		notification.query_id
		// stream dropped here
	};

	// The kill is detached, so give it a bounded chance to land.
	let started = std::time::Instant::now();
	loop {
		let registered = registered_lives(&db, &table).await;
		if !registered.contains(&live_id.to_string()) {
			break;
		}
		assert!(
			started.elapsed() < LQ_TIMEOUT,
			"dropping the stream should have retired {live_id}, still registered: {registered}"
		);
		tokio::time::sleep(Duration::from_millis(50)).await;
	}

	drop(permit);
}

/// A killed live query must end its stream.
///
/// `Stream::drop` tears a subscription down through its own path, so this
/// exercises the other one: a `KILL` issued as a query, where the `Killed`
/// notification is the only termination signal the subscriber ever gets. If it
/// is not delivered the stream stays open and silent forever, which a consumer
/// cannot distinguish from an idle subscription.
pub async fn live_query_kill_terminates_stream(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	let table = format!("table_{}", Ulid::new());
	db.query(format!("DEFINE TABLE {table}")).await.unwrap();

	let mut users = db.select(&table).live().await.unwrap();

	// Confirm the subscription is delivering before killing it.
	let _: Option<ApiRecordId> = db.create(&table).await.unwrap();
	let notification: Notification<ApiRecordId> =
		tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
	assert_eq!(notification.action, Action::Create);

	db.query(format!("KILL u'{}'", notification.query_id)).await.unwrap().check().unwrap();

	assert!(
		tokio::time::timeout(LQ_TIMEOUT, users.next())
			.await
			.expect("stream did not terminate after KILL")
			.is_none(),
		"stream must end after KILL, not yield another notification"
	);

	drop(permit);
}

/// Removing a table must end the streams subscribed to it.
///
/// Unlike `KILL` this is not caller-initiated, so a subscriber has no other way
/// to learn its subscription is gone.
pub async fn live_query_remove_table_terminates_stream(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	let table = format!("table_{}", Ulid::new());
	db.query(format!("DEFINE TABLE {table}")).await.unwrap();

	let mut users = db.select(&table).live().await.unwrap();

	// Confirm the subscription is delivering before removing the table.
	let _: Option<ApiRecordId> = db.create(&table).await.unwrap();
	let notification: Notification<ApiRecordId> =
		tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
	assert_eq!(notification.action, Action::Create);

	db.query(format!("REMOVE TABLE {table}")).await.unwrap().check().unwrap();

	assert!(
		tokio::time::timeout(LQ_TIMEOUT, users.next())
			.await
			.expect("stream did not terminate after REMOVE TABLE")
			.is_none(),
		"stream must end after REMOVE TABLE, not yield another notification"
	);

	drop(permit);
}

/// Test that LIVE SELECT returns UUID via take() method
/// This is a regression test for https://github.com/surrealdb/surrealdb/issues/6693
pub async fn live_select_returns_uuid(new_db: impl CreateDb) {
	let config = Config::new();
	let (permit, db) = new_db.create_db(config).await;

	db.use_ns(Ulid::new().to_string()).use_db(Ulid::new().to_string()).await.unwrap();

	let table = format!("table_{}", Ulid::new());
	db.query(format!("DEFINE TABLE {table}")).await.unwrap();

	// Execute LIVE SELECT and get the result via take()
	let mut response = db.query(format!("LIVE SELECT * FROM {table}")).await.unwrap();

	// The response should have 1 statement
	assert_eq!(response.num_statements(), 1, "LIVE SELECT should return exactly one result");

	// Take the result - it should be a UUID
	let result: Value = response.take(0).unwrap();
	assert!(result.is_uuid(), "LIVE SELECT should return a UUID, got: {:?}", result);

	drop(permit);
}

define_include_tests!(live => {
	#[test_log::test(tokio::test)]
	live_select_table,
	#[test_log::test(tokio::test)]
	live_select_record_id,
	#[test_log::test(tokio::test)]
	live_select_record_ranges,
	#[test_log::test(tokio::test)]
	live_select_query,
	#[test_log::test(tokio::test)]
	live_select_with_fetch,
	#[test_log::test(tokio::test)]
	live_query_delete_notifications,
	#[test_log::test(tokio::test)]
	live_select_returns_uuid,
	#[test_log::test(tokio::test)]
	live_query_kill_terminates_stream,
	#[test_log::test(tokio::test)]
	live_query_remove_table_terminates_stream,
	#[test_log::test(tokio::test)]
	live_query_stream_drop_retires_the_query,
	#[test_log::test(tokio::test)]
	live_query_kill_reports_unknown_id,
	#[test_log::test(tokio::test)]
	live_query_remove_database_terminates_stream,
	#[test_log::test(tokio::test)]
	live_query_remove_namespace_terminates_stream,
});
