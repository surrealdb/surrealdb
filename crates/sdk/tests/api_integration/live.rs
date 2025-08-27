#![cfg(any(
	feature = "protocol-ws",
	feature = "kv-mem",
	feature = "kv-rocksdb",
	feature = "kv-tikv",
	feature = "kv-fdb-7_3",
	feature = "kv-fdb-7_1",
	feature = "kv-surrealkv",
))]

// Tests for running live queries
// Supported by the storage engines and the WS protocol

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use surrealdb::method::QueryStream;
use surrealdb::opt::Resource;
use surrealdb::{Action, Notification, RecordId, Value};
use surrealdb_core::val;
use tokio::sync::RwLock;
use tracing::info;
use ulid::Ulid;

use super::{CreateDb, NS};
use crate::api_integration::ApiRecordId;

const LQ_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_NOTIFICATIONS: usize = 100;

pub async fn live_select_table(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

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
		let _: Option<ApiRecordId> =
			db.update(&notification.data.id).content(json!({"foo": "bar"})).await.unwrap();
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
		let notification = tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.into_inner().is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);
	}

	drop(permit);
}

pub async fn live_select_record_id(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	{
		let table = format!("table_{}", Ulid::new());
		db.query(format!("DEFINE TABLE {table}")).await.unwrap();

		let record_id = RecordId::from((table, "john".to_owned()));

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
		let _: Option<ApiRecordId> =
			db.update(&notification.data.id).content(json!({"foo": "bar"})).await.unwrap();
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

		let record_id = RecordId::from((table, "john".to_owned()));

		// Start listening
		let mut users = db.select(Resource::from(&record_id)).live().await.unwrap();

		// Create a record
		db.create(Resource::from(record_id)).await.unwrap();
		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.into_inner().is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);
	}

	drop(permit);
}

pub async fn live_select_record_ranges(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

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
		let _: Option<ApiRecordId> =
			db.update(&notification.data.id).content(json!({"foo": "bar"})).await.unwrap();
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
		let created_value =
			match db.create(Resource::from((table, "job"))).await.unwrap().into_inner() {
				val::Value::Object(created_value) => created_value,
				_ => panic!("Expected an object"),
			};

		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.into_inner().is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		// Delete the record
		let thing = match created_value.get("id").unwrap() {
			val::Value::RecordId(thing) => thing,
			_ => panic!("Expected a thing"),
		};
		db.query("DELETE $item").bind(("item", RecordId::from_inner(thing.clone()))).await.unwrap();

		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();

		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
		let notification = match notification.data.into_inner() {
			val::Value::Object(notification) => notification,
			_ => panic!("Expected an object"),
		};
		assert_eq!(notification, created_value);
	}

	drop(permit);
}

pub async fn live_select_query(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
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
		let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;
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
		let _: Option<ApiRecordId> =
			db.update(&notifications[0].data.id).content(json!({"foo": "bar"})).await.unwrap();
		let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;

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
		let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;
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
		let notification = tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();

		// The returned record should be an object
		assert!(notification.data.into_inner().is_object());
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
		let _: Option<ApiRecordId> =
			db.update(&notification.data.id).content(json!({"foo": "bar"})).await.unwrap();
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
			.query("BEGIN")
			.query(format!("LIVE SELECT * FROM {table}"))
			.query("COMMIT")
			.await
			.unwrap()
			.stream::<Value>(())
			.unwrap();

		// Create a record
		db.create(Resource::from(&table)).await.unwrap();
		// Pull the notification
		let notification = tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.into_inner().is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);
	}

	drop(permit);
}

#[derive(Debug, Clone, Deserialize, PartialEq, PartialOrd)]
struct ApiRecordIdWithFetchedLink {
	id: RecordId,
	link: Option<ApiRecordId>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, PartialOrd)]
struct ApiRecordIdWithUnfetchedLink {
	id: RecordId,
	link: RecordId,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, PartialOrd)]
struct LinkContent {
	link: RecordId,
}

pub async fn live_select_with_fetch(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

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
});
