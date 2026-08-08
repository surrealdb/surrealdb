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
use surrealdb::{Notification, Result};
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

/// Re-authenticating a session ends the live queries the previous principal
/// registered.
///
/// A parity test between the transports, and the embedded half of
/// GHSA-2xrp-m9c6-75rj: a subscription authorised for one principal must not
/// keep delivering once the session belongs to another. The RPC path tears live
/// queries down on all six methods that change the principal (`signup`,
/// `signin`, `authenticate`, `refresh`, `invalidate`, `reset`); the embedded
/// engine tears them down on none.
pub async fn signin_ends_the_previous_principals_live_queries(new_db: impl CreateDb) {
	let (permit, db) = new_db.create_db(Config::new()).await;

	let namespace = Ulid::new().to_string();
	let database = Ulid::new().to_string();
	db.use_ns(&namespace).use_db(&database).await.unwrap();

	let table = format!("table_{}", Ulid::new());
	let access = Ulid::new();
	let email = format!("{access}@example.com");
	let pass = "password123";
	db.query(format!(
		"
        DEFINE TABLE {table};
        DEFINE ACCESS `{access}` ON DB TYPE RECORD
        SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
        SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
        DURATION FOR SESSION 1d FOR TOKEN 15s
    "
	))
	.await
	.unwrap()
	.check()
	.unwrap();

	// A second session on the same connection, so the write below comes from a
	// principal that is still allowed to make it. Cloning copies the current
	// (root) session state.
	let writer = db.clone();

	// Subscribe while this session is still root.
	let mut stream = db.select(Resource::from(&table)).live().await.unwrap();

	// Change the principal: root -> record user. The subscription above belongs
	// to the principal that has just been replaced, so it must not survive.
	db.signup(surrealdb::opt::auth::Record {
		namespace: namespace.clone(),
		database: database.clone(),
		access: access.to_string(),
		params: super::AuthParams {
			pass: pass.to_string(),
			email: email.clone(),
		},
	})
	.await
	.unwrap();

	// Anything the old subscription would have matched, written by a principal
	// that is still permitted to write it.
	let _: Value = writer.create(Resource::from(&table)).await.unwrap();

	// The stream must not deliver: either it has ended, or nothing arrives.
	if let Ok(Some(notification)) = tokio::time::timeout(LQ_TIMEOUT, stream.next()).await {
		panic!(
			"a live query registered before the principal changed still delivered: {:?}",
			notification.map(|n| n.action)
		);
	}

	drop(permit);
}

define_include_tests!(live => {
	#[test_log::test(tokio::test)]
	signin_ends_the_previous_principals_live_queries,
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
});
