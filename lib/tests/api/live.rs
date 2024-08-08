// Tests for running live queries
// Supported by the storage engines and the WS protocol

use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use std::ops::DerefMut;
use surrealdb::method::QueryStream;
use surrealdb::Action;
use surrealdb::Notification;
use surrealdb_core::sql::Object;
use tokio::sync::RwLock;
use tracing::info;

const LQ_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_NOTIFICATIONS: usize = 100;

#[test_log::test(tokio::test)]
async fn live_select_table() {
	let (permit, db) = new_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	{
		let table = format!("table_{}", Ulid::new());
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}

		// Start listening
		let mut users = db.select(&table).live().await.unwrap();

		// Create a record
		let created: Vec<RecordId> = db.create(table).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, vec![notification.data.clone()]);
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		// Update the record
		let _: Option<RecordId> =
			db.update(&notification.data.id).content(json!({"foo": "bar"})).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be updated
		assert_eq!(notification.action, Action::Update);

		// Delete the record
		let _: Option<RecordId> = db.delete(&notification.data.id).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> = users.next().await.unwrap().unwrap();
		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
	}

	{
		let table = format!("table_{}", Ulid::new());
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}

		// Start listening
		let mut users = db.select(Resource::from(&table)).live().await.unwrap();

		// Create a record
		db.create(Resource::from(&table)).await.unwrap();
		// Pull the notification
		let notification = tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);
	}

	drop(permit);
}

#[test_log::test(tokio::test)]
async fn live_select_record_id() {
	let (permit, db) = new_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	{
		let table = format!("table_{}", Ulid::new());
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}
		let record_id = Thing::from((table, "john".to_owned()));

		// Start listening
		let mut users = db.select(&record_id).live().await.unwrap();

		// Create a record
		let created: Option<RecordId> = db.create(record_id).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, Some(notification.data.clone()));
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		// Update the record
		let _: Option<RecordId> =
			db.update(&notification.data.id).content(json!({"foo": "bar"})).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be updated
		assert_eq!(notification.action, Action::Update);

		// Delete the record
		let _: Option<RecordId> = db.delete(&notification.data.id).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
	}

	{
		let table = format!("table_{}", Ulid::new());
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}
		let record_id = Thing::from((table, "john".to_owned()));

		// Start listening
		let mut users = db.select(Resource::from(&record_id)).live().await.unwrap();

		// Create a record
		db.create(Resource::from(record_id)).await.unwrap();
		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);
	}

	drop(permit);
}

#[test_log::test(tokio::test)]
async fn live_select_record_ranges() {
	let (permit, db) = new_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	{
		let table = format!("table_{}", Ulid::new());
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}

		// Start listening
		let mut users = db.select(&table).range("jane".."john").live().await.unwrap();

		// Create a record
		let created: Option<RecordId> = db.create((table, "jane")).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, Some(notification.data.clone()));
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		// Update the record
		let _: Option<RecordId> =
			db.update(&notification.data.id).content(json!({"foo": "bar"})).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be updated
		assert_eq!(notification.action, Action::Update);

		// Delete the record
		let _: Option<RecordId> = db.delete(&notification.data.id).await.unwrap();

		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();

		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
	}

	{
		let table = format!("table_{}", Ulid::new());
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}

		// Start listening
		let mut users =
			db.select(Resource::from(&table)).range("jane".."john").live().await.unwrap();

		// Create a record
		let created_value = match db.create(Resource::from((table, "job"))).await.unwrap() {
			Value::Object(created_value) => created_value,
			_ => panic!("Expected an object"),
		};

		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();
		// The returned record should be an object
		assert!(notification.data.is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		// Delete the record
		let thing = match created_value.0.get("id").unwrap() {
			Value::Thing(thing) => thing,
			_ => panic!("Expected a thing"),
		};
		db.query("DELETE $item").bind(("item", thing.clone())).await.unwrap();

		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();

		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
		let notification = match notification.data {
			Value::Object(notification) => notification,
			_ => panic!("Expected an object"),
		};
		assert_eq!(notification.0, created_value.0);
	}

	drop(permit);
}

#[test_log::test(tokio::test)]
async fn live_select_query() {
	let (permit, db) = new_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	{
		let table = format!("table_{}", Ulid::new());
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}

		// Start listening
		info!("Starting live query");
		let users: QueryStream<Notification<RecordId>> = db
			.query(format!("LIVE SELECT * FROM {table}"))
			.await
			.unwrap()
			.stream::<Notification<_>>(0)
			.unwrap();
		let users = Arc::new(RwLock::new(users));

		// Create a record
		info!("Creating record");
		let created: Vec<RecordId> = db.create(table).await.unwrap();
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
		assert_eq!(created, vec![notifications[0].data.clone()]);

		// Update the record
		info!("Updating record");
		let _: Option<RecordId> =
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
		let _: Option<RecordId> = db.delete(&notifications[0].data.id).await.unwrap();
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
		let created: Vec<RecordId> = db.create(table).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, vec![notification.data.clone()]);
		// It should be newly created
		assert_eq!(notification.action, Action::Create, "{:?}", notification);

		// Update the record
		let _: Option<RecordId> =
			db.update(&notification.data.id).content(json!({"foo": "bar"})).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// It should be updated
		assert_eq!(notification.action, Action::Update, "{:?}", notification);

		// Delete the record
		let _: Option<RecordId> = db.delete(&notification.data.id).await.unwrap();
		// Pull the notification
		let notification: Notification<RecordId> =
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
		assert!(notification.data.is_object());
		// It should be newly created
		assert_eq!(notification.action, Action::Create);

		info!("Removing table");
		let s = db.query(format!("REMOVE TABLE {}", table)).await.unwrap();

		// Pull the notification
		let notification = tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();
		// It should be Terminated
		assert_eq!(notification.action, Action::Terminate);
		assert_eq!(notification.data, Value::None);
	}

	drop(permit);
}

async fn receive_all_pending_notifications<
	S: Stream<Item = Result<Notification<I>, Error>> + Unpin,
	I,
>(
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
