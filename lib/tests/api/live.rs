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
		let created: Vec<ApiRecordId> = db.create(table).await.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, vec![notification.data.clone()]);
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
		assert!(notification.data.into_inner().is_object());
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
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}
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
		let created_value =
			match db.create(Resource::from((table, "job"))).await.unwrap().into_inner() {
				CoreValue::Object(created_value) => created_value,
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
			CoreValue::Thing(thing) => thing,
			_ => panic!("Expected a thing"),
		};
		db.query("DELETE $item").bind(("item", RecordId::from_inner(thing.clone()))).await.unwrap();

		// Pull the notification
		let notification: Notification<Value> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap();

		// It should be deleted
		assert_eq!(notification.action, Action::Delete);
		let notification = match notification.data.into_inner() {
			CoreValue::Object(notification) => notification,
			_ => panic!("Expected an object"),
		};
		assert_eq!(notification, created_value);
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
		let users: QueryStream<Notification<ApiRecordId>> = db
			.query(format!("LIVE SELECT * FROM {table}"))
			.await
			.unwrap()
			.stream::<Notification<_>>(0)
			.unwrap();
		let users = Arc::new(RwLock::new(users));

		// Create a record
		info!("Creating record");
		let created: Vec<ApiRecordId> = db.create(table).await.unwrap();
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
		let created: Vec<ApiRecordId> = db.create(table).await.unwrap();
		// Pull the notification
		let notification: Notification<ApiRecordId> =
			tokio::time::timeout(LQ_TIMEOUT, users.next()).await.unwrap().unwrap().unwrap();
		// The returned record should match the created record
		assert_eq!(created, vec![notification.data.clone()]);
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

#[test_log::test(tokio::test)]
async fn live_select_query_with_filter() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	// A list of objects to be inserted when creating the DB.
	let record_objects = vec![
		json!({"value": 10, "name": "A"}),
		json!({"value": 20, "name": "B"}),
		json!({"value": 30, "name": "C"}),
		json!({"value": 40, "name": "D"}),
		json!({"value": 50, "name": "E"}),
	];

	// A function to create a record with a given content.
	let create_record = |table: &str, content: serde_json::Value| {
		let db = db.clone();
		let table = table.to_string();
		async move {
			let record: Vec<RecordId> = db.create(&table).content(content).await.unwrap();
			record[0].clone()
		}
	};

	fn assert_indices(
		received_ids: Vec<RecordId>,
		expected_ids: Vec<RecordId>,
		where_clause: &str,
	) {
		assert_eq!(
			received_ids.len(),
			expected_ids.len(),
			"Wrong number of notifications for '{}': expected {}, got {}",
			where_clause,
			expected_ids.len(),
			received_ids.len()
		);

		for id in expected_ids {
			assert!(
				received_ids.contains(&id),
				"Expected ID {:?} not found in results for '{}'",
				id,
				where_clause
			);
		}
	}

	// Testing Live Query filtering with multiple operators.
	{
		let table = format!("table_{}", Ulid::new());
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}

		// Test cases
		let test_cases = vec![
			("value", ">", "25", vec![2, 3, 4]),
			("value", ">=", "51", vec![]),
			("value", "<", "30", vec![0, 1]),
			("value", "<=", "30", vec![0, 1, 2]),
			("value", "=", "20", vec![1]),
			("value", "!=", "30", vec![0, 1, 3, 4]),
			("name", "=", "'A'", vec![0]),
			("name", "!=", "'B'", vec![0, 2, 3, 4]),
		];

		for (col, operator, value, expected_indices) in test_cases.clone() {
			let where_clause = format!("{} {} {}", col, operator, value);
			let query = format!("LIVE SELECT * FROM {table} WHERE {where_clause}");
			info!("Starting live query: {}", query);

			let users: QueryStream<Notification<RecordId>> =
				db.query(query).await.unwrap().stream::<Notification<_>>(0).unwrap();
			let users = Arc::new(RwLock::new(users));

			// Create multiple records
			let mut records: Vec<RecordId> = Vec::new();
			for obj in &record_objects {
				records.push(create_record(&table, obj.clone()).await);
			}

			// Wait for initial notifications
			let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;

			// Check if the correct records are returned
			let received_ids: Vec<_> = notifications.iter().map(|n| n.data.clone()).collect();
			let expected_ids: Vec<_> =
				expected_indices.iter().map(|&i| records[i].clone()).collect();
			assert_indices(received_ids, expected_ids, &where_clause);
		}

		for (col, operator, value, expected_indices) in test_cases {
			let where_clause = format!("{} {} {}", col, operator, value);
			let query = format!("LET $var = {}; LIVE SELECT * FROM {table} WHERE {} {} $var", value, col, operator);
			info!("Starting live query: {}", query);

			let users: QueryStream<Notification<RecordId>> =
				db.query(query).await.unwrap().stream::<Notification<_>>(0).unwrap();
			let users = Arc::new(RwLock::new(users));

			// Create multiple records
			let mut records: Vec<RecordId> = Vec::new();
			for obj in &record_objects {
				records.push(create_record(&table, obj.clone()).await);
			}

			// Wait for initial notifications
			let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;

			// Check if the correct records are returned
			let received_ids: Vec<_> = notifications.iter().map(|n| n.data.clone()).collect();
			let expected_ids: Vec<_> =
				expected_indices.iter().map(|&i| records[i].clone()).collect();
			assert_indices(received_ids, expected_ids, &where_clause);
		}
	}

	// Testing Live Query filtering with update and delete operations.
	{
		let table = format!("table_{}", Ulid::new());
		let where_clause = "value >= 30";
		if FFLAGS.change_feed_live_queries.enabled() {
			db.query(format!("DEFINE TABLE {table} CHANGEFEED 10m INCLUDE ORIGINAL"))
				.await
				.unwrap();
		} else {
			db.query(format!("DEFINE TABLE {table}")).await.unwrap();
		}

		let query = format!("LIVE SELECT * FROM {table} WHERE {where_clause}");
		info!("Starting live query: {}", query);

		let users: QueryStream<Notification<RecordId>> =
			db.query(query).await.unwrap().stream::<Notification<_>>(0).unwrap();
		let users = Arc::new(RwLock::new(users));

		// Create multiple records
		let mut records: Vec<RecordId> = Vec::new();
		for obj in &record_objects {
			records.push(create_record(&table, obj.clone()).await);
		}

		// Wait for initial notifications
		let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;
		let expected_indices = vec![2, 3, 4];

		// Check if the correct records are returned
		let received_ids: Vec<_> = notifications.iter().map(|n| n.data.clone()).collect();
		let expected_ids: Vec<_> = expected_indices.iter().map(|&i| records[i].clone()).collect();

		assert_indices(received_ids.clone(), expected_ids, where_clause);

		// Update the record
		info!("Updating record");
		let record_to_update = &records[1];
		let _: Option<RecordId> = db
			.update(record_to_update.id.clone())
			.content(json!({"value": 35, "name": "J"}))
			.await
			.unwrap();
		let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;

		// We should get a notification for the update
		assert_eq!(
			notifications.iter().map(|n| n.action).collect::<Vec<_>>(),
			[Action::Update],
			"{:?}",
			notifications
		);
		assert_eq!(notifications[0].data.id, record_to_update.id.clone());

		// Update the record
		info!("Updating record");
		let record_to_update = &records[3];
		let _: Option<RecordId> = db
			.update(record_to_update.id.clone())
			.content(json!({"value": 15, "name": "M"}))
			.await
			.unwrap();

		let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;

		// Ensure that we don't get the filtered-out notifications.
		assert!(notifications.is_empty());

		// Delete the record
		info!("Deleting record");
		let record_to_delete = &records[4];
		let _: Option<RecordId> = db.delete(record_to_delete.id.clone()).await.unwrap();

		// Pull the notification
		let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;

		// It should be deleted
		assert_eq!(
			notifications.iter().map(|n| n.action).collect::<Vec<_>>(),
			[Action::Delete],
			"{:?}",
			notifications
		);
		assert_eq!(notifications[0].data.id, record_to_delete.id.clone());

		// Delete the record
		info!("Deleting record");
		let record_to_delete = &records[0];
		let _: Option<RecordId> = db.delete(record_to_delete.id.clone()).await.unwrap();

		// Pull the notification
		let notifications = receive_all_pending_notifications(users.clone(), LQ_TIMEOUT).await;

		// Ensure that we don't get the filtered-out notifications.
		assert!(notifications.is_empty());
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
