// Tests for running live queries
// Supported by the storage engines and the WS protocol

use futures::StreamExt;
use futures::TryStreamExt;
use surrealdb::Action;
use surrealdb::Notification;

const LQ_TIMEOUT: Duration = Duration::from_secs(10);

#[test_log::test(tokio::test)]
async fn live_select_table() {
	let (permit, db) = new_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	{
		let table = format!("table_{}", Ulid::new());

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
		let record_id = Thing::from((Ulid::new().to_string(), "john".to_owned()));

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
		let record_id = Thing::from((Ulid::new().to_string(), "john".to_owned()));

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

		// Start listening
		let mut users =
			db.select(Resource::from(&table)).range("jane".."john").live().await.unwrap();

		// Create a record
		db.create(Resource::from((table, "job"))).await.unwrap();
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
async fn live_select_query() {
	let (permit, db) = new_db().await;

	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();

	{
		let table = format!("table_{}", Ulid::new());

		// Start listening
		let mut users = db
			.query(format!("LIVE SELECT * FROM {table}"))
			.await
			.unwrap()
			.stream::<Notification<_>>(0)
			.unwrap();

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
		let notification =
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
	}

	drop(permit);
}
