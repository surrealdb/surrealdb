use std::collections::HashMap;
use surrealdb::dbs::Notification;
use surrealdb::sql;
use surrealdb::sql::{Object, Strand};

#[tokio::test]
async fn live_query_with_permission_gets_updates() {
	let db = new_db().await;
	let ns_name = "8bd3ed3d-8820-4e49-b733-7e2e12dbbdcb";
	let db_name = "c0fe8923-1a43-4ae5-95a6-c61d33d010af";
	db.use_ns(ns_name).use_db(db_name).await.unwrap();

	let scope = "40eccb7c-aa6f-4034-8da7-fecb73ba1dcd";
	let email = format!("{scope}@example.com");
	let pass = "password123";
	let sql = format!(
		"
        DEFINE SCOPE {scope} SESSION 1s
        SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
        SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
    "
	);
	let response = db.query(sql).await.unwrap();
	response.check().unwrap();
	db.signup(Scope {
		namespace: &ns_name,
		database: &db_name,
		scope: &scope,
		params: AuthParams {
			pass,
			email: &email,
		},
	})
	.await
	.unwrap();
	db.signin(Scope {
		namespace: &ns_name,
		database: &db_name,
		scope: &scope,
		params: AuthParams {
			pass,
			email: &email,
		},
	})
	.await
	.unwrap();

	// TODO change this to the live endpoint when ready in rust API
	let table = "table_name";
	let live_query_id: Option<sql::Uuid> =
		db.query(format!("LIVE SELECT * FROM {table}")).await.unwrap().take(0).unwrap();
	assert_ne!(live_query_id, None);

	let mut some_data = HashMap::new();
	some_data.insert("some_key".to_string(), Value::Strand(Strand::from("some_value")));
	let value = Value::Object(Object::from(some_data));
	db.query(format!("INSERT INTO {table} {{value}}")).bind(("value", value)).await.unwrap();

	// let live_events: Vec<Notification> = db.get_live_notifications();
	let live_events: Vec<Notification> = vec![];

	assert_eq!(live_events.len(), 1);
}

#[tokio::test]
async fn live_query_without_permission_does_not_get_updates() {
	let db = new_db().await;
	let ns_name = "0df38097-d8e7-41d6-88b1-30146eb76e7b";
	let db_name = "b5d67dad-04f5-4660-b0a7-e64fe8f021ad";
	db.use_ns(ns_name).use_db(db_name).await.unwrap();

	let scope = "cdc594be-46b5-49cf-90ba-4d74c371cb1e";
	let email = format!("{scope}@example.com");
	let pass = "password123";
	let sql = format!(
		"
        DEFINE SCOPE {scope} SESSION 1s
        SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
        SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
    "
	);
	let response = db.query(sql).await.unwrap();
	response.check().unwrap();
	db.signup(Scope {
		namespace: &ns_name,
		database: &db_name,
		scope: &scope,
		params: AuthParams {
			pass,
			email: &email,
		},
	})
	.await
	.unwrap();
	db.signin(Scope {
		namespace: &ns_name,
		database: &db_name,
		scope: &scope,
		params: AuthParams {
			pass,
			email: &email,
		},
	})
	.await
	.unwrap();

	// TODO change this to the live endpoint when ready in rust API
	let table = "table_name";
	let live_query_id: Option<sql::Uuid> =
		db.query(format!("LIVE SELECT * FROM {table}")).await.unwrap().take(0).unwrap();
	assert_ne!(live_query_id, None);

	let mut some_data = HashMap::new();
	some_data.insert("some_key".to_string(), Value::Strand(Strand::from("some_value")));
	let value = Value::Object(Object::from(some_data));
	db.query(format!("INSERT INTO {table} {{value}}")).bind(("value", value)).await.unwrap();

	// let live_events: Vec<Notification> = db.get_live_notifications();
	let live_events: Vec<Notification> = vec![];

	assert_eq!(live_events.len(), 0);
}
