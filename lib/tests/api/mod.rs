// Tests common to all protocols and storage engines

use surrealdb::fflags::FFLAGS;
use surrealdb::sql::value;
use surrealdb::Response;

static PERMITS: Semaphore = Semaphore::const_new(1);

#[test_log::test(tokio::test)]
async fn connect() {
	let (permit, db) = new_db().await;
	drop(permit);
	db.health().await.unwrap();
}

#[test_log::test(tokio::test)]
async fn yuse() {
	let (permit, db) = new_db().await;
	let item = Ulid::new().to_string();
	match db.create(Resource::from(item.as_str())).await.unwrap_err() {
		// Local engines return this error
		Error::Db(DbError::NsEmpty) => {}
		// Remote engines return this error
		Error::Api(ApiError::Query(error)) if error.contains("Specify a namespace to use") => {}
		error => panic!("{:?}", error),
	}
	db.use_ns(NS).await.unwrap();
	match db.create(Resource::from(item.as_str())).await.unwrap_err() {
		// Local engines return this error
		Error::Db(DbError::DbEmpty) => {}
		// Remote engines return this error
		Error::Api(ApiError::Query(error)) if error.contains("Specify a database to use") => {}
		error => panic!("{:?}", error),
	}
	db.use_db(item.as_str()).await.unwrap();
	db.create(Resource::from(item)).await.unwrap();
	drop(permit);
}

#[test_log::test(tokio::test)]
async fn invalidate() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	db.invalidate().await.unwrap();
	let error = db.create::<Option<RecordId>>(("user", "john")).await.unwrap_err();
	assert!(
		error.to_string().contains("Not enough permissions to perform this action"),
		"Unexpected error: {:?}",
		error
	);
}

#[test_log::test(tokio::test)]
async fn signup_record() {
	let (permit, db) = new_db().await;
	let database = Ulid::new().to_string();
	db.use_ns(NS).use_db(&database).await.unwrap();
	let access = Ulid::new().to_string();
	let sql = format!(
		"
        DEFINE ACCESS `{access}` ON DB TYPE RECORD
        SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
        SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1d FOR TOKEN 15s
    "
	);
	let response = db.query(sql).await.unwrap();
	drop(permit);
	response.check().unwrap();
	db.signup(RecordAccess {
		namespace: NS,
		database: &database,
		access: &access,
		params: AuthParams {
			email: "john.doe@example.com",
			pass: "password123",
		},
	})
	.await
	.unwrap();
}

#[test_log::test(tokio::test)]
async fn signin_ns() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let user = Ulid::new().to_string();
	let pass = "password123";
	let sql = format!("DEFINE USER `{user}` ON NAMESPACE PASSWORD '{pass}'");
	let response = db.query(sql).await.unwrap();
	drop(permit);
	response.check().unwrap();
	db.signin(Namespace {
		namespace: NS,
		username: &user,
		password: pass,
	})
	.await
	.unwrap();
}

#[test_log::test(tokio::test)]
async fn signin_db() {
	let (permit, db) = new_db().await;
	let database = Ulid::new().to_string();
	db.use_ns(NS).use_db(&database).await.unwrap();
	let user = Ulid::new().to_string();
	let pass = "password123";
	let sql = format!("DEFINE USER `{user}` ON DATABASE PASSWORD '{pass}'");
	let response = db.query(sql).await.unwrap();
	drop(permit);
	response.check().unwrap();
	db.signin(Database {
		namespace: NS,
		database: &database,
		username: &user,
		password: pass,
	})
	.await
	.unwrap();
}

#[test_log::test(tokio::test)]
async fn signin_record() {
	let (permit, db) = new_db().await;
	let database = Ulid::new().to_string();
	db.use_ns(NS).use_db(&database).await.unwrap();
	let access = Ulid::new().to_string();
	let email = format!("{access}@example.com");
	let pass = "password123";
	let sql = format!(
		"
        DEFINE ACCESS `{access}` ON DB TYPE RECORD
        SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
        SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
		DURATION FOR SESSION 1d FOR TOKEN 15s
    "
	);
	let response = db.query(sql).await.unwrap();
	drop(permit);
	response.check().unwrap();
	db.signup(RecordAccess {
		namespace: NS,
		database: &database,
		access: &access,
		params: AuthParams {
			pass,
			email: &email,
		},
	})
	.await
	.unwrap();
	db.signin(RecordAccess {
		namespace: NS,
		database: &database,
		access: &access,
		params: AuthParams {
			pass,
			email: &email,
		},
	})
	.await
	.unwrap();
}

#[test_log::test(tokio::test)]
async fn record_access_throws_error() {
	let (permit, db) = new_db().await;
	let database = Ulid::new().to_string();
	db.use_ns(NS).use_db(&database).await.unwrap();
	let access = Ulid::new().to_string();
	let email = format!("{access}@example.com");
	let pass = "password123";
	let sql = format!(
		"
        DEFINE ACCESS `{access}` ON DB TYPE RECORD
        SIGNUP {{ THROW 'signup_thrown_error' }}
        SIGNIN {{ THROW 'signin_thrown_error' }}
		DURATION FOR SESSION 1d FOR TOKEN 15s
    "
	);
	let response = db.query(sql).await.unwrap();
	drop(permit);
	response.check().unwrap();

	match db
		.signup(RecordAccess {
			namespace: NS,
			database: &database,
			access: &access,
			params: AuthParams {
				pass,
				email: &email,
			},
		})
		.await
	{
		Err(Error::Db(surrealdb::err::Error::Thrown(e))) => assert_eq!(e, "signup_thrown_error"),
		Err(Error::Api(surrealdb::error::Api::Query(e))) => assert!(e.contains("signup")),
		Err(Error::Api(surrealdb::error::Api::Http(e))) => assert_eq!(
			e,
			"HTTP status client error (400 Bad Request) for url (http://127.0.0.1:8000/signup)"
		),
		v => panic!("Unexpected response or error: {v:?}"),
	};

	match db
		.signin(RecordAccess {
			namespace: NS,
			database: &database,
			access: &access,
			params: AuthParams {
				pass,
				email: &email,
			},
		})
		.await
	{
		Err(Error::Db(surrealdb::err::Error::Thrown(e))) => assert_eq!(e, "signin_thrown_error"),
		Err(Error::Api(surrealdb::error::Api::Query(e))) => assert!(e.contains("signin")),
		Err(Error::Api(surrealdb::error::Api::Http(e))) => assert_eq!(
			e,
			"HTTP status client error (400 Bad Request) for url (http://127.0.0.1:8000/signin)"
		),
		v => panic!("Unexpected response or error: {v:?}"),
	};
}

#[test_log::test(tokio::test)]
async fn record_access_invalid_query() {
	let (permit, db) = new_db().await;
	let database = Ulid::new().to_string();
	db.use_ns(NS).use_db(&database).await.unwrap();
	let access = Ulid::new().to_string();
	let email = format!("{access}@example.com");
	let pass = "password123";
	let sql = format!(
		"
        DEFINE ACCESS `{access}` ON DB TYPE RECORD
        SIGNUP {{ SELECT * FROM ONLY [1, 2] }}
        SIGNIN {{ SELECT * FROM ONLY [1, 2] }}
		DURATION FOR SESSION 1d FOR TOKEN 15s
    "
	);
	let response = db.query(sql).await.unwrap();
	drop(permit);
	response.check().unwrap();

	match db
		.signup(RecordAccess {
			namespace: NS,
			database: &database,
			access: &access,
			params: AuthParams {
				pass,
				email: &email,
			},
		})
		.await
	{
		Err(Error::Db(surrealdb::err::Error::AccessRecordSignupQueryFailed)) => (),
		Err(Error::Api(surrealdb::error::Api::Query(e))) => {
			assert_eq!(
				e,
				"There was a problem with the database: The record access signup query failed"
			)
		}
		Err(Error::Api(surrealdb::error::Api::Http(e))) => assert_eq!(
			e,
			"HTTP status client error (400 Bad Request) for url (http://127.0.0.1:8000/signup)"
		),
		v => panic!("Unexpected response or error: {v:?}"),
	};

	match db
		.signin(RecordAccess {
			namespace: NS,
			database: &database,
			access: &access,
			params: AuthParams {
				pass,
				email: &email,
			},
		})
		.await
	{
		Err(Error::Db(surrealdb::err::Error::AccessRecordSigninQueryFailed)) => (),
		Err(Error::Api(surrealdb::error::Api::Query(e))) => {
			assert_eq!(
				e,
				"There was a problem with the database: The record access signin query failed"
			)
		}
		Err(Error::Api(surrealdb::error::Api::Http(e))) => assert_eq!(
			e,
			"HTTP status client error (400 Bad Request) for url (http://127.0.0.1:8000/signin)"
		),
		v => panic!("Unexpected response or error: {v:?}"),
	};
}

#[test_log::test(tokio::test)]
async fn authenticate() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let user = Ulid::new().to_string();
	let pass = "password123";
	let sql = format!("DEFINE USER `{user}` ON NAMESPACE PASSWORD '{pass}'");
	let response = db.query(sql).await.unwrap();
	drop(permit);
	response.check().unwrap();
	let token = db
		.signin(Namespace {
			namespace: NS,
			username: &user,
			password: pass,
		})
		.await
		.unwrap();
	db.authenticate(token).await.unwrap();
}

#[test_log::test(tokio::test)]
async fn query() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let _ = db
		.query(
			"
        CREATE user:john
        SET name = 'John Doe'
    ",
		)
		.await
		.unwrap()
		.check()
		.unwrap();
	let mut response = db.query("SELECT name FROM user:john").await.unwrap().check().unwrap();
	let Some(name): Option<String> = response.take("name").unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(name, "John Doe");
}

#[test_log::test(tokio::test)]
async fn query_decimals() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let sql = "
	    DEFINE TABLE foo;
	    DEFINE FIELD bar ON foo TYPE decimal;
	    CREATE foo CONTENT { bar: 42.69 };
    ";
	let _ = db.query(sql).await.unwrap().check().unwrap();
	drop(permit);
}

#[test_log::test(tokio::test)]
async fn query_binds() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let mut response =
		db.query("CREATE user:john SET name = $name").bind(("name", "John Doe")).await.unwrap();
	let Some(record): Option<RecordName> = response.take(0).unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(record.name, "John Doe");
	let mut response = db
		.query("SELECT * FROM $record_id")
		.bind(("record_id", thing("user:john").unwrap()))
		.await
		.unwrap();
	let Some(record): Option<RecordName> = response.take(0).unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(record.name, "John Doe");
	let mut response = db
		.query("CREATE user SET name = $name")
		.bind(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
	let Some(record): Option<RecordName> = response.take(0).unwrap() else {
		panic!("query returned no record");
	};
	assert_eq!(record.name, "John Doe");
}

#[test_log::test(tokio::test)]
async fn query_with_stats() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let sql = "CREATE foo; SELECT * FROM foo";
	let mut response = db.query(sql).with_stats().await.unwrap();
	// First query statement
	let (stats, result) = response.take(0).unwrap();
	assert!(stats.execution_time > Some(Duration::ZERO));
	let _: Value = result.unwrap();
	// Second query statement
	let (stats, result) = response.take(1).unwrap();
	assert!(stats.execution_time > Some(Duration::ZERO));
	let _: Vec<RecordId> = result.unwrap();
}

#[test_log::test(tokio::test)]
async fn query_chaining() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let response = db
		.query(BeginStatement::default())
		.query("CREATE account:one SET balance = 135605.16")
		.query("CREATE account:two SET balance = 91031.31")
		.query("UPDATE account:one SET balance += 300.00")
		.query("UPDATE account:two SET balance -= 300.00")
		.query(CommitStatement::default())
		.await
		.unwrap();
	response.check().unwrap();
}

#[test_log::test(tokio::test)]
async fn mixed_results_query() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let sql = "CREATE bar SET baz = rand('a'); CREATE foo;";
	let mut response = db.query(sql).await.unwrap();
	response.take::<Value>(0).unwrap_err();
	let _: Option<RecordId> = response.take(1).unwrap();
}

#[test_log::test(tokio::test)]
async fn create_record_no_id() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let _: Vec<RecordId> = db.create("user").await.unwrap();
	let _: Value = db.create(Resource::from("user")).await.unwrap();
}

#[test_log::test(tokio::test)]
async fn create_record_with_id() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let _: Option<RecordId> = db.create(("user", "jane")).await.unwrap();
	let _: Value = db.create(Resource::from(("user", "john"))).await.unwrap();
	let _: Value = db.create(Resource::from("user:doe")).await.unwrap();
}

#[test_log::test(tokio::test)]
async fn create_record_no_id_with_content() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let _: Vec<RecordId> = db
		.create("user")
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
	let _: Value = db
		.create(Resource::from("user"))
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
}

#[test_log::test(tokio::test)]
async fn create_record_with_id_with_content() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let record: Option<RecordId> = db
		.create(("user", "john"))
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
	assert_eq!(record.unwrap().id, thing("user:john").unwrap());
	let value: Value = db
		.create(Resource::from("user:jane"))
		.content(Record {
			name: "Jane Doe",
		})
		.await
		.unwrap();
	assert_eq!(value.record(), thing("user:jane").ok());
}

#[test_log::test(tokio::test)]
async fn insert_table() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let table = "user";
	let _: Vec<RecordId> = db.insert(table).await.unwrap();
	let _: Vec<RecordId> = db.insert(table).content(json!({ "foo": "bar" })).await.unwrap();
	let _: Vec<RecordId> = db.insert(table).content(json!([{ "foo": "bar" }])).await.unwrap();
	let _: Value = db.insert(Resource::from(table)).await.unwrap();
	let _: Value = db.insert(Resource::from(table)).content(json!({ "foo": "bar" })).await.unwrap();
	let _: Value =
		db.insert(Resource::from(table)).content(json!([{ "foo": "bar" }])).await.unwrap();
	let users: Vec<RecordId> = db.insert(table).await.unwrap();
	assert!(!users.is_empty());
}

#[test_log::test(tokio::test)]
async fn insert_thing() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let table = "user";
	let _: Option<RecordId> = db.insert((table, "user1")).await.unwrap();
	let _: Option<RecordId> =
		db.insert((table, "user1")).content(json!({ "foo": "bar" })).await.unwrap();
	let _: Value = db.insert(Resource::from((table, "user2"))).await.unwrap();
	let _: Value =
		db.insert(Resource::from((table, "user2"))).content(json!({ "foo": "bar" })).await.unwrap();
	let user: Option<RecordId> = db.insert((table, "user3")).await.unwrap();
	assert_eq!(
		user,
		Some(RecordId {
			id: thing("user:user3").unwrap(),
		})
	);
}

#[test_log::test(tokio::test)]
async fn select_table() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let table = "user";
	let _: Vec<RecordId> = db.create(table).await.unwrap();
	let _: Vec<RecordId> = db.create(table).await.unwrap();
	let _: Value = db.create(Resource::from(table)).await.unwrap();
	let users: Vec<RecordId> = db.select(table).await.unwrap();
	assert_eq!(users.len(), 3);
}

#[test_log::test(tokio::test)]
async fn select_record_id() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let record_id = ("user", "john");
	let _: Option<RecordId> = db.create(record_id).await.unwrap();
	let Some(record): Option<RecordId> = db.select(record_id).await.unwrap() else {
		panic!("record not found");
	};
	assert_eq!(record.id, thing("user:john").unwrap());
	let value: Value = db.select(Resource::from(record_id)).await.unwrap();
	assert_eq!(value.record(), thing("user:john").ok());
}

#[test_log::test(tokio::test)]
async fn select_record_ranges() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let table = "user";
	let _: Option<RecordId> = db.create((table, "amos")).await.unwrap();
	let _: Option<RecordId> = db.create((table, "jane")).await.unwrap();
	let _: Option<RecordId> = db.create((table, "john")).await.unwrap();
	let _: Value = db.create(Resource::from((table, "zoey"))).await.unwrap();
	let convert = |users: Vec<RecordId>| -> Vec<String> {
		users.into_iter().map(|user| user.id.id.to_string()).collect()
	};
	let users: Vec<RecordId> = db.select(table).range(..).await.unwrap();
	assert_eq!(convert(users), vec!["amos", "jane", "john", "zoey"]);
	let users: Vec<RecordId> = db.select(table).range(.."john").await.unwrap();
	assert_eq!(convert(users), vec!["amos", "jane"]);
	let users: Vec<RecordId> = db.select(table).range(..="john").await.unwrap();
	assert_eq!(convert(users), vec!["amos", "jane", "john"]);
	let users: Vec<RecordId> = db.select(table).range("jane"..).await.unwrap();
	assert_eq!(convert(users), vec!["jane", "john", "zoey"]);
	let users: Vec<RecordId> = db.select(table).range("jane".."john").await.unwrap();
	assert_eq!(convert(users), vec!["jane"]);
	let users: Vec<RecordId> = db.select(table).range("jane"..="john").await.unwrap();
	assert_eq!(convert(users), vec!["jane", "john"]);
	let Value::Array(array): Value =
		db.select(Resource::from(table)).range("jane"..="john").await.unwrap()
	else {
		unreachable!();
	};
	assert_eq!(array.len(), 2);
	let users: Vec<RecordId> =
		db.select(table).range((Bound::Excluded("jane"), Bound::Included("john"))).await.unwrap();
	assert_eq!(convert(users), vec!["john"]);
}

#[test_log::test(tokio::test)]
async fn select_records_order_by_start_limit() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let sql = "
        CREATE user:john SET name = 'John';
        CREATE user:zoey SET name = 'Zoey';
    	CREATE user:amos SET name = 'Amos';
        CREATE user:jane SET name = 'Jane';
    ";
	db.query(sql).await.unwrap().check().unwrap();

	let check_start_limit = |mut response: Response, expected: Vec<&str>| {
		let users: Vec<RecordName> = response.take(0).unwrap();
		let users: Vec<String> = users.into_iter().map(|user| user.name).collect();
		assert_eq!(users, expected);
	};

	let response =
		db.query("SELECT name FROM user ORDER BY name DESC START 1 LIMIT 2").await.unwrap();
	check_start_limit(response, vec!["John", "Jane"]);

	let response = db.query("SELECT name FROM user ORDER BY name DESC START 1").await.unwrap();
	check_start_limit(response, vec!["John", "Jane", "Amos"]);

	let response = db.query("SELECT name FROM user ORDER BY name DESC START 4").await.unwrap();
	check_start_limit(response, vec![]);

	let response = db.query("SELECT name FROM user ORDER BY name DESC LIMIT 2").await.unwrap();
	check_start_limit(response, vec!["Zoey", "John"]);

	let response = db.query("SELECT name FROM user ORDER BY name DESC LIMIT 10").await.unwrap();
	check_start_limit(response, vec!["Zoey", "John", "Jane", "Amos"]);
}

#[test_log::test(tokio::test)]
async fn select_records_order_by() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let sql = "
        CREATE user:john SET name = 'John';
        CREATE user:zoey SET name = 'Zoey';
    	CREATE user:amos SET name = 'Amos';
        CREATE user:jane SET name = 'Jane';
    ";
	db.query(sql).await.unwrap().check().unwrap();
	let sql = "SELECT name FROM user ORDER BY name DESC";
	let mut response = db.query(sql).await.unwrap();
	let users: Vec<RecordName> = response.take(0).unwrap();
	let convert = |users: Vec<RecordName>| -> Vec<String> {
		users.into_iter().map(|user| user.name).collect()
	};
	assert_eq!(convert(users), vec!["Zoey", "John", "Jane", "Amos"]);
}

#[test_log::test(tokio::test)]
async fn select_records_fetch() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let sql = "
        CREATE tag:rs SET name = 'Rust';
		CREATE tag:go SET name = 'Golang';
		CREATE tag:js SET name = 'JavaScript';
		CREATE person:tobie SET tags = [tag:rs, tag:go, tag:js];
		CREATE person:jaime SET tags = [tag:js];
    ";
	db.query(sql).await.unwrap().check().unwrap();

	let check_fetch = |mut response: Response, expected: &str| {
		let val: Value = response.take(0).unwrap();
		let exp = value(expected).unwrap();
		assert_eq!(format!("{val:#}"), format!("{exp:#}"));
	};

	let sql = "SELECT * FROM person LIMIT 1 FETCH tags;";
	let response = db.query(sql).await.unwrap();
	check_fetch(
		response,
		"[
					{
						id: person:jaime,
						tags: [
							{
								id: tag:js,
								name: 'JavaScript'
							}
						]
					}
				]",
	);

	let sql = "SELECT * FROM person START 1 LIMIT 1 FETCH tags;";
	let response = db.query(sql).await.unwrap();
	check_fetch(
		response,
		"[
					{
						id: person:tobie,
						tags: [
							{
								id: tag:rs,
								name: 'Rust'
							},
							{
								id: tag:go,
								name: 'Golang'
							},
							{
								id: tag:js,
								name: 'JavaScript'
							}
						]
					}
				]",
	);

	let sql = "SELECT * FROM person ORDER BY id FETCH tags;";
	let response = db.query(sql).await.unwrap();
	check_fetch(
		response,
		"[
					{
						id: person:jaime,
						tags: [
							{
								id: tag:js,
								name: 'JavaScript'
							}
						]
					},
					{
						id: person:tobie,
						tags: [
							{
								id: tag:rs,
								name: 'Rust'
							},
							{
								id: tag:go,
								name: 'Golang'
							},
							{
								id: tag:js,
								name: 'JavaScript'
							}
						]
					}
				]",
	);
}

#[test_log::test(tokio::test)]
async fn update_table() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let table = "user";
	let _: Vec<RecordId> = db.create(table).await.unwrap();
	let _: Vec<RecordId> = db.create(table).await.unwrap();
	let _: Value = db.update(Resource::from(table)).await.unwrap();
	let users: Vec<RecordId> = db.update(table).await.unwrap();
	assert_eq!(users.len(), 2);
}

#[test_log::test(tokio::test)]
async fn update_record_id() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let table = "user";
	let _: Option<RecordId> = db.create((table, "john")).await.unwrap();
	let _: Option<RecordId> = db.create((table, "jane")).await.unwrap();
	let users: Vec<RecordId> = db.update(table).await.unwrap();
	assert_eq!(users.len(), 2);
}

#[test_log::test(tokio::test)]
async fn update_table_with_content() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let sql = "
        CREATE type::thing($table, 'amos') SET name = 'Amos';
        CREATE type::thing($table, 'jane') SET name = 'Jane';
        CREATE type::thing($table, 'john') SET name = 'John';
        CREATE type::thing($table, 'zoey') SET name = 'Zoey';
    ";
	let table = "user";
	let response = db.query(sql).bind(("table", table)).await.unwrap();
	response.check().unwrap();
	let users: Vec<RecordBuf> = db
		.update(table)
		.content(Record {
			name: "Doe",
		})
		.await
		.unwrap();
	let expected = &[
		RecordBuf {
			id: thing("user:amos").unwrap(),
			name: "Doe".to_owned(),
		},
		RecordBuf {
			id: thing("user:jane").unwrap(),
			name: "Doe".to_owned(),
		},
		RecordBuf {
			id: thing("user:john").unwrap(),
			name: "Doe".to_owned(),
		},
		RecordBuf {
			id: thing("user:zoey").unwrap(),
			name: "Doe".to_owned(),
		},
	];
	assert_eq!(users, expected);
	let users: Vec<RecordBuf> = db.select(table).await.unwrap();
	assert_eq!(users, expected);
}

#[test_log::test(tokio::test)]
async fn update_record_range_with_content() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let sql = "
        CREATE type::thing($table, 'amos') SET name = 'Amos';
        CREATE type::thing($table, 'jane') SET name = 'Jane';
        CREATE type::thing($table, 'john') SET name = 'John';
        CREATE type::thing($table, 'zoey') SET name = 'Zoey';
    ";
	let table = "user";
	let response = db.query(sql).bind(("table", table)).await.unwrap();
	response.check().unwrap();
	let users: Vec<RecordBuf> = db
		.update(table)
		.range("jane".."zoey")
		.content(Record {
			name: "Doe",
		})
		.await
		.unwrap();
	assert_eq!(
		users,
		&[
			RecordBuf {
				id: thing("user:jane").unwrap(),
				name: "Doe".to_owned(),
			},
			RecordBuf {
				id: thing("user:john").unwrap(),
				name: "Doe".to_owned(),
			},
		]
	);
	let users: Vec<RecordBuf> = db.select(table).await.unwrap();
	assert_eq!(
		users,
		&[
			RecordBuf {
				id: thing("user:amos").unwrap(),
				name: "Amos".to_owned(),
			},
			RecordBuf {
				id: thing("user:jane").unwrap(),
				name: "Doe".to_owned(),
			},
			RecordBuf {
				id: thing("user:john").unwrap(),
				name: "Doe".to_owned(),
			},
			RecordBuf {
				id: thing("user:zoey").unwrap(),
				name: "Zoey".to_owned(),
			},
		]
	);
}

#[test_log::test(tokio::test)]
async fn update_record_id_with_content() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let record_id = ("user", "john");
	let user: Option<RecordName> = db
		.create(record_id)
		.content(Record {
			name: "Jane Doe",
		})
		.await
		.unwrap();
	assert_eq!(user.unwrap().name, "Jane Doe");
	let user: Option<RecordName> = db
		.update(record_id)
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
	assert_eq!(user.unwrap().name, "John Doe");
	let user: Option<RecordName> = db.select(record_id).await.unwrap();
	assert_eq!(user.unwrap().name, "John Doe");
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
struct Name {
	first: Cow<'static, str>,
	last: Cow<'static, str>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
struct Person {
	#[serde(skip_serializing)]
	id: Option<Thing>,
	title: Cow<'static, str>,
	name: Name,
	marketing: bool,
}

#[test_log::test(tokio::test)]
async fn merge_record_id() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let record_id = ("person", "jaime");
	let mut jaime: Option<Person> = db
		.create(record_id)
		.content(Person {
			id: None,
			title: "Founder & COO".into(),
			name: Name {
				first: "Jaime".into(),
				last: "Morgan Hitchcock".into(),
			},
			marketing: false,
		})
		.await
		.unwrap();
	assert_eq!(jaime.unwrap().id.unwrap(), thing("person:jaime").unwrap());
	jaime = db.update(record_id).merge(json!({ "marketing": true })).await.unwrap();
	assert!(jaime.as_ref().unwrap().marketing);
	jaime = db.select(record_id).await.unwrap();
	assert_eq!(
		jaime.unwrap(),
		Person {
			id: Some(thing("person:jaime").unwrap()),
			title: "Founder & COO".into(),
			name: Name {
				first: "Jaime".into(),
				last: "Morgan Hitchcock".into(),
			},
			marketing: true,
		}
	);
}

#[test_log::test(tokio::test)]
async fn patch_record_id() {
	#[derive(Debug, Deserialize, Eq, PartialEq)]
	struct Record {
		id: Thing,
		baz: String,
		hello: Vec<String>,
	}

	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let id = "john";
	let _: Option<RecordId> = db
		.create(("user", id))
		.content(json!({
			"baz": "qux",
			"foo": "bar"
		}))
		.await
		.unwrap();
	let _: Option<Record> = db
		.update(("user", id))
		.patch(PatchOp::replace("/baz", "boo"))
		.patch(PatchOp::add("/hello", ["world"]))
		.patch(PatchOp::remove("/foo"))
		.await
		.unwrap();
	let value: Option<Record> = db.select(("user", id)).await.unwrap();
	assert_eq!(
		value,
		Some(Record {
			id: thing(&format!("user:{id}")).unwrap(),
			baz: "boo".to_owned(),
			hello: vec!["world".to_owned()],
		})
	);
}

#[test_log::test(tokio::test)]
async fn delete_table() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let table = "user";
	let _: Vec<RecordId> = db.create(table).await.unwrap();
	let _: Vec<RecordId> = db.create(table).await.unwrap();
	let _: Vec<RecordId> = db.create(table).await.unwrap();
	let users: Vec<RecordId> = db.select(table).await.unwrap();
	assert_eq!(users.len(), 3);
	let users: Vec<RecordId> = db.delete(table).await.unwrap();
	assert_eq!(users.len(), 3);
	let users: Vec<RecordId> = db.select(table).await.unwrap();
	assert!(users.is_empty());
}

#[test_log::test(tokio::test)]
async fn delete_record_id() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let record_id = ("user", "john");
	let _: Option<RecordId> = db.create(record_id).await.unwrap();
	let _: Option<RecordId> = db.select(record_id).await.unwrap();
	let john: Option<RecordId> = db.delete(record_id).await.unwrap();
	assert!(john.is_some());
	let john: Option<RecordId> = db.select(record_id).await.unwrap();
	assert!(john.is_none());
	// non-existing user
	let jane: Option<RecordId> = db.delete(("user", "jane")).await.unwrap();
	assert!(jane.is_none());
	let value = db.delete(Resource::from(("user", "jane"))).await.unwrap();
	assert_eq!(value, Value::None);
}

#[test_log::test(tokio::test)]
async fn delete_record_range() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let sql = "
        CREATE type::thing($table, 'amos') SET name = 'Amos';
        CREATE type::thing($table, 'jane') SET name = 'Jane';
        CREATE type::thing($table, 'john') SET name = 'John';
        CREATE type::thing($table, 'zoey') SET name = 'Zoey';
    ";
	let table = "user";
	let response = db.query(sql).bind(("table", table)).await.unwrap();
	response.check().unwrap();
	let users: Vec<RecordBuf> = db.delete(table).range("jane".."zoey").await.unwrap();
	assert_eq!(
		users,
		&[
			RecordBuf {
				id: thing("user:jane").unwrap(),
				name: "Jane".to_owned(),
			},
			RecordBuf {
				id: thing("user:john").unwrap(),
				name: "John".to_owned(),
			},
		]
	);
	let users: Vec<RecordBuf> = db.select(table).await.unwrap();
	assert_eq!(
		users,
		&[
			RecordBuf {
				id: thing("user:amos").unwrap(),
				name: "Amos".to_owned(),
			},
			RecordBuf {
				id: thing("user:zoey").unwrap(),
				name: "Zoey".to_owned(),
			},
		]
	);
}

#[test_log::test(tokio::test)]
async fn delete_record_large_range() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);

	let table = "user";

	for id in 0..2500 {
		let sql = format!("CREATE user:i{id:04} SET name = '{id}';");
		let response = db.query(sql).bind(("table", table)).await.unwrap();
		response.check().unwrap();
	}
	// The first record
	let users: Vec<RecordBuf> = db.delete(table).range("i0000".."i0001").await.unwrap();
	assert_eq!(users.len(), 1);
	// The last record
	let users: Vec<RecordBuf> = db.delete(table).range("i2499".."i9999").await.unwrap();
	assert_eq!(users.len(), 1);
	// 1015 record (TIKV: one full batch and one partial)
	let users: Vec<RecordBuf> = db.delete(table).range("i0010".."i1025").await.unwrap();
	assert_eq!(users.len(), 1015);
	// Every records
	let users: Vec<RecordBuf> = db.delete(table).range("i0000".."i9999").await.unwrap();
	assert_eq!(users.len(), 2500);
}

#[test_log::test(tokio::test)]
async fn changefeed() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	// Enable change feeds
	let sql = "
	DEFINE TABLE user CHANGEFEED 1h;
	";
	let response = db.query(sql).await.unwrap();
	drop(permit);
	response.check().unwrap();
	// Create and update users
	let sql = "
        CREATE user:amos SET name = 'Amos';
        CREATE user:jane SET name = 'Jane';
        UPDATE user:amos SET name = 'AMOS';
    ";
	let table = "user";
	let response = db.query(sql).await.unwrap();
	response.check().unwrap();
	let users: Vec<RecordBuf> = db
		.update(table)
		.content(Record {
			name: "Doe",
		})
		.await
		.unwrap();
	let expected = &[
		RecordBuf {
			id: thing("user:amos").unwrap(),
			name: "Doe".to_owned(),
		},
		RecordBuf {
			id: thing("user:jane").unwrap(),
			name: "Doe".to_owned(),
		},
	];
	assert_eq!(users, expected);
	let users: Vec<RecordBuf> = db.select(table).await.unwrap();
	assert_eq!(users, expected);
	let sql = "
        SHOW CHANGES FOR TABLE user SINCE 0 LIMIT 10;
    ";
	let mut response = db.query(sql).await.unwrap();
	let value: Value = response.take(0).unwrap();
	let Value::Array(array) = value.clone() else {
		unreachable!()
	};
	assert_eq!(array.len(), 5);
	// DEFINE TABLE
	let a = array.first().unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(_versionstamp1) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	let changes = a.get("changes").unwrap().to_owned();
	assert_eq!(
		changes,
		surrealdb::sql::value(
			"[
		{
			define_table: {
				name: 'user'
			}
		}
	]"
		)
		.unwrap()
	);
	// UPDATE user:amos
	let a = array.get(1).unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp1) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	let changes = a.get("changes").unwrap().to_owned();
	match FFLAGS.change_feed_live_queries.enabled() {
		true => {
			assert_eq!(
				changes,
				surrealdb::sql::value(
					r#"[
				 {
					  create: {
						  id: user:amos,
						  name: 'Amos'
					  }
				 }
			]"#
				)
				.unwrap()
			);
		}
		false => {
			assert_eq!(
				changes,
				surrealdb::sql::value(
					r#"[
				 {
					  update: {
						  id: user:amos,
						  name: 'Amos'
					  }
				 }
			]"#
				)
				.unwrap()
			);
		}
	}
	// UPDATE user:jane
	let a = array.get(2).unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp2) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	assert!(versionstamp1 < versionstamp2);
	let changes = a.get("changes").unwrap().to_owned();
	match FFLAGS.change_feed_live_queries.enabled() {
		true => {
			assert_eq!(
				changes,
				surrealdb::sql::value(
					"[
					{
						 create: {
							 id: user:jane,
							 name: 'Jane'
						 }
					}
				]"
				)
				.unwrap()
			);
		}
		false => {
			assert_eq!(
				changes,
				surrealdb::sql::value(
					"[
					{
						 update: {
							 id: user:jane,
							 name: 'Jane'
						 }
					}
				]"
				)
				.unwrap()
			);
		}
	}
	// UPDATE user:amos
	let a = array.get(3).unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp3) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	assert!(versionstamp2 < versionstamp3);
	let changes = a.get("changes").unwrap().to_owned();
	match FFLAGS.change_feed_live_queries.enabled() {
		true => {
			assert_eq!(
				changes,
				surrealdb::sql::value(
					"[
					{
						create: {
							id: user:amos,
							name: 'AMOS'
						}
					}
				]"
				)
				.unwrap()
			);
		}
		false => {
			assert_eq!(
				changes,
				surrealdb::sql::value(
					"[
					{
						update: {
							id: user:amos,
							name: 'AMOS'
						}
					}
				]"
				)
				.unwrap()
			);
		}
	};
	// UPDATE table
	let a = array.get(4).unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp4) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	assert!(versionstamp3 < versionstamp4);
	let changes = a.get("changes").unwrap().to_owned();
	assert_eq!(
		changes,
		surrealdb::sql::value(
			"[
		{
			update: {
				id: user:amos,
				name: 'Doe'
			}
		},
		{
			update: {
				id: user:jane,
				name: 'Doe'
			}
		}
	]"
		)
		.unwrap()
	);
}

#[test_log::test(tokio::test)]
async fn version() {
	let (permit, db) = new_db().await;
	drop(permit);
	db.version().await.unwrap();
}

#[test_log::test(tokio::test)]
async fn set_unset() {
	let (permit, db) = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	drop(permit);
	let (key, value) = ("name", "Doe");
	let sql = "RETURN $name";
	db.set(key, value).await.unwrap();
	let mut response = db.query(sql).await.unwrap();
	let Some(name): Option<String> = response.take(0).unwrap() else {
		panic!("record not found");
	};
	assert_eq!(name, value);
	// `token` is a reserved variable
	db.set("token", value).await.unwrap_err();
	// make sure we can still run queries after trying to set a protected variable
	db.query("RETURN true").await.unwrap().check().unwrap();
	db.unset(key).await.unwrap();
	let mut response = db.query(sql).await.unwrap();
	let name: Option<String> = response.take(0).unwrap();
	assert!(name.is_none());
}

#[test_log::test(tokio::test)]
async fn return_bool() {
	let (permit, db) = new_db().await;
	let mut response = db.query("RETURN true").await.unwrap();
	drop(permit);
	let Some(boolean): Option<bool> = response.take(0).unwrap() else {
		panic!("record not found");
	};
	assert!(boolean);
	let mut response = db.query("RETURN false").await.unwrap();
	let value: Value = response.take(0).unwrap();
	assert_eq!(value, Value::Bool(false));
}
