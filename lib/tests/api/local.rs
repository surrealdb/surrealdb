#[tokio::test]
async fn changefeed_with_ts() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	// Enable change feeds
	let sql = "
	DEFINE TABLE user CHANGEFEED 1h;
	";
	let response = db.query(sql).await.unwrap();
	response.check().unwrap();
	// Save timestamp 1
	let ts1_dt = "2023-08-01T00:00:00Z";
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
	assert_eq!(array.len(), 4);
	// UPDATE user:amos
	let a = array.get(0).unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp1) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	let changes = a.get("changes").unwrap().to_owned();
	assert_eq!(
		changes,
		surrealdb::sql::value(
			"[
		{
			update: {
				id: user:amos,
				name: 'Amos'
			}
		}
	]"
		)
		.unwrap()
	);
	// UPDATE user:jane
	let a = array.get(1).unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp2) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	assert!(versionstamp1 < versionstamp2);
	let changes = a.get("changes").unwrap().to_owned();
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
	// UPDATE user:amos
	let a = array.get(2).unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp3) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	assert!(versionstamp2 < versionstamp3);
	let changes = a.get("changes").unwrap().to_owned();
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
	// UPDATE table
	let a = array.get(3).unwrap();
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
	// Save timestamp 2
	let _ts2_dt = "2023-08-01T00:00:05Z";
	//
	// Show changes using timestamp 1
	//
	let sql = format!(
		"
        SHOW CHANGES FOR TABLE user SINCE '{ts1_dt}' LIMIT 10;
    "
	);
	let mut response = db.query(sql).await.unwrap();
	let value: Value = response.take(0).unwrap();
	let Value::Array(array) = value.clone() else {
		unreachable!()
	};
	assert_eq!(array.len(), 4);
	// UPDATE user:amos
	let a = array.get(0).unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp1b) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	assert!(versionstamp1 == versionstamp1b);
	let changes = a.get("changes").unwrap().to_owned();
	assert_eq!(
		changes,
		surrealdb::sql::value(
			"[
		{
			update: {
				id: user:amos,
				name: 'Amos'
			}
		}
	]"
		)
		.unwrap()
	);
	// Save timestamp 3
	let ts3_dt = "2023-08-01T00:00:10Z";
	//
	// Show changes using timestamp 3
	//
	let sql = format!("SHOW CHANGES FOR TABLE user SINCE '{ts3_dt}' LIMIT 10;");
	let mut response = db.query(sql).await.unwrap();
	let value: Value = response.take(0).unwrap();
	let Value::Array(array) = value.clone() else {
		unreachable!()
	};
	assert_eq!(array.len(), 0);
}
