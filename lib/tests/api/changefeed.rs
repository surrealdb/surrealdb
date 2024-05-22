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
