// Tests common to all protocols and storage engines

#[tokio::test]
async fn connect() {
	let db = new_db().await;
	db.health().await.unwrap();
}

#[tokio::test]
async fn yuse() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
}

#[tokio::test]
async fn query() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let _ = db.query("
        CREATE user:john
        SET name = 'John Doe'
    ")
    .await
    .unwrap()
    .check()
    .unwrap();
	let mut response = db
        .query("SELECT name FROM user:john")
        .await
        .unwrap()
        .check()
        .unwrap();
    let Some(name): Option<String> = response.take("name").unwrap() else {
        panic!("query returned no record");
    };
    assert_eq!(name, "John Doe");
}

#[tokio::test]
async fn query_binds() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let mut response = db.query("CREATE user:john SET name = $name")
        .bind(("name", "John Doe"))
        .await
        .unwrap();
    let Some(record): Option<RecordName> = response.take(0).unwrap() else {
        panic!("query returned no record");
    };
    assert_eq!(record.name, "John Doe");
	let mut response = db.query("SELECT * FROM $record_id")
        .bind(("record_id", "user:john"))
        .await
        .unwrap();
    let Some(record): Option<RecordName> = response.take(0).unwrap() else {
        panic!("query returned no record");
    };
    assert_eq!(record.name, "John Doe");
	let mut response = db.query("CREATE user SET name = $name")
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

#[tokio::test]
async fn query_chaining() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let response = db
        .query(BeginStatement)
		.query("CREATE account:one SET balance = 135605.16")
		.query("CREATE account:two SET balance = 91031.31")
		.query("UPDATE account:one SET balance += 300.00")
		.query("UPDATE account:two SET balance -= 300.00")
		.query(CommitStatement)
		.await
		.unwrap();
    response.check().unwrap();
}

#[tokio::test]
async fn create_record_no_id() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let _: RecordId = db.create("user").await.unwrap();
}

#[tokio::test]
async fn create_record_with_id() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let _: RecordId = db.create(("user", "john")).await.unwrap();
}

#[tokio::test]
async fn create_record_no_id_with_content() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let _: RecordId = db
		.create("user")
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
}

#[tokio::test]
async fn create_record_with_id_with_content() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let record: RecordId = db
		.create(("user", "john"))
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
	assert_eq!(record.id, format!("user:john"));
}

#[tokio::test]
async fn select_table() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let table = "user";
	let _: RecordId = db.create(table).await.unwrap();
	let _: RecordId = db.create(table).await.unwrap();
	let _: RecordId = db.create(table).await.unwrap();
	let users: Vec<RecordId> = db.select(table).await.unwrap();
    assert_eq!(users.len(), 3);
}

#[tokio::test]
async fn select_record_id() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let record_id = ("user", "john");
	let _: RecordId = db.create(record_id).await.unwrap();
	let Some(record): Option<RecordId> = db.select(record_id).await.unwrap() else {
        panic!("record not found");
    };
    assert_eq!(record.id, "user:john");
}

#[tokio::test]
async fn select_record_ranges() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let table = "user";
	let _: RecordId = db.create((table, "amos")).await.unwrap();
	let _: RecordId = db.create((table, "jane")).await.unwrap();
	let _: RecordId = db.create((table, "john")).await.unwrap();
	let _: RecordId = db.create((table, "zoey")).await.unwrap();
	let convert = |users: Vec<RecordId>| -> Vec<String> {
		users
			.into_iter()
			.map(|user| user.id.split_once(':').unwrap().1.to_owned())
			.collect()
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
	let users: Vec<RecordId> =
		db.select(table).range((Bound::Excluded("jane"), Bound::Included("john"))).await.unwrap();
	assert_eq!(convert(users), vec!["john"]);
}

#[tokio::test]
async fn update_table() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let table = "user";
    let _: RecordId = db.create(table).await.unwrap();
    let _: RecordId = db.create(table).await.unwrap();
	let users: Vec<RecordId> = db.update(table).await.unwrap();
    assert_eq!(users.len(), 2);
}

#[tokio::test]
async fn update_record_id() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let table = "user";
    let _: RecordId = db.create((table, "john")).await.unwrap();
    let _: RecordId = db.create((table, "jane")).await.unwrap();
	let users: Vec<RecordId> = db.update(table).await.unwrap();
    assert_eq!(users.len(), 2);
}

#[tokio::test]
async fn update_table_with_content() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let sql = "
        CREATE type::thing($table, 'amos') SET name = 'Amos';
        CREATE type::thing($table, 'jane') SET name = 'Jane';
        CREATE type::thing($table, 'john') SET name = 'John';
        CREATE type::thing($table, 'zoey') SET name = 'Zoey';
    ";
	let table = "user";
    let response = db.query(sql)
        .bind(("table", table))
        .await
        .unwrap();
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
            id: "user:amos".to_owned(),
            name: "Doe".to_owned(),
        },
        RecordBuf {
            id: "user:jane".to_owned(),
            name: "Doe".to_owned(),
        },
        RecordBuf {
            id: "user:john".to_owned(),
            name: "Doe".to_owned(),
        },
        RecordBuf {
            id: "user:zoey".to_owned(),
            name: "Doe".to_owned(),
        },
    ];
    assert_eq!(users, expected);
	let users: Vec<RecordBuf> = db
		.select(table)
		.await
		.unwrap();
    assert_eq!(users, expected);
}

#[tokio::test]
async fn update_record_range_with_content() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let sql = "
        CREATE type::thing($table, 'amos') SET name = 'Amos';
        CREATE type::thing($table, 'jane') SET name = 'Jane';
        CREATE type::thing($table, 'john') SET name = 'John';
        CREATE type::thing($table, 'zoey') SET name = 'Zoey';
    ";
	let table = "user";
    let response = db.query(sql)
        .bind(("table", table))
        .await
        .unwrap();
    response.check().unwrap();
	let users: Vec<RecordBuf> = db
		.update(table)
		.range("jane".."zoey")
		.content(Record {
			name: "Doe",
		})
		.await
		.unwrap();
    assert_eq!(users, &[
        RecordBuf {
            id: "user:jane".to_owned(),
            name: "Doe".to_owned(),
        },
        RecordBuf {
            id: "user:john".to_owned(),
            name: "Doe".to_owned(),
        },
    ]);
	let users: Vec<RecordBuf> = db
		.select(table)
		.await
		.unwrap();
    assert_eq!(users, &[
        RecordBuf {
            id: "user:amos".to_owned(),
            name: "Amos".to_owned(),
        },
        RecordBuf {
            id: "user:jane".to_owned(),
            name: "Doe".to_owned(),
        },
        RecordBuf {
            id: "user:john".to_owned(),
            name: "Doe".to_owned(),
        },
        RecordBuf {
            id: "user:zoey".to_owned(),
            name: "Zoey".to_owned(),
        },
    ]);
}

#[tokio::test]
async fn update_record_id_with_content() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let record_id = ("user", "john");
	let user: RecordName = db
		.create(record_id)
		.content(Record {
			name: "Jane Doe",
		})
		.await
		.unwrap();
    assert_eq!(user.name, "Jane Doe");
	let user: RecordName = db
		.update(record_id)
		.content(Record {
			name: "John Doe",
		})
		.await
		.unwrap();
    assert_eq!(user.name, "John Doe");
	let user: RecordName = db
		.select(record_id)
		.await
		.unwrap();
    assert_eq!(user.name, "John Doe");
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
struct Name {
    first: Cow<'static, str>,
    last: Cow<'static, str>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
struct Person {
    #[serde(skip_serializing)]
    id: Option<String>,
    title: Cow<'static, str>,
    name: Name,
    marketing: bool,
}

#[tokio::test]
async fn merge_record_id() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let record_id = ("person", "jaime");
    let mut jaime: Person = db
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
    assert_eq!(jaime.id.unwrap(), "person:jaime");
    jaime = db
        .update(record_id)
        .merge(json!({ "marketing": true }))
        .await
        .unwrap();
    assert!(jaime.marketing);
    jaime = db.select(record_id).await.unwrap();
    assert_eq!(jaime, Person {
        id: Some("person:jaime".into()),
        title: "Founder & COO".into(),
        name: Name {
            first: "Jaime".into(),
            last: "Morgan Hitchcock".into(),
        },
        marketing: true,
    });
}

#[tokio::test]
async fn patch_record_id() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
	let id = "john";
	let _: Option<RecordId> = db
		.create(("user", id))
		.content(json!({
			"baz": "qux",
			"foo": "bar"
		}))
		.await
		.unwrap();
	let _: Option<serde_json::Value> = db
		.update(("user", id))
		.patch(PatchOp::replace("/baz", "boo"))
		.patch(PatchOp::add("/hello", ["world"]))
		.patch(PatchOp::remove("/foo"))
		.await
		.unwrap();
	let value: Option<serde_json::Value> = db.select(("user", id)).await.unwrap();
	assert_eq!(
		value,
		Some(json!({
			"id": format!("user:{id}"),
			"baz": "boo",
			"hello": ["world"]
		}))
	);
}

#[tokio::test]
async fn delete_table() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let table = "user";
	let _: RecordId = db.create(table).await.unwrap();
	let _: RecordId = db.create(table).await.unwrap();
	let _: RecordId = db.create(table).await.unwrap();
    let users: Vec<RecordId> = db.select(table).await.unwrap();
    assert_eq!(users.len(), 3);
	db.delete(table).await.unwrap();
    let users: Vec<RecordId> = db.select(table).await.unwrap();
    assert!(users.is_empty());
}

#[tokio::test]
async fn delete_record_id() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let record_id = ("user", "john");
	let _: RecordId = db.create(record_id).await.unwrap();
    let _: RecordId = db.select(record_id).await.unwrap();
	db.delete(record_id).await.unwrap();
    let john: Option<RecordId> = db.select(record_id).await.unwrap();
    assert!(john.is_none());
}

#[tokio::test]
async fn delete_record_range() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let sql = "
        CREATE type::thing($table, 'amos') SET name = 'Amos';
        CREATE type::thing($table, 'jane') SET name = 'Jane';
        CREATE type::thing($table, 'john') SET name = 'John';
        CREATE type::thing($table, 'zoey') SET name = 'Zoey';
    ";
	let table = "user";
    let response = db.query(sql)
        .bind(("table", table))
        .await
        .unwrap();
    response.check().unwrap();
	db.delete(table).range("jane".."zoey").await.unwrap();
	let users: Vec<RecordBuf> = db
		.select(table)
		.await
		.unwrap();
    assert_eq!(users, &[
        RecordBuf {
            id: "user:amos".to_owned(),
            name: "Amos".to_owned(),
        },
        RecordBuf {
            id: "user:zoey".to_owned(),
            name: "Zoey".to_owned(),
        },
    ]);
}

#[tokio::test]
async fn version() {
	let db = new_db().await;
	db.version().await.unwrap();
}

#[tokio::test]
async fn set_unset() {
	let db = new_db().await;
	db.use_ns(NS).use_db(Ulid::new().to_string()).await.unwrap();
    let (key, value) = ("name", "Doe");
    let sql = "RETURN $name";
	db.set(key, value).await.unwrap();
    let mut response = db.query(sql).await.unwrap();
    let Some(name): Option<String> = response.take(0).unwrap() else {
        panic!("record not found");
    };
    assert_eq!(name, value);
	db.unset(key).await.unwrap();
    let mut response = db.query(sql).await.unwrap();
    let name: Option<String> = response.take(0).unwrap();
    assert!(name.is_none());
}

#[tokio::test]
async fn return_bool() {
	let db = new_db().await;
	let mut response = db.query("RETURN true").await.unwrap();
    let Some(boolean): Option<bool> = response.take(0).unwrap() else {
        panic!("record not found");
    };
    assert!(boolean);
}
