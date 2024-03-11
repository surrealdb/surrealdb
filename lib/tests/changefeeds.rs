use chrono::DateTime;

use helpers::new_ds;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;
use surrealdb_core::fflags::FFLAGS;
use surrealdb_core::kvs::Datastore;

mod helpers;
mod parse;

#[tokio::test]
async fn database_change_feeds() -> Result<(), Error> {
	let sql = "
	    DEFINE DATABASE test CHANGEFEED 1h;
        DEFINE TABLE person;
		DEFINE FIELD name ON TABLE person
			ASSERT
				IF $input THEN
					$input = /^[A-Z]{1}[a-z]+$/
				ELSE
					true
				END
			VALUE
				IF $input THEN
					'Name: ' + $input
				ELSE
					$value
				END
		;
	";
	let sql2 = "
		UPDATE person:test CONTENT { name: 'Tobie' };
		DELETE person:test;
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test-db-cf").with_db("test-db-cf");
	let start_ts = 0u64;
	let end_ts = start_ts + 1;
	dbs.tick_at(start_ts).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	dbs.tick_at(end_ts).await?;
	assert_eq!(res.len(), 6);
	// DEFINE DATABASE
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	// DEFINE TABLE
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	// DEFINE FIELD
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());

	let cf_val_arr = match FFLAGS.change_feed_live_queries.enabled() {
		true => Value::parse(
			"[
			{
				versionstamp: 65536,
				changes: [
					{
						create: {
							id: person:test,
							name: 'Name: Tobie'
						}
					}
				]
			},
			{
				versionstamp: 131072,
				changes: [
					{
						delete: {
							id: person:test
						}
					}
				]
			}
		]",
		),
		false => Value::parse(
			"[
			{
				versionstamp: 65536,
				changes: [
					{
						update: {
							id: person:test,
							name: 'Name: Tobie'
						}
					}
				]
			},
			{
				versionstamp: 131072,
				changes: [
					{
						delete: {
							id: person:test
						}
					}
				]
			}
		]",
		),
	};

	// Declare check that is repeatable
	async fn check_test(
		dbs: &Datastore,
		sql2: &str,
		ses: &Session,
		cf_val_arr: &Value,
	) -> Result<(), String> {
		let res = &mut dbs.execute(sql2, ses, None).await?;
		// UPDATE CONTENT
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
		);
		Some(tmp)
			.filter(|x| *x != val)
			.ok_or(format!("Expected the same value:\nleft: {}\nright: {}", tmp, val))?;
		// DELETE
		let tmp = res.remove(0).result?;
		let val = Value::parse("[]");
		Some(tmp)
			.filter(|x| *x != val)
			.ok_or(format!("Expected the same value:\nleft: {}\nright: {}", tmp, val))?;
		// SHOW CHANGES
		let tmp = res.remove(0).result?;
		Some(tmp)
			.filter(|x| x != cf_val_arr)
			.ok_or(format!("Expected the same value:\nleft: {}\nright: {}", tmp, val))?;
		Ok(())
	}

	// Check the validation with repeats
	for i in 0..5 {
		let test_result = check_test(&dbs, sql2, &ses, &cf_val_arr).await;
		match test_result {
			Ok(_) => break,
			Err(e) => {
				if i == 4 {
					panic!("Failed after retries: {}", e);
				}
				tokio::time::sleep(std::time::Duration::from_millis(100)).await;
			}
		}
	}
	// Retain for 1h
	let sql = "
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	dbs.tick_at(end_ts + 3599).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, cf_val_arr);
	// GC after 1hs
	dbs.tick_at(end_ts + 3600).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn table_change_feeds() -> Result<(), Error> {
	let sql = "
        DEFINE TABLE person CHANGEFEED 1h;
		DEFINE FIELD name ON TABLE person
			ASSERT
				IF $input THEN
					$input = /^[A-Z]{1}[a-z]+$/
				ELSE
					true
				END
			VALUE
				IF $input THEN
					'Name: ' + $input
				ELSE
					$value
				END
		;
		UPDATE person:test CONTENT { name: 'Tobie' };
		UPDATE person:test REPLACE { name: 'jaime' };
		UPDATE person:test MERGE { name: 'Jaime' };
		UPDATE person:test SET name = 'tobie';
		UPDATE person:test SET name = 'Tobie';
		DELETE person:test;
		CREATE person:1000 SET name = 'Yusuke';
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test-tb-cf").with_db("test-tb-cf");
	let start_ts = 0u64;
	let end_ts = start_ts + 1;
	dbs.tick_at(start_ts).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	dbs.tick_at(end_ts).await?;
	assert_eq!(res.len(), 10);
	// DEFINE TABLE
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	// DEFINE FIELD
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	// UPDATE CONTENT
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	// UPDATE REPLACE
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: jaime' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	// UPDATE MERGE
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Jaime',
			}
		]",
	);
	assert_eq!(tmp, val);
	// UPDATE SET
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: tobie' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	// UPDATE SET
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	);
	assert_eq!(tmp, val);
	// DELETE
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	// CREATE
	let _tmp = res.remove(0).result?;
	// SHOW CHANGES
	let tmp = res.remove(0).result?;
	let val = match FFLAGS.change_feed_live_queries.enabled() {
		true => Value::parse(
			"[
			{
				versionstamp: 65536,
				changes: [
					{
						define_table: {
							name: 'person'
						}
					}
				]
			},
			{
				versionstamp: 131072,
				changes: [
					{
						create: {
							id: person:test,
							name: 'Name: Tobie'
						}
					}
				]
			},
			{
				versionstamp: 196608,
				changes: [
					{
						update: {
							id: person:test,
							name: 'Name: Jaime'
						}
					}
				]
			},
			{
				versionstamp: 262144,
				changes: [
					{
						update: {
							id: person:test,
							name: 'Name: Tobie'
						}
					}
				]
			},
			{
				versionstamp: 327680,
				changes: [
					{
						delete: {
							id: person:test
						}
					}
				]
			},
			{
				versionstamp: 393216,
				changes: [
					{
						create: {
							id: person:1000,
							name: 'Name: Yusuke'
						}
					}
				]
			}
		]",
		),
		false => Value::parse(
			"[
			{
				versionstamp: 65536,
				changes: [
					{
						define_table: {
							name: 'person'
						}
					}
				]
			},
			{
				versionstamp: 131072,
				changes: [
					{
						update: {
							id: person:test,
							name: 'Name: Tobie'
						}
					}
				]
			},
			{
				versionstamp: 196608,
				changes: [
					{
						update: {
							id: person:test,
							name: 'Name: Jaime'
						}
					}
				]
			},
			{
				versionstamp: 262144,
				changes: [
					{
						update: {
							id: person:test,
							name: 'Name: Tobie'
						}
					}
				]
			},
			{
				versionstamp: 327680,
				changes: [
					{
						delete: {
							id: person:test
						}
					}
				]
			},
			{
				versionstamp: 393216,
				changes: [
					{
						update: {
							id: person:1000,
							name: 'Name: Yusuke'
						}
					}
				]
			}
		]",
		),
	};
	assert_eq!(tmp, val);
	// Retain for 1h
	let sql = "
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	dbs.tick_at(end_ts + 3599).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	assert_eq!(tmp, val);
	// GC after 1hs
	dbs.tick_at(end_ts + 3600).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn changefeed_with_ts() -> Result<(), Error> {
	let db = new_ds().await?;
	let ses = Session::owner().with_ns("test-cf-ts").with_db("test-cf-ts");
	// Enable change feeds
	let sql = "
	DEFINE TABLE user CHANGEFEED 1h;
	";
	db.execute(sql, &ses, None).await?.remove(0).result?;
	// Save timestamp 1
	let ts1_dt = "2023-08-01T00:00:00Z";
	let ts1 = DateTime::parse_from_rfc3339(ts1_dt).unwrap();
	db.tick_at(ts1.timestamp().try_into().unwrap()).await.unwrap();
	// Create and update users
	let sql = "
        CREATE user:amos SET name = 'Amos';
        CREATE user:jane SET name = 'Jane';
        UPDATE user:amos SET name = 'AMOS';
    ";
	let table = "user";
	let res = db.execute(sql, &ses, None).await?;
	for res in res {
		res.result?;
	}
	let sql = format!("UPDATE {table} SET name = 'Doe'");
	let users = db.execute(&sql, &ses, None).await?.remove(0).result?;
	let expected = Value::parse(
		"[
		{
			id: user:amos,
			name: 'Doe',
		},
		{
			id: user:jane,
			name: 'Doe',
		},
	]",
	);
	assert_eq!(users, expected);
	let sql = format!("SELECT * FROM {table}");
	let users = db.execute(&sql, &ses, None).await?.remove(0).result?;
	assert_eq!(users, expected);
	let sql = "
        SHOW CHANGES FOR TABLE user SINCE 0 LIMIT 10;
    ";
	let value: Value = db.execute(sql, &ses, None).await?.remove(0).result?;
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
	let Value::Number(versionstamp2) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
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
				name: 'Amos'
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
				name: 'Amos'
			}
		}
	]"
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
				name: 'AMOS'
			}
		}
	]"
		)
		.unwrap()
	);
	// UPDATE table
	let a = array.get(4).unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp5) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	assert!(versionstamp4 < versionstamp5);
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
	let ts2_dt = "2023-08-01T00:00:05Z";
	let ts2 = DateTime::parse_from_rfc3339(ts2_dt).unwrap();
	db.tick_at(ts2.timestamp().try_into().unwrap()).await.unwrap();
	//
	// Show changes using timestamp 1
	//
	let sql = format!("SHOW CHANGES FOR TABLE user SINCE d'{ts1_dt}' LIMIT 10; ");
	let value: Value = db.execute(&sql, &ses, None).await?.remove(0).result?;
	let Value::Array(array) = value.clone() else {
		unreachable!()
	};
	assert_eq!(array.len(), 4);
	// UPDATE user:amos
	let a = array.first().unwrap();
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp1b) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	assert!(versionstamp2 == versionstamp1b);
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
							 name: 'Amos'
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
							 name: 'Amos'
						 }
					}
				]"
				)
				.unwrap()
			);
		}
	}
	// Save timestamp 3
	let ts3_dt = "2023-08-01T00:00:10Z";
	let ts3 = DateTime::parse_from_rfc3339(ts3_dt).unwrap();
	db.tick_at(ts3.timestamp().try_into().unwrap()).await.unwrap();
	//
	// Show changes using timestamp 3
	//
	let sql = format!("SHOW CHANGES FOR TABLE user SINCE d'{ts3_dt}' LIMIT 10; ");
	let value: Value = db.execute(&sql, &ses, None).await?.remove(0).result?;
	let Value::Array(array) = value.clone() else {
		unreachable!()
	};
	assert_eq!(array.len(), 0);
	Ok(())
}
