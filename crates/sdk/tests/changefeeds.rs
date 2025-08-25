use anyhow::anyhow;
use chrono::DateTime;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_core::kvs::LockType::Optimistic;
use surrealdb_core::kvs::TransactionType::Write;
use surrealdb_core::syn;
use surrealdb_core::val::{Array, Value};
use surrealdb_core::vs::VersionStamp;

mod helpers;

#[test_log::test(tokio::test)]
async fn database_change_feeds() -> Result<()> {
	// This is a unique shared identifier
	let identifier = "alpaca";
	let ns = format!("namespace_{identifier}");
	let db = format!("database_{identifier}");
	let sql = format!(
		"
	    DEFINE DATABASE {db} CHANGEFEED 1h;
        DEFINE TABLE person;
		DEFINE FIELD name ON TABLE person
			ASSERT
				IF $input THEN
					$input = /^[A-Z]{{1}}[a-z]+$/
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
	"
	);
	let sql2 = "
		UPSERT person:test CONTENT { name: 'Tobie' };
		DELETE person:test;
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns(ns.as_str()).with_db(db.as_str());
	let mut current_time = 0u64;
	dbs.changefeed_process_at(None, current_time).await?;
	let res = &mut dbs.execute(sql.as_str(), &ses, None).await?;
	// Increment by a second (sic)
	current_time += 1;
	dbs.changefeed_process_at(None, current_time).await?;
	assert_eq!(res.len(), 3);
	// DEFINE DATABASE
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// DEFINE TABLE
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// DEFINE FIELD
	let tmp = res.remove(0).result;
	tmp.unwrap();

	// Two timestamps
	let variance = 4;
	let first_timestamp = VersionStamp::ZERO.iter().take(variance);
	let second_timestamp = first_timestamp
		.flat_map(|vs1| vs1.iter().skip(1).take(variance).map(move |vs2| (vs1, vs2)));

	let potential_show_changes_values: Vec<Value> = second_timestamp
		.map(|(vs1, vs2)| {
			let vs1 = vs1.into_u128();
			let vs2 = vs2.into_u128();
			syn::value(
				format!(
					r#"[
						{{ versionstamp: {}, changes: [ {{ update: {{ id: person:test, name: 'Name: Tobie' }} }} ] }},
						{{ versionstamp: {}, changes: [ {{ delete: {{ id: person:test }} }} ] }}
						]"#,
					vs1, vs2
				)
				.as_str(),
			)
			.unwrap()
		})
		.collect();

	// Declare check that is repeatable
	async fn check_test(
		dbs: &Datastore,
		sql2: &str,
		ses: &Session,
		cf_val_arr: &[Value],
	) -> Result<()> {
		let res = &mut dbs.execute(sql2, ses, None).await?;
		assert_eq!(res.len(), 3);
		// UPDATE CONTENT
		let tmp = res.remove(0).result?;
		let val = syn::value(
			"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
		)
		.unwrap();
		Some(&tmp)
			.filter(|x| *x == &val)
			.map(|_v| ())
			.ok_or_else(|| anyhow!("Expected UPDATE value:\nleft: {}\nright: {}", tmp, val))?;
		// DELETE
		let tmp = res.remove(0).result?;
		let val = Array::new().into();
		Some(&tmp)
			.filter(|x| **x == val)
			.map(|_v| ())
			.ok_or_else(|| anyhow!("Expected DELETE value:\nleft: {}\nright: {}", tmp, val))?;
		// SHOW CHANGES
		let tmp = res.remove(0).result?;
		cf_val_arr
			.iter()
			.find(|x| *x == &tmp)
			// We actually dont want to capture if its found
			.map(|_v| ())
			.ok_or_else(|| {
				anyhow!(
					"Expected SHOW CHANGES value not found:\n{}\nin:\n{}",
					tmp,
					cf_val_arr
						.iter()
						.map(|vs| vs.to_string())
						.reduce(|left, right| format!("{}\n{}", left, right))
						.unwrap()
				)
			})?;
		Ok(())
	}

	// Check the validation with repeats
	let limit = 1;
	for i in 0..limit {
		let test_result = check_test(&dbs, sql2, &ses, &potential_show_changes_values).await;
		match test_result {
			Ok(_) => break,
			Err(e) => {
				if i == limit - 1 {
					panic!("Failed after retries: {}", e);
				}
				println!("Failed after retry {}:\n{}", i, e);
				tokio::time::sleep(std::time::Duration::from_millis(500)).await;
			}
		}
	}
	// Retain for 1h
	let sql = "
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	// This is neccessary to mark a point in time that can be GC'd
	current_time += 1;
	dbs.changefeed_process_at(None, current_time).await?;
	let tx = dbs.transaction(Write, Optimistic).await?;
	tx.cancel().await?;

	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	assert!(potential_show_changes_values.contains(&tmp));
	// GC after 1hs
	let one_hour_in_secs = 3600;
	current_time += one_hour_in_secs;
	current_time += 1;
	dbs.changefeed_process_at(None, current_time).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	let val: Value = Array::new().into();
	assert_eq!(val, tmp);
	//
	Ok(())
}

#[tokio::test]
async fn table_change_feeds() -> Result<()> {
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
		UPSERT person:test CONTENT { name: 'Tobie' };
		UPSERT person:test REPLACE { name: 'jaime' };
		UPSERT person:test MERGE { name: 'Jaime' };
		UPSERT person:test SET name = 'tobie';
		UPSERT person:test SET name = 'Tobie';
		DELETE person:test;
		CREATE person:1000 SET name = 'Yusuke';
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test-tb-cf").with_db("test-tb-cf");
	let start_ts = 0u64;
	let end_ts = start_ts + 1;
	dbs.changefeed_process_at(None, start_ts).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	dbs.changefeed_process_at(None, end_ts).await?;
	assert_eq!(res.len(), 10);
	// DEFINE TABLE
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// DEFINE FIELD
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// UPDATE CONTENT
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	// UPDATE REPLACE
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: jaime' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	// UPDATE MERGE
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Name: Jaime',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	// UPDATE SET
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Found 'Name: tobie' for field `name`, with record `person:test`, but field must conform to: IF $input THEN $input = /^[A-Z]{1}[a-z]+$/ ELSE true END"#
	));
	// UPDATE SET
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: person:test,
				name: 'Name: Tobie',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	// DELETE
	let tmp = res.remove(0).result?;
	let val = Array::new().into();
	assert_eq!(tmp, val);
	// CREATE
	let _tmp = res.remove(0).result?;
	// SHOW CHANGES
	let tmp = res.remove(0).result?;
	// If you want to write a macro, you are welcome to
	let limit_variance = 3;
	let first = VersionStamp::ZERO.iter().take(limit_variance);
	let second =
		first.flat_map(|vs1| vs1.iter().take(limit_variance).skip(1).map(move |vs2| (vs1, vs2)));
	let third = second.flat_map(|(vs1, vs2)| {
		vs2.iter().take(limit_variance).skip(1).map(move |vs3| (vs1, vs2, vs3))
	});
	let fourth = third.flat_map(|(vs1, vs2, vs3)| {
		vs3.iter().take(limit_variance).skip(1).map(move |vs4| (vs1, vs2, vs3, vs4))
	});
	let fifth = fourth.flat_map(|(vs1, vs2, vs3, vs4)| {
		vs4.iter().take(limit_variance).skip(1).map(move |vs5| (vs1, vs2, vs3, vs4, vs5))
	});
	let sixth = fifth.flat_map(|(vs1, vs2, vs3, vs4, vs5)| {
		vs5.iter().take(limit_variance).skip(1).map(move |vs6| (vs1, vs2, vs3, vs4, vs5, vs6))
	});
	let allowed_values: Vec<Value> = sixth
			.map(|(vs1, vs2, vs3, vs4, vs5, vs6)| {
				let (vs1, vs2, vs3, vs4, vs5, vs6) = (
					vs1.into_u128(),
					vs2.into_u128(),
					vs3.into_u128(),
					vs4.into_u128(),
					vs5.into_u128(),
					vs6.into_u128(),
				);
				// define_table: { changefeed: { expiry: '1h', original: false }, drop: false, kind: { kind: 'ANY' }, name: 'person', permissions: { create: false, select: false, update: false }, schemafull: false }
				syn::value(
					format!(
						r#"[
						{{ versionstamp: {vs1}, changes: [ {{ define_table: {{ name: 'person', changefeed: {{ expiry: '1h', original: false }}, drop: false, kind: {{ kind: 'ANY' }}, permissions: {{ create: false, delete: false, select: false, update: false }}, schemafull: false }} }} ] }},
						{{ versionstamp: {vs2}, changes: [ {{ update: {{ id: person:test, name: 'Name: Tobie' }} }} ] }},
						{{ versionstamp: {vs3}, changes: [ {{ update: {{ id: person:test, name: 'Name: Jaime' }} }} ] }},
						{{ versionstamp: {vs4}, changes: [ {{ update: {{ id: person:test, name: 'Name: Tobie' }} }} ] }},
						{{ versionstamp: {vs5}, changes: [ {{ delete: {{ id: person:test }} }} ] }},
						{{ versionstamp: {vs6}, changes: [ {{ update: {{ id: person:1000, name: 'Name: Yusuke' }} }} ] }}
						]"#
					)
					.as_str(),
				).unwrap()
			})
			.collect();
	assert!(
		allowed_values.contains(&tmp),
		"tmp:\n{}\nchecked:\n{}",
		tmp,
		allowed_values
			.iter()
			.map(|v| v.to_string())
			.reduce(|a, b| format!("{}\n{}", a, b))
			.unwrap()
	);
	// Retain for 1h
	let sql = "
        SHOW CHANGES FOR TABLE person SINCE 0;
	";
	dbs.changefeed_process_at(None, end_ts + 3599).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	assert!(
		allowed_values.contains(&tmp),
		"tmp:\n{}\nchecked:\n{}",
		tmp,
		allowed_values
			.iter()
			.map(|v| v.to_string())
			.reduce(|a, b| format!("{}\n{}", a, b))
			.unwrap()
	);
	// GC after 1hs
	dbs.changefeed_process_at(None, end_ts + 3600).await?;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	let tmp = res.remove(0).result?;
	let val = Array::new().into();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn changefeed_with_ts() -> Result<()> {
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
	db.changefeed_process_at(None, ts1.timestamp().try_into().unwrap()).await.unwrap();
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
	let expected = syn::value(
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
	)
	.unwrap();
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
		syn::value(
			"[
		{
			define_table: {
				name: 'user',
				changefeed: {
					expiry: '1h',
					original: false,
				},
				drop: false,
				kind: {
					kind: 'ANY',
				},
				permissions: {
					create: false,
					delete: false,
					select: false,
					update: false,
				},
				schemafull: false,
			}
		}
	]"
		)
		.unwrap()
	);
	// UPDATE user:amos
	let a = &array[1];
	let Value::Object(a) = a else {
		unreachable!()
	};
	let Value::Number(versionstamp2) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	let changes = a.get("changes").unwrap().to_owned();
	assert_eq!(
		changes,
		syn::value(
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
	let a = &array[2];
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
		syn::value(
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
	let a = &array[3];
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
		syn::value(
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
	let a = &array[4];
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
		syn::value(
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
	db.changefeed_process_at(None, ts2.timestamp().try_into().unwrap()).await.unwrap();
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
	assert_eq!(
		changes,
		syn::value(
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
	let ts3 = DateTime::parse_from_rfc3339(ts3_dt).unwrap();
	db.changefeed_process_at(None, ts3.timestamp().try_into().unwrap()).await.unwrap();
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
