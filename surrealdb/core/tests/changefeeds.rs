use anyhow::Result;
use helpers::new_ds;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_types::{Array, Value};

mod helpers;

#[test_log::test(tokio::test)]
async fn database_change_feeds() -> Result<()> {
	// This is a unique shared identifier
	let identifier = "alpaca";
	let ns = format!("namespace_{identifier}");
	let db = format!("database_{identifier}");
	let sql = format!(
		"
	    DEFINE DATABASE OVERWRITE {db} CHANGEFEED 1h;
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
	let dbs = new_ds(ns.as_str(), db.as_str()).await?;
	let ses = Session::owner().with_ns(ns.as_str()).with_db(db.as_str());
	let res = &mut dbs.execute(sql.as_str(), &ses, None).await?;
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

	let res = &mut dbs.execute(sql2, &ses, None).await?;
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
	assert_eq!(tmp, val, "Expected UPDATE value");
	// DELETE
	let tmp = res.remove(0).result?;
	let val = Value::Array(Array::new());
	assert_eq!(tmp, val, "Expected DELETE value");
	// SHOW CHANGES
	let tmp = res.remove(0).result?;
	let Value::Array(changes_array) = tmp else {
		panic!("Expected array of changes");
	};
	assert_eq!(changes_array.len(), 2, "Expected 2 changesets");

	// First changeset: UPDATE
	let Value::Object(first) = &changes_array[0] else {
		panic!("Expected object");
	};
	let Value::Number(vs1) = first.get("versionstamp").expect("versionstamp") else {
		panic!("Expected versionstamp number");
	};
	let changes = first.get("changes").expect("changes");
	let expected_update =
		syn::value("[{ update: { id: person:test, name: 'Name: Tobie' } }]").unwrap();
	assert_eq!(changes, &expected_update, "First changeset should be UPDATE");

	// Second changeset: DELETE
	let Value::Object(second) = &changes_array[1] else {
		panic!("Expected object");
	};
	let Value::Number(vs2) = second.get("versionstamp").expect("versionstamp") else {
		panic!("Expected versionstamp number");
	};
	let changes = second.get("changes").expect("changes");
	let expected_delete = syn::value("[{ delete: { id: person:test } }]").unwrap();
	assert_eq!(changes, &expected_delete, "Second changeset should be DELETE");

	// Verify versionstamps are ordered
	assert!(vs1 < vs2, "Versionstamps should be ordered");

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
	let dbs = new_ds("test-tb-cf", "test-tb-cf").await?;
	let ses = Session::owner().with_ns("test-tb-cf").with_db("test-tb-cf");
	let res = &mut dbs.execute(sql, &ses, None).await?;
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
	let val = Value::Array(Array::new());
	assert_eq!(tmp, val);
	// CREATE
	let _tmp = res.remove(0).result?;
	// SHOW CHANGES
	let tmp = res.remove(0).result?;
	let Value::Array(changes_array) = tmp else {
		panic!("Expected array of changes");
	};

	// Verify we have 6 changesets (DEFINE TABLE, 3 UPDATEs, 1 DELETE, 1 CREATE)
	assert_eq!(changes_array.len(), 6, "Expected 6 changesets");

	// Verify DEFINE TABLE
	let Value::Object(cs0) = &changes_array[0] else {
		panic!("Expected object");
	};
	let changes = cs0.get("changes").expect("changes");
	let expected = syn::value(
		"[{ define_table: { id: 0, name: 'person', changefeed: { expiry: 1h, original: false }, drop: false, kind: { kind: 'ANY' }, permissions: { create: false, delete: false, select: false, update: false }, schemafull: false } }]"
	).unwrap();
	assert_eq!(changes, &expected, "First changeset should be DEFINE TABLE");

	// Verify first UPDATE (Tobie)
	let Value::Object(cs1) = &changes_array[1] else {
		panic!("Expected object");
	};
	let changes = cs1.get("changes").expect("changes");
	let expected = syn::value("[{ update: { id: person:test, name: 'Name: Tobie' } }]").unwrap();
	assert_eq!(changes, &expected, "Second changeset should be UPDATE Tobie");

	// Verify second UPDATE (Jaime)
	let Value::Object(cs2) = &changes_array[2] else {
		panic!("Expected object");
	};
	let changes = cs2.get("changes").expect("changes");
	let expected = syn::value("[{ update: { id: person:test, name: 'Name: Jaime' } }]").unwrap();
	assert_eq!(changes, &expected, "Third changeset should be UPDATE Jaime");

	// Verify third UPDATE (Tobie again)
	let Value::Object(cs3) = &changes_array[3] else {
		panic!("Expected object");
	};
	let changes = cs3.get("changes").expect("changes");
	let expected = syn::value("[{ update: { id: person:test, name: 'Name: Tobie' } }]").unwrap();
	assert_eq!(changes, &expected, "Fourth changeset should be UPDATE Tobie");

	// Verify DELETE
	let Value::Object(cs4) = &changes_array[4] else {
		panic!("Expected object");
	};
	let changes = cs4.get("changes").expect("changes");
	let expected = syn::value("[{ delete: { id: person:test } }]").unwrap();
	assert_eq!(changes, &expected, "Fifth changeset should be DELETE");

	// Verify CREATE person:1000
	let Value::Object(cs5) = &changes_array[5] else {
		panic!("Expected object");
	};
	let changes = cs5.get("changes").expect("changes");
	let expected = syn::value("[{ update: { id: person:1000, name: 'Name: Yusuke' } }]").unwrap();
	assert_eq!(changes, &expected, "Sixth changeset should be CREATE/UPDATE person:1000");

	// Verify versionstamps are ordered
	for i in 0..5 {
		let Value::Object(cs_i) = &changes_array[i] else {
			panic!("Expected object at index {}", i);
		};
		let Value::Object(cs_next) = &changes_array[i + 1] else {
			panic!("Expected object at index {}", i + 1);
		};
		let Value::Number(vs_i) = cs_i.get("versionstamp").expect("versionstamp") else {
			panic!("Expected versionstamp number");
		};
		let Value::Number(vs_next) = cs_next.get("versionstamp").expect("versionstamp") else {
			panic!("Expected versionstamp number");
		};
		assert!(vs_i < vs_next, "Versionstamps should be ordered");
	}

	Ok(())
}

#[tokio::test]
async fn changefeed_with_ts() -> Result<()> {
	let db = new_ds("test-cf-ts", "test-cf-ts").await?;
	let ses = Session::owner().with_ns("test-cf-ts").with_db("test-cf-ts");
	// Enable change feeds
	let sql = "
	DEFINE TABLE user CHANGEFEED 1h;
	";
	let mut res = db.execute(sql, &ses, None).await?;
	res.remove(0).result.unwrap();

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
	let Value::Number(versionstamp1) = a.get("versionstamp").unwrap() else {
		unreachable!()
	};
	let changes = a.get("changes").unwrap().to_owned();
	assert_eq!(
		changes,
		syn::value(
			"[
		{
			define_table: {
				id: 0,
				name: 'user',
				changefeed: {
					expiry: 1h,
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
	//
	// Show changes using versionstamp1 (should exclude DEFINE TABLE, return 4 items)
	//
	let vs1_int = versionstamp1.to_int().unwrap() as u64 + 1;
	let sql = format!("SHOW CHANGES FOR TABLE user SINCE {vs1_int} LIMIT 10; ");
	let value: Value = db.execute(&sql, &ses, None).await?.remove(0).result?;
	let Value::Array(array) = value.clone() else {
		unreachable!()
	};
	assert_eq!(array.len(), 4);

	//
	// Show changes using a versionstamp past all operations (should return 0 items)
	//
	// Use versionstamp5 (last operation) which was extracted earlier
	let vs5_int = versionstamp5.to_int().unwrap() as u64 + 1;
	let sql = format!("SHOW CHANGES FOR TABLE user SINCE {vs5_int} LIMIT 10; ");
	let value: Value = db.execute(&sql, &ses, None).await?.remove(0).result?;
	let Value::Array(array) = value else {
		unreachable!()
	};
	assert_eq!(array.len(), 0);
	Ok(())
}
