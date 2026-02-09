mod helpers;
use anyhow::Result;
use helpers::{new_ds, skip_ok};
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_types::{RecordId, Value};

use crate::helpers::Test;

#[tokio::test]
async fn select_array_group_group_by() -> Result<()> {
	let sql = "
		CREATE test:1 SET user = 1, role = 1;
        CREATE test:2 SET user = 1, role = 2;
        CREATE test:3 SET user = 2, role = 1;
        CREATE test:4 SET user = 2, role = 2;
        SELECT user, array::group(role) FROM test GROUP BY user;
	";
	let dbs = new_ds("test", "test").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	skip_ok(res, 4)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
                {
                        "array::group": [
                                1,2
                        ],
                        user: 1
                },
                {
                        "array::group": [
                                1,2
                        ],
                        user: 2
                }
        ]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

async fn select_count_group_all_permissions(
	perm: &str,
	expect_count_optim: Option<bool>,
	expect_result: &str,
) -> Result<()> {
	// Define the permissions
	let sql = format!(
		r"
				DEFINE TABLE OVERWRITE table PERMISSIONS {perm};
				CREATE table:baz CONTENT {{ bar: 'hello', foo: 'world'}};
			"
	);
	let mut t = Test::new(&sql).await?;
	t.expect_size(2)?;
	t.skip_ok(2)?;
	// Create and select as a record user
	let sql = r"
			SELECT COUNT() FROM table GROUP ALL EXPLAIN;
			SELECT COUNT() FROM table GROUP ALL;
			SELECT COUNT() FROM table:a..z EXPLAIN;
			SELECT COUNT() FROM table:a..z;
		";
	let mut t = Test::new_ds_session(
		t.ds,
		Session::for_record(
			"test",
			"test",
			"test",
			Value::RecordId(RecordId::new("table".to_string(), "baz".to_string())),
		),
		sql,
	)
	.await?;
	t.expect_size(4)?;
	// The explain plan is still visible
	let operation = match expect_count_optim {
		None => "",
		Some(true) => {
			"{
				detail: {
					direction: 'forward',
					table: 'table'
				},
				operation: 'Iterate Table Count'
			},"
		}
		Some(false) => {
			"{
				detail: {
					direction: 'forward',
					table: 'table'
				},
				operation: 'Iterate Table'
			},"
		}
	};
	t.expect_val(&format!(
		r"[
					{operation}
					{{
						detail: {{
							'Aggregate expressions': {{}},
							Aggregations: {{
								_a0: 'Count',
							}},
							'Group expressions': {{}},
							'Select expression': {{
								count: '_a0'
							}},
							type: 'Group'
						}},
						operation: 'Collector'
					}}
				]",
	))?;
	// Check what is returned
	t.expect_val(expect_result)?;
	// The explain plan is still visible
	let operation = match expect_count_optim {
		None => "",
		Some(true) => {
			"{
				detail: {
					direction: 'forward',
					range: 'a'..'z',
					table: 'table'
				},
				operation: 'Iterate Range Keys'
			},"
		}
		Some(false) => {
			"{
				detail: {
					direction: 'forward',
					range: 'a'..'z',
					table: 'table'
				},
				operation: 'Iterate Range'
			},"
		}
	};
	t.expect_val(&format!(
		r"[
				{operation}
				{{
					detail: {{
						type: 'Memory'
					}},
					operation: 'Collector'
				}}
			]",
	))?;
	// Check what is returned
	t.expect_val(expect_result)?;
	Ok(())
}

#[tokio::test]
async fn select_count_group_all_permissions_select_none() -> Result<()> {
	select_count_group_all_permissions("FOR SELECT NONE", None, "[]").await
}

#[tokio::test]
async fn select_count_group_all_permissions_select_full() -> Result<()> {
	select_count_group_all_permissions("FOR SELECT FULL", Some(true), "[{ count: 1}]").await
}

#[tokio::test]
async fn select_count_group_all_permissions_select_where_false() -> Result<()> {
	select_count_group_all_permissions("FOR SELECT WHERE FALSE", Some(false), "[]").await
}

async fn select_count_range_keys_only_permissions(
	perms: &str,
	expect_count_optim: Option<bool>,
	expect_group_all: &str,
	expect_count: &str,
) -> Result<()> {
	// Define the permissions and create some records
	let sql = format!(
		r"
			USE NS test DB test;
			DEFINE TABLE table PERMISSIONS {perms};
			SELECT COUNT() FROM table:a..z GROUP ALL;
			SELECT COUNT() FROM table:a..z;
			CREATE table:me CONTENT {{ bar: 'hello', foo: 'world'}};
			CREATE table:you CONTENT {{ bar: 'don\'t', foo: 'show up'}};
		"
	);
	let mut t = Test::new(&sql).await?;
	t.skip_ok(2).expect("failed to skip ok");
	// The first select should be successful
	t.expect_vals(&["[{count: 0}]", "[]"])?;
	//
	t.skip_ok(2).expect("failed to skip ok");
	// Create and select as a record user
	let sql = r"
			USE NS test DB test;
			SELECT COUNT() FROM table:a..z GROUP ALL EXPLAIN;
			SELECT COUNT() FROM table:a..z GROUP ALL;
			SELECT COUNT() FROM table:a..z EXPLAIN;
			SELECT COUNT() FROM table:a..z;
		";
	let mut t = Test::new_ds_session(
		t.ds,
		Session::for_record(
			"test",
			"test",
			"test",
			Value::RecordId(RecordId::new("table".to_owned(), "me".to_owned())),
		),
		sql,
	)
	.await?;
	t.expect_size(5)?;
	//
	t.skip_ok(1).expect("failed to skip ok");
	//
	// The explain plan is still accessible
	let operation = match expect_count_optim {
		None => "",
		Some(true) => {
			"{
				detail: {
					direction: 'forward',
					range: 'a'..'z',
					table: 'table'
				},
				operation: 'Iterate Range Count'
			},"
		}
		Some(false) => {
			"{
				detail: {
					direction: 'forward',
					range: 'a'..'z',
					table: 'table'
				},
				operation: 'Iterate Range'
			},"
		}
	};
	t.expect_val(&format!(
		r"[
				{operation}
				{{
					detail: {{
						'Aggregate expressions': {{}},
						Aggregations: {{
							_a0: 'Count',
						}},
						'Group expressions': {{}},
						'Select expression': {{
							count: '_a0'
						}},
						type: 'Group'
					}},
					operation: 'Collector'
				}}
			]"
	))?;
	// Check what is returned
	t.expect_val_info(expect_group_all, "GROUP ALL")?;
	// The explain plan is still accessible
	let operation = match expect_count_optim {
		None => "",
		Some(true) => {
			"{
				detail: {
					direction: 'forward',
					range: 'a'..'z',
					table: 'table'
				},
				operation: 'Iterate Range Keys'
			},"
		}
		Some(false) => {
			"{
				detail: {
					direction: 'forward',
					range: 'a'..'z',
					table: 'table'
				},
				operation: 'Iterate Range'
			},"
		}
	};
	t.expect_val(&format!(
		r"[
				{operation}
				{{
					detail: {{
						type: 'Memory'
					}},
					operation: 'Collector'
				}}
			]",
	))?;
	// Check what is returned
	t.expect_val_info(expect_count, "COUNT")?;
	Ok(())
}

#[tokio::test]
async fn select_count_range_keys_only_permissions_select_none() -> Result<()> {
	select_count_range_keys_only_permissions("FOR SELECT NONE", None, "[]", "[]").await
}

#[tokio::test]
async fn select_count_range_keys_only_permissions_select_full() -> Result<()> {
	select_count_range_keys_only_permissions(
		"FOR SELECT FULL",
		Some(true),
		"[{ count: 2 }]",
		"[{ count: 1 }, { count: 1 }]",
	)
	.await
}

#[tokio::test]
async fn select_count_range_keys_only_permissions_select_where_false() -> Result<()> {
	select_count_range_keys_only_permissions("FOR SELECT WHERE FALSE", Some(false), "[]", "[]")
		.await
}

#[tokio::test]
async fn select_count_range_only_permissions_select_where_match() -> Result<()> {
	select_count_range_keys_only_permissions(
		"FOR SELECT WHERE bar = 'hello'",
		Some(false),
		"[{ count: 1 }]",
		"[{ count: 1 }]",
	)
	.await
}
