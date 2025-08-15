mod helpers;
use helpers::{new_ds, skip_ok};
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::val::RecordId;
use surrealdb_core::{strand, syn};

use crate::helpers::Test;

#[tokio::test]
async fn select_multi_aggregate_composed() -> Result<()> {
	let sql = "
		CREATE test:1 SET group = 1, one = 1.7, two = 2.4;
		CREATE test:2 SET group = 1, one = 4.7, two = 3.9;
		CREATE test:3 SET group = 2, one = 3.2, two = 9.7;
		CREATE test:4 SET group = 2, one = 4.4, two = 3.0;
		SELECT group, math::sum(math::floor(one)) AS one, math::sum(math::floor(two)) AS two FROM test GROUP BY group;
		SELECT group, math::sum(math::round(one)) AS one, math::sum(math::round(two)) AS two FROM test GROUP BY group;
		SELECT group, math::sum(math::ceil(one)) AS one, math::sum(math::ceil(two)) AS two FROM test GROUP BY group;
		SELECT group, math::sum(math::ceil(one)) AS one, math::sum(math::ceil(two)) AS two FROM test GROUP BY group EXPLAIN;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 8);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:1,
				group: 1,
				one: 1.7,
				two: 2.4,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:2,
				group: 1,
				one: 4.7,
				two: 3.9,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:3,
				group: 2,
				one: 3.2,
				two: 9.7,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: test:4,
				group: 2,
				one: 4.4,
				two: 3.0,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				group: 1,
				one: 5,
				two: 5,
			},
			{
				group: 2,
				one: 7,
				two: 12,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				group: 1,
				one: 7,
				two: 6,
			},
			{
				group: 2,
				one: 7,
				two: 13,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				group: 1,
				one: 7,
				two: 7,
			},
			{
				group: 2,
				one: 9,
				two: 13,
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					detail: {
						direction: 'forward',
						table: 'test'
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						idioms: {
							group: [
								'first'
							],
							one: [
								'math::sum'
							],
							two: [
								'math::sum'
							]
						},
						type: 'Group'
					},
					operation: 'Collector'
				}
			]",
	)
	.unwrap();
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	Ok(())
}

#[tokio::test]
async fn select_array_group_group_by() -> Result<()> {
	let sql = "
		CREATE test:1 SET user = 1, role = 1;
        CREATE test:2 SET user = 1, role = 2;
        CREATE test:3 SET user = 2, role = 1;
        CREATE test:4 SET user = 2, role = 2;
        SELECT user, array::group(role) FROM test GROUP BY user;
	";
	let dbs = new_ds().await?;
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
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	Ok(())
}

#[tokio::test]
async fn select_array_count_subquery_group_by() -> Result<()> {
	let sql = r#"
		CREATE table CONTENT { bar: "hello", foo: "Man"};
		CREATE table CONTENT { bar: "hello", foo: "World"};
		CREATE table CONTENT { bar: "world"};
		SELECT COUNT(foo != none) FROM table GROUP ALL EXPLAIN;
		SELECT COUNT(foo != none) FROM table GROUP ALL;
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	skip_ok(res, 3)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
				{
					detail: {
						direction: 'forward',
						table: 'table'
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						idioms: {
							count: [
								'count+func'
							]
						},
						type: 'Group'
					},
					operation: 'Collector'
				}
			]"#,
	)
	.unwrap();
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
					{
						count: 2
					}
				]"#,
	)
	.unwrap();
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	Ok(())
}

#[tokio::test]
async fn select_count_group_all() -> Result<()> {
	let sql = r#"
		CREATE table CONTENT { bar: "hello", foo: "Man"};
		CREATE table CONTENT { bar: "hello", foo: "World"};
		CREATE table CONTENT { bar: "world"};
		SELECT COUNT() FROM table GROUP ALL EXPLAIN;
		SELECT COUNT() FROM table GROUP ALL;
		SELECT COUNT() FROM table EXPLAIN;
		SELECT COUNT() FROM table;
	"#;
	let mut t = Test::new(sql).await?;
	t.expect_size(7)?;
	//
	t.skip_ok(3)?;
	//
	t.expect_val(
		r#"[
				{
					detail: {
						direction: 'forward',
						table: 'table'
					},
					operation: 'Iterate Table Count'
				},
				{
					detail: {
						idioms: {
							count: [
								'count'
							]
						},
						type: 'Group'
					},
					operation: 'Collector'
				}
			]"#,
	)?;
	//
	t.expect_val(
		r#"[
					{
						count: 3
					}
				]"#,
	)?;
	//
	t.expect_val(
		r#"[
					{
						detail: {
							direction: 'forward',
							table: 'table'
						},
						operation: 'Iterate Table Keys'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
				]"#,
	)?;
	//
	t.expect_val(
		r#"[
				{
					count: 1
				},
				{
					count: 1
				},
				{
					count: 1
				}
			]"#,
	)?;
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
				DEFINE TABLE table PERMISSIONS {perm};
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
			RecordId::new("table".to_owned(), strand!("baz").to_owned()).into(),
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
							idioms: {{
								count: [
									'count'
								]
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

#[tokio::test]
async fn select_count_range_keys_only() -> Result<()> {
	let sql = r#"
		CREATE table:1 CONTENT { bar: "hello", foo: "Man"};
		CREATE table:2 CONTENT { bar: "hello", foo: "World"};
		CREATE table:3 CONTENT { bar: "world"};
		SELECT COUNT() FROM table:1..4 GROUP ALL EXPLAIN;
		SELECT COUNT() FROM table:1..4 GROUP ALL;
		SELECT COUNT() FROM table:1..4 EXPLAIN;
		SELECT COUNT() FROM table:1..4;
	"#;
	let mut t = Test::new(sql).await?;
	t.expect_size(7)?;
	//
	t.skip_ok(3)?;
	//
	t.expect_val(
		r#"[
				{
					detail: {
						direction: 'forward',
						range: 1..4,
						table: 'table'
					},
					operation: 'Iterate Range Count'
				},
				{
					detail: {
						idioms: {
							count: [
								'count'
							]
						},
						type: 'Group'
					},
					operation: 'Collector'
				}
			]"#,
	)?;
	//
	t.expect_val(
		r#"[
					{
						count: 3
					}
				]"#,
	)?;
	//
	t.expect_val(
		r#"[
					{
						detail: {
							direction: 'forward',
							range: 1..4,
							table: 'table'
						},
						operation: 'Iterate Range Keys'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
				]"#,
	)?;
	//
	t.expect_val(
		r#"[
				{
					count: 1
				},
				{
					count: 1
				},
				{
					count: 1
				}
			]"#,
	)?;
	Ok(())
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
			SELECT COUNT() FROM table:a..z GROUP ALL;
			SELECT COUNT() FROM table:a..z;
			DEFINE TABLE table PERMISSIONS {perms};
			CREATE table:me CONTENT {{ bar: 'hello', foo: 'world'}};
			CREATE table:you CONTENT {{ bar: 'don\'t', foo: 'show up'}};
		"
	);
	let mut t = Test::new(&sql).await?;
	t.skip_ok(1)?;
	// The first select should be successful
	t.expect_vals(&["[{count: 0}]", "[]"])?;
	//
	t.skip_ok(3)?;
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
			RecordId::new("table".to_owned(), strand!("me").to_owned()).into(),
		),
		sql,
	)
	.await?;
	t.expect_size(5)?;
	//
	t.skip_ok(1)?;
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
						idioms: {{
							count: [
								'count'
							]
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
