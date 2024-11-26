mod parse;
use parse::Parse;
mod helpers;
use crate::helpers::Test;
use helpers::new_ds;
use helpers::skip_ok;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;
use surrealdb_core::sql::Thing;

#[tokio::test]
async fn select_aggregate() -> Result<(), Error> {
	let sql = "
		CREATE temperature:1 SET country = 'GBP', time = d'2020-01-01T08:00:00Z';
		CREATE temperature:2 SET country = 'GBP', time = d'2020-02-01T08:00:00Z';
		CREATE temperature:3 SET country = 'GBP', time = d'2020-03-01T08:00:00Z';
		CREATE temperature:4 SET country = 'GBP', time = d'2021-01-01T08:00:00Z';
		CREATE temperature:5 SET country = 'GBP', time = d'2021-01-01T08:00:00Z';
		CREATE temperature:6 SET country = 'EUR', time = d'2021-01-01T08:00:00Z';
		CREATE temperature:7 SET country = 'USD', time = d'2021-01-01T08:00:00Z';
		CREATE temperature:8 SET country = 'AUD', time = d'2021-01-01T08:00:00Z';
		CREATE temperature:9 SET country = 'CHF', time = d'2023-01-01T08:00:00Z';
		SELECT *, time::year(time) AS year FROM temperature;
		SELECT count(), time::min(time) as min, time::max(time) as max, time::year(time) AS year, country FROM temperature GROUP BY country, year;
		SELECT count(), time::min(time) as min, time::max(time) as max, time::year(time) AS year, country FROM temperature GROUP BY country, year EXPLAIN;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 12);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:1,
				time: d'2020-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:2,
				time: d'2020-02-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:3,
				time: d'2020-03-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:4,
				time: d'2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:5,
				time: d'2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'EUR',
				id: temperature:6,
				time: d'2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'USD',
				id: temperature:7,
				time: d'2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'AUD',
				id: temperature:8,
				time: d'2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'CHF',
				id: temperature:9,
				time: d'2023-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:1,
				time: d'2020-01-01T08:00:00Z',
				year: 2020
			},
			{
				country: 'GBP',
				id: temperature:2,
				time: d'2020-02-01T08:00:00Z',
				year: 2020
			},
			{
				country: 'GBP',
				id: temperature:3,
				time: d'2020-03-01T08:00:00Z',
				year: 2020
			},
			{
				country: 'GBP',
				id: temperature:4,
				time: d'2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'GBP',
				id: temperature:5,
				time: d'2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'EUR',
				id: temperature:6,
				time: d'2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'USD',
				id: temperature:7,
				time: d'2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'AUD',
				id: temperature:8,
				time: d'2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'CHF',
				id: temperature:9,
				time: d'2023-01-01T08:00:00Z',
				year: 2023
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					count: 1,
					country: 'AUD',
					max: d'2021-01-01T08:00:00Z',
					min: d'2021-01-01T08:00:00Z',
					year: 2021
				},
				{
					count: 1,
					country: 'CHF',
					max: d'2023-01-01T08:00:00Z',
					min: d'2023-01-01T08:00:00Z',
					year: 2023
				},
				{
					count: 1,
					country: 'EUR',
					max: d'2021-01-01T08:00:00Z',
					min: d'2021-01-01T08:00:00Z',
					year: 2021
				},
				{
					count: 3,
					country: 'GBP',
					max: d'2020-03-01T08:00:00Z',
					min: d'2020-01-01T08:00:00Z',
					year: 2020
				},
				{
					count: 2,
					country: 'GBP',
					max: d'2021-01-01T08:00:00Z',
					min: d'2021-01-01T08:00:00Z',
					year: 2021
				},
				{
					count: 1,
					country: 'USD',
					max: d'2021-01-01T08:00:00Z',
					min: d'2021-01-01T08:00:00Z',
					year: 2021
				}
			]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					detail: {
						table: 'temperature'
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						idioms: {
							count: [
								'count'
							],
							country: [
								'first'
							],
							max: [
								'time::max'
							],
							min: [
								'time::min'
							],
							year: [
								'array'
							]
						},
						type: 'Group'
					},
					operation: 'Collector'
				}
			]",
	);
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	Ok(())
}

#[tokio::test]
async fn select_multi_aggregate() -> Result<(), Error> {
	let sql = "
		CREATE test:1 SET group = 1, one = 1.7, two = 2.4;
		CREATE test:2 SET group = 1, one = 4.7, two = 3.9;
		CREATE test:3 SET group = 2, one = 3.2, two = 9.7;
		CREATE test:4 SET group = 2, one = 4.4, two = 3.0;
		SELECT group, math::sum(one) AS one, math::sum(two) AS two, math::min(one) as min FROM test GROUP BY group;
		SELECT group, math::sum(two) AS two, math::sum(one) AS one, math::max(two) as max, math::mean(one) as mean FROM test GROUP BY group;
		SELECT group, math::sum(two) AS two, math::sum(one) AS one, math::max(two) as max, math::mean(one) as mean FROM test GROUP BY group EXPLAIN;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:1,
				group: 1,
				one: 1.7,
				two: 2.4,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:2,
				group: 1,
				one: 4.7,
				two: 3.9,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:3,
				group: 2,
				one: 3.2,
				two: 9.7,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:4,
				group: 2,
				one: 4.4,
				two: 3.0,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					group: 1,
					min: 1.7,
					one: 6.4,
					two: 6.3
				},
				{
					group: 2,
					min: 3.2f,
					one: 7.6000000000000005,
					two: 12.7
				}
			]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					group: 1,
					max: 3.9,
					mean: 3.2,
					one: 6.4,
					two: 6.3
				},
				{
					group: 2,
					max: 9.7,
					mean: 3.8000000000000003,
					one: 7.6000000000000005,
					two: 12.7
				}
			]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					detail: {
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
							max: [
								'math::max'
							],
							mean: [
								'math::mean'
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
	);
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	Ok(())
}

#[tokio::test]
async fn select_multi_aggregate_composed() -> Result<(), Error> {
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
	let val = Value::parse(
		"[
			{
				id: test:1,
				group: 1,
				one: 1.7,
				two: 2.4,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:2,
				group: 1,
				one: 4.7,
				two: 3.9,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:3,
				group: 2,
				one: 3.2,
				two: 9.7,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:4,
				group: 2,
				one: 4.4,
				two: 3.0,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					detail: {
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
	);
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	Ok(())
}

#[tokio::test]
async fn select_array_group_group_by() -> Result<(), Error> {
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
	let val = Value::parse(
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
	);
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	Ok(())
}

#[tokio::test]
async fn select_array_count_subquery_group_by() -> Result<(), Error> {
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
	let val = Value::parse(
		r#"[
				{
					detail: {
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
	);
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
					{
						count: 2
					}
				]"#,
	);
	assert_eq!(format!("{tmp:#}"), format!("{val:#}"));
	//
	Ok(())
}

#[tokio::test]
async fn select_aggregate_mean_update() -> Result<(), Error> {
	let sql = "
		CREATE test:a SET a = 3;
		DEFINE TABLE foo AS SELECT
			math::mean(a) AS avg
		FROM test
		GROUP ALL;

		UPDATE test:a SET a = 2;

		SELECT avg FROM foo;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
		{
			id: test:a,
			a: 3
		}
	]",
	);

	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("None");

	assert_eq!(tmp, val);

	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:a,
				a: 2
			}
		]",
	);

	assert_eq!(tmp, val);

	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				avg: 2
			}
		]",
	);
	assert_eq!(tmp, val);

	Ok(())
}

#[tokio::test]
async fn select_count_group_all() -> Result<(), Error> {
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
						table: 'table'
					},
					operation: 'Iterate Table Keys'
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
	expect_keys_only: bool,
	expect_result: &str,
) -> Result<(), Error> {
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
		Session::for_record("test", "test", "test", Thing::from(("table", "baz")).into()),
		sql,
	)
	.await?;
	t.expect_size(4)?;
	// The explain plan is still visible
	let operation = if expect_keys_only {
		"Iterate Table Keys"
	} else {
		"Iterate Table"
	};
	t.expect_val(&format!(
		r"[
					{{
						detail: {{
							table: 'table'
						}},
						operation: '{operation}'
					}},
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
	let operation = if expect_keys_only {
		"Iterate Range Keys"
	} else {
		"Iterate Range"
	};
	t.expect_val(&format!(
		r"[
					{{
						detail: {{
							range: 'a'..'z',
							table: 'table'
						}},
						operation: '{operation}'
					}},
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
async fn select_count_group_all_permissions_select_none() -> Result<(), Error> {
	select_count_group_all_permissions("FOR SELECT NONE", true, "[]").await
}

#[tokio::test]
async fn select_count_group_all_permissions_select_full() -> Result<(), Error> {
	select_count_group_all_permissions("FOR SELECT FULL", true, "[{ count: 1}]").await
}

#[tokio::test]
async fn select_count_group_all_permissions_select_where_false() -> Result<(), Error> {
	select_count_group_all_permissions("FOR SELECT WHERE FALSE", false, "[]").await
}

#[tokio::test]
async fn select_count_range_keys_only() -> Result<(), Error> {
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
						range: 1..4,
						table: 'table'
					},
					operation: 'Iterate Range Keys'
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
	expect_keys_only: bool,
	expect_group_all: &str,
	expect_count: &str,
) -> Result<(), Error> {
	// Define the permissions and create some records
	let sql = format!(
		r"
			SELECT COUNT() FROM table:a..z GROUP ALL;
			SELECT COUNT() FROM table:a..z;
			DEFINE TABLE table PERMISSIONS {perms};
			CREATE table:me CONTENT {{ bar: 'hello', foo: 'world'}};
			CREATE table:you CONTENT {{ bar: 'don\'t', foo: 'show up'}};
		"
	);
	let mut t = Test::new(&sql).await?;
	// The first select should be successful (and empty) when the table does not exist
	t.expect_vals(&["[]", "[]"])?;
	//
	t.skip_ok(2)?;
	// Create and select as a record user
	let sql = r"
			SELECT COUNT() FROM table:a..z GROUP ALL EXPLAIN;
			SELECT COUNT() FROM table:a..z GROUP ALL;
			SELECT COUNT() FROM table:a..z EXPLAIN;
			SELECT COUNT() FROM table:a..z;
		";
	let mut t = Test::new_ds_session(
		t.ds,
		Session::for_record("test", "test", "test", Thing::from(("table", "me")).into()),
		sql,
	)
	.await?;
	t.expect_size(4)?;
	// The explain plan is still accessible
	let operation = if expect_keys_only {
		"Iterate Range Keys"
	} else {
		"Iterate Range"
	};
	t.expect_val(&format!(
		r"[
				{{
					detail: {{
						range: 'a'..'z',
						table: 'table'
					}},
					operation: '{operation}'
				}},
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
	t.expect_val(&format!(
		r"[
					{{
						detail: {{
							range: 'a'..'z',
							table: 'table'
						}},
						operation: '{operation}'
					}},
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
async fn select_count_range_keys_only_permissions_select_none() -> Result<(), Error> {
	select_count_range_keys_only_permissions("FOR SELECT NONE", true, "[]", "[]").await
}

#[tokio::test]
async fn select_count_range_keys_only_permissions_select_full() -> Result<(), Error> {
	select_count_range_keys_only_permissions(
		"FOR SELECT FULL",
		true,
		"[{ count: 2 }]",
		"[{ count: 1 }, { count: 1 }]",
	)
	.await
}

#[tokio::test]
async fn select_count_range_keys_only_permissions_select_where_false() -> Result<(), Error> {
	select_count_range_keys_only_permissions("FOR SELECT WHERE FALSE", false, "[]", "[]").await
}

#[tokio::test]
async fn select_count_range_only_permissions_select_where_match() -> Result<(), Error> {
	select_count_range_keys_only_permissions(
		"FOR SELECT WHERE bar = 'hello'",
		false,
		"[{ count: 1 }]",
		"[{ count: 1 }]",
	)
	.await
}
