mod parse;

use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::{Response, Session};
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_where_iterate_three_multi_index() -> Result<(), Error> {
	let mut res = execute_test(&three_multi_index_query("", ""), 12, 8).await?;
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Tobie' }, { name: 'Lizzie' }]")?;
	// OR results
	check_result(&mut res, THREE_MULTI_INDEX_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_FT_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_parallel() -> Result<(), Error> {
	let mut res = execute_test(&three_multi_index_query("", "PARALLEL"), 12, 8).await?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Tobie' }, { name: 'Lizzie' }]")?;
	check_result(&mut res, THREE_MULTI_INDEX_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_FT_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_with_all_index() -> Result<(), Error> {
	let mut res = execute_test(
		&three_multi_index_query("WITH INDEX uniq_name,idx_genre,ft_company", ""),
		12,
		8,
	)
	.await?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Tobie' }, { name: 'Lizzie' }]")?;
	check_result(&mut res, THREE_MULTI_INDEX_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_FT_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_with_one_ft_index() -> Result<(), Error> {
	let mut res =
		execute_test(&three_multi_index_query("WITH INDEX ft_company", ""), 12, 8).await?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' } ]")?;
	check_result(&mut res, THREE_TABLE_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_FT_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_with_one_index() -> Result<(), Error> {
	let mut res = execute_test(&three_multi_index_query("WITH INDEX uniq_name", ""), 12, 8).await?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' } ]")?;
	check_result(&mut res, THREE_TABLE_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_UNIQ_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_two_multi_index() -> Result<(), Error> {
	let mut res = execute_test(&two_multi_index_query("", ""), 9, 5).await?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Tobie' }]")?;
	check_result(&mut res, TWO_MULTI_INDEX_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_IDX_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_two_multi_index_with_one_index() -> Result<(), Error> {
	let mut res = execute_test(&two_multi_index_query("WITH INDEX idx_genre", ""), 9, 5).await?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Tobie' }]")?;
	check_result(&mut res, &table_explain(2))?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_IDX_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_two_multi_index_with_two_index() -> Result<(), Error> {
	let mut res =
		execute_test(&two_multi_index_query("WITH INDEX idx_genre,uniq_name", ""), 9, 5).await?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Tobie' }]")?;
	check_result(&mut res, TWO_MULTI_INDEX_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_IDX_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_two_no_index() -> Result<(), Error> {
	let mut res = execute_test(&two_multi_index_query("WITH NOINDEX", ""), 9, 5).await?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Tobie' }]")?;
	check_result(&mut res, &table_explain_no_index(2))?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, &table_explain_no_index(1))?;
	Ok(())
}

async fn execute_test(
	sql: &str,
	expected_result: usize,
	check_results: usize,
) -> Result<Vec<Response>, Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), expected_result);
	// Check that the setup is ok
	for _ in 0..check_results {
		let _ = res.remove(0).result?;
	}
	Ok(res)
}

fn check_result(res: &mut Vec<Response>, expected: &str) -> Result<(), Error> {
	let tmp = res.remove(0).result?;
	let val = Value::parse(expected);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

fn two_multi_index_query(with: &str, parallel: &str) -> String {
	format!(
		"CREATE person:tobie SET name = 'Tobie', genre='m', company='SurrealDB';
		CREATE person:jaime SET name = 'Jaime', genre='m', company='SurrealDB';
		CREATE person:lizzie SET name = 'Lizzie', genre='f', company='SurrealDB';
		DEFINE INDEX uniq_name ON TABLE person COLUMNS name UNIQUE;
		DEFINE INDEX idx_genre ON TABLE person COLUMNS genre;
		SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' {parallel};
	    SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' {parallel} EXPLAIN FULL;
		SELECT name FROM person {with} WHERE name = 'Jaime' AND genre = 'm' {parallel};
	    SELECT name FROM person {with} WHERE name = 'Jaime' AND genre = 'm' {parallel} EXPLAIN FULL;"
	)
}

fn three_multi_index_query(with: &str, parallel: &str) -> String {
	format!("
		CREATE person:tobie SET name = 'Tobie', genre='m', company='SurrealDB';
		CREATE person:jaime SET name = 'Jaime', genre='m', company='SurrealDB';
		CREATE person:lizzie SET name = 'Lizzie', genre='f', company='SurrealDB';
		CREATE person:neytiry SET name = 'Neytiri', genre='f', company='Metkayina';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX ft_company ON person FIELDS company SEARCH ANALYZER simple BM25;
		DEFINE INDEX uniq_name ON TABLE person COLUMNS name UNIQUE;
		DEFINE INDEX idx_genre ON TABLE person COLUMNS genre;
		SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' {parallel};
	    SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' {parallel} EXPLAIN FULL;
		SELECT name FROM person {with} WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' {parallel};
	    SELECT name FROM person {with} WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' {parallel} EXPLAIN FULL;")
}

fn table_explain(fetch_count: usize) -> String {
	format!(
		"[
			{{
				detail: {{
					table: 'person'
				}},
				operation: 'Iterate Table'
			}},
			{{
				detail: {{
					count: {fetch_count}
				}},
				operation: 'Fetch'
			}}
		]"
	)
}

fn table_explain_no_index(fetch_count: usize) -> String {
	format!(
		"[
			{{
				detail: {{
					table: 'person'
				}},
				operation: 'Iterate Table'
			}},
			{{
				detail: {{
					reason: 'WITH NOINDEX'
				}},
				operation: 'Fallback'
			}},
			{{
				detail: {{
					count: {fetch_count}
				}},
				operation: 'Fetch'
			}}
		]"
	)
}

const THREE_TABLE_EXPLAIN: &str = "[
	{
		detail: {
			table: 'person'
		},
		operation: 'Iterate Table'
	},
	{
		detail: {
			count: 3
		},
		operation: 'Fetch'
	}
]";

const THREE_MULTI_INDEX_EXPLAIN: &str = "[
				{
					detail: {
						plan: {
							index: 'uniq_name',
							operator: '=',
							value: 'Jaime'
						},
						table: 'person',
					},
					operation: 'Iterate Index'
				},
                {
					detail: {
						plan: {
							index: 'idx_genre',
							operator: '=',
							value: 'm'
						},
						table: 'person',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						plan: {
							index: 'ft_company',
							operator: '@@',
							value: 'surrealdb'
						},
						table: 'person',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						count: 3
					},
					operation: 'Fetch'
				}
			]";

const SINGLE_INDEX_FT_EXPLAIN: &str = "[
				{
					detail: {
						plan: {
							index: 'ft_company',
							operator: '@@',
							value: 'surrealdb'
						},
						table: 'person',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						count: 1
					},
					operation: 'Fetch'
				}
			]";

const SINGLE_INDEX_UNIQ_EXPLAIN: &str = "[
				{
					detail: {
						plan: {
							index: 'uniq_name',
							operator: '=',
							value: 'Jaime'
						},
						table: 'person',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						count: 1
					},
					operation: 'Fetch'
				}
			]";

const SINGLE_INDEX_IDX_EXPLAIN: &str = "[
	{
		detail: {
			plan: {
				index: 'idx_genre',
				operator: '=',
				value: 'm'
			},
			table: 'person'
		},
		operation: 'Iterate Index'
	},
	{
		detail: {
			count: 1
		},
		operation: 'Fetch'
	}
]";

const TWO_MULTI_INDEX_EXPLAIN: &str = "[
				{
					detail: {
						plan: {
							index: 'uniq_name',
							operator: '=',
							value: 'Jaime'
						},
						table: 'person',
					},
					operation: 'Iterate Index'
				},
                {
					detail: {
						plan: {
							index: 'idx_genre',
							operator: '=',
							value: 'm'
						},
						table: 'person',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						count: 2
					},
					operation: 'Fetch'
				}
			]";

#[tokio::test]
async fn select_with_no_index_unary_operator() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = dbs
		.execute("SELECT * FROM table WITH NOINDEX WHERE !param.subparam EXPLAIN", &ses, None)
		.await?;
	assert_eq!(res.len(), 1);
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
						reason: 'WITH NOINDEX'
					},
					operation: 'Fallback'
				}
			]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_unsupported_unary_operator() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res =
		dbs.execute("SELECT * FROM table WHERE !param.subparam EXPLAIN", &ses, None).await?;
	assert_eq!(res.len(), 1);
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
						reason: 'unary expressions not supported'
					},
					operation: 'Fallback'
				}
			]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

fn range_test(unique: bool, from_incl: bool, to_incl: bool) -> String {
	let from_op = if from_incl {
		">="
	} else {
		">"
	};
	let to_op = if to_incl {
		"<="
	} else {
		"<"
	};
	format!(
		"DEFINE INDEX year ON TABLE test COLUMNS year {};
	CREATE test:0 SET year = 2000;
	CREATE test:10 SET year = 2010;
	CREATE test:15 SET year = 2015;
	CREATE test:16 SET year = {};
	CREATE test:20 SET year = 2020;
	SELECT id FROM test WHERE year {} 2000 AND year {} 2020 EXPLAIN;
	SELECT id FROM test WHERE year {} 2000 AND year {} 2020;",
		if unique {
			"UNIQUE"
		} else {
			""
		},
		if unique {
			"2016"
		} else {
			"2015"
		},
		from_op,
		to_op,
		from_op,
		to_op,
	)
}

async fn select_range(
	unique: bool,
	from_incl: bool,
	to_incl: bool,
	explain: &str,
	result: &str,
) -> Result<(), Error> {
	let mut res = execute_test(&range_test(unique, from_incl, to_incl), 8, 6).await?;
	{
		let tmp = res.remove(0).result?;
		let val = Value::parse(explain);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	{
		let tmp = res.remove(0).result?;
		let val = Value::parse(result);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

const EXPLAIN_FROM_TO: &str = r"[
		{
			detail: {
				plan: {
					from: {
						inclusive: false,
						value: 2000
					},
					index: 'year',
					to: {
						inclusive: false,
						value: 2020
					}
				},
				table: 'test'
			},
			operation: 'Iterate Index'
		}
	]";

const RESULT_FROM_TO: &str = r"[
		{
			id: test:10,
		},
		{
			id: test:15,
		},
		{
			id: test:16,
		}
	]";
#[tokio::test]
async fn select_index_range_from_to() -> Result<(), Error> {
	select_range(false, false, false, EXPLAIN_FROM_TO, RESULT_FROM_TO).await
}

#[tokio::test]
async fn select_unique_range_from_to() -> Result<(), Error> {
	select_range(true, false, false, EXPLAIN_FROM_TO, RESULT_FROM_TO).await
}

const EXPLAIN_FROM_INCL_TO: &str = r"[
		{
			detail: {
				plan: {
					from: {
						inclusive: true,
						value: 2000
					},
					index: 'year',
					to: {
						inclusive: false,
						value: 2020
					}
				},
				table: 'test'
			},
			operation: 'Iterate Index'
		}
	]";

const RESULT_FROM_INCL_TO: &str = r"[
		{
			id: test:0,
		},
		{
			id: test:10,
		},
		{
			id: test:15,
		},
		{
			id: test:16,
		}
	]";

#[tokio::test]
async fn select_index_range_from_incl_to() -> Result<(), Error> {
	select_range(false, true, false, EXPLAIN_FROM_INCL_TO, RESULT_FROM_INCL_TO).await
}

#[tokio::test]
async fn select_unique_range_from_incl_to() -> Result<(), Error> {
	select_range(true, true, false, EXPLAIN_FROM_INCL_TO, RESULT_FROM_INCL_TO).await
}

const EXPLAIN_FROM_TO_INCL: &str = r"[
			{
				detail: {
					plan: {
						from: {
							inclusive: false,
							value: 2000
						},
						index: 'year',
						to: {
							inclusive: true,
							value: 2020
						}
					},
					table: 'test'
				},
				operation: 'Iterate Index'
			}
		]";

const RESULT_FROM_TO_INCL: &str = r"[
		{
			id: test:10,
		},
		{
			id: test:15,
		},
		{
			id: test:16,
		},
		{
			id: test:20,
		},
	]";

#[tokio::test]
async fn select_index_range_from_to_incl() -> Result<(), Error> {
	select_range(false, false, true, EXPLAIN_FROM_TO_INCL, RESULT_FROM_TO_INCL).await
}

#[tokio::test]
async fn select_unique_range_from_to_incl() -> Result<(), Error> {
	select_range(true, false, true, EXPLAIN_FROM_TO_INCL, RESULT_FROM_TO_INCL).await
}

const EXPLAIN_FROM_INCL_TO_INCL: &str = r"[
			{
				detail: {
					plan: {
						from: {
							inclusive: true,
							value: 2000
						},
						index: 'year',
						to: {
							inclusive: true,
							value: 2020
						}
					},
					table: 'test'
				},
				operation: 'Iterate Index'
			}
		]";

const RESULT_FROM_INCL_TO_INCL: &str = r"[
		{
			id: test:0,
		},
		{
			id: test:10,
		},
		{
			id: test:15,
		},
		{
			id: test:16,
		},
		{
			id: test:20,
		},
	]";

#[tokio::test]
async fn select_index_range_from_incl_to_incl() -> Result<(), Error> {
	select_range(false, true, true, EXPLAIN_FROM_INCL_TO_INCL, RESULT_FROM_INCL_TO_INCL).await
}

#[tokio::test]
async fn select_unique_range_from_incl_to_incl() -> Result<(), Error> {
	select_range(true, true, true, EXPLAIN_FROM_INCL_TO_INCL, RESULT_FROM_INCL_TO_INCL).await
}

fn single_range_operator_test(unique: bool, op: &str) -> String {
	format!(
		"DEFINE INDEX year ON TABLE test COLUMNS year {};
		CREATE test:10 SET year = 2010;
		CREATE test:15 SET year = 2015;
		CREATE test:20 SET year = 2020;
		SELECT id FROM test WHERE year {} 2015 EXPLAIN;
		SELECT id FROM test WHERE year {} 2015;",
		if unique {
			"UNIQUE"
		} else {
			""
		},
		op,
		op,
	)
}

async fn select_single_range_operator(
	unique: bool,
	op: &str,
	explain: &str,
	result: &str,
) -> Result<(), Error> {
	let mut res = execute_test(&single_range_operator_test(unique, op), 6, 4).await?;
	{
		let tmp = res.remove(0).result?;
		let val = Value::parse(explain);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	{
		let tmp = res.remove(0).result?;
		let val = Value::parse(result);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

const EXPLAIN_LESS: &str = r"[
			{
				detail: {
					plan: {
						from: {
							inclusive: false,
							value: None
						},
						index: 'year',
						to: {
							inclusive: false,
							value: 2015
						}
					},
					table: 'test'
				},
				operation: 'Iterate Index'
			}
		]";

const RESULT_LESS: &str = r"[
		{
			id: test:10,
		}
	]";
#[tokio::test]
async fn select_index_single_range_operator_less() -> Result<(), Error> {
	select_single_range_operator(false, "<", EXPLAIN_LESS, RESULT_LESS).await
}

#[tokio::test]
async fn select_unique_single_range_operator_less() -> Result<(), Error> {
	select_single_range_operator(true, "<", EXPLAIN_LESS, RESULT_LESS).await
}

const EXPLAIN_LESS_OR_EQUAL: &str = r"[
			{
				detail: {
					plan: {
						from: {
							inclusive: false,
							value: None
						},
						index: 'year',
						to: {
							inclusive: true,
							value: 2015
						}
					},
					table: 'test'
				},
				operation: 'Iterate Index'
			}
		]";

const RESULT_LESS_OR_EQUAL: &str = r"[
		{
			id: test:10,
		},
		{
			id: test:15,
		}
	]";

#[tokio::test]
async fn select_index_single_range_operator_less_or_equal() -> Result<(), Error> {
	select_single_range_operator(false, "<=", EXPLAIN_LESS_OR_EQUAL, RESULT_LESS_OR_EQUAL).await
}

#[tokio::test]
async fn select_unique_single_range_operator_less_or_equal() -> Result<(), Error> {
	select_single_range_operator(true, "<=", EXPLAIN_LESS_OR_EQUAL, RESULT_LESS_OR_EQUAL).await
}

const EXPLAIN_MORE: &str = r"[
			{
				detail: {
					plan: {
						from: {
							inclusive: false,
							value: 2015
						},
						index: 'year',
						to: {
							inclusive: false,
							value: None
						}
					},
					table: 'test'
				},
				operation: 'Iterate Index'
			}
		]";

const RESULT_MORE: &str = r"[
		{
			id: test:20,
		}
	]";
#[tokio::test]
async fn select_index_single_range_operator_more() -> Result<(), Error> {
	select_single_range_operator(false, ">", EXPLAIN_MORE, RESULT_MORE).await
}

#[tokio::test]
async fn select_unique_single_range_operator_more() -> Result<(), Error> {
	select_single_range_operator(true, ">", EXPLAIN_MORE, RESULT_MORE).await
}

const EXPLAIN_MORE_OR_EQUAL: &str = r"[
			{
				detail: {
					plan: {
						from: {
							inclusive: true,
							value: 2015
						},
						index: 'year',
						to: {
							inclusive: false,
							value: None
						}
					},
					table: 'test'
				},
				operation: 'Iterate Index'
			}
		]";

const RESULT_MORE_OR_EQUAL: &str = r"[
		{
			id: test:15,
		},
		{
			id: test:20,
		}
	]";

#[tokio::test]
async fn select_index_single_range_operator_more_or_equal() -> Result<(), Error> {
	select_single_range_operator(false, ">=", EXPLAIN_MORE_OR_EQUAL, RESULT_MORE_OR_EQUAL).await
}

#[tokio::test]
async fn select_unique_single_range_operator_more_or_equal() -> Result<(), Error> {
	select_single_range_operator(true, ">=", EXPLAIN_MORE_OR_EQUAL, RESULT_MORE_OR_EQUAL).await
}
