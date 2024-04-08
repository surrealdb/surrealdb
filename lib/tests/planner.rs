mod parse;

use parse::Parse;
mod helpers;
use helpers::{new_ds, skip_ok};
use surrealdb::dbs::{Response, Session};
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_where_iterate_three_multi_index() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &three_multi_index_query("", ""), 12).await?;
	skip_ok(&mut res, 8)?;
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
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &three_multi_index_query("", "PARALLEL"), 12).await?;
	skip_ok(&mut res, 8)?;
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
	let dbs = new_ds().await?;
	let mut res = execute_test(
		&dbs,
		&three_multi_index_query("WITH INDEX uniq_name,idx_genre,ft_company", ""),
		12,
	)
	.await?;
	skip_ok(&mut res, 8)?;
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
	let dbs = new_ds().await?;
	let mut res =
		execute_test(&dbs, &three_multi_index_query("WITH INDEX ft_company", ""), 12).await?;
	skip_ok(&mut res, 8)?;

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
	let dbs = new_ds().await?;
	let mut res =
		execute_test(&dbs, &three_multi_index_query("WITH INDEX uniq_name", ""), 12).await?;
	skip_ok(&mut res, 8)?;

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
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &two_multi_index_query("", ""), 9).await?;
	skip_ok(&mut res, 5)?;
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
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &two_multi_index_query("WITH INDEX idx_genre", ""), 9).await?;
	skip_ok(&mut res, 5)?;
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
	let dbs = new_ds().await?;
	let mut res =
		execute_test(&dbs, &two_multi_index_query("WITH INDEX idx_genre,uniq_name", ""), 9).await?;
	skip_ok(&mut res, 5)?;
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
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &two_multi_index_query("WITH NOINDEX", ""), 9).await?;
	skip_ok(&mut res, 5)?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Tobie' }]")?;
	check_result(&mut res, &table_explain_no_index(2))?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, &table_explain_no_index(1))?;
	Ok(())
}

async fn execute_test(
	dbs: &Datastore,
	sql: &str,
	expected_result: usize,
) -> Result<Vec<Response>, Error> {
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), expected_result);
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
					type: 'Memory'
				}},
				operation: 'Collector'
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
					type: 'Memory'
				}},
				operation: 'Collector'
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
			type: 'Memory'
		},
		operation: 'Collector'
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
						type: 'Memory'
					},
					operation: 'Collector'
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
						type: 'Memory'
					},
					operation: 'Collector'
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
						type: 'Memory'
					},
					operation: 'Collector'
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
			type: 'Memory'
		},
		operation: 'Collector'
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
							type: 'Memory'
						},
						operation: 'Collector'
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
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
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
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
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
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &range_test(unique, from_incl, to_incl), 8).await?;
	skip_ok(&mut res, 6)?;
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
		},
		{
			detail: {
				type: 'Memory'
			},
			operation: 'Collector'
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
		},
		{
			detail: {
				type: 'Memory'
			},
			operation: 'Collector'
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
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
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
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
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
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &single_range_operator_test(unique, op), 6).await?;
	skip_ok(&mut res, 4)?;
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
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
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
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
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
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
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
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
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

#[tokio::test]
async fn select_with_idiom_param_value() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let sql = "
		CREATE person:tobie SET name = 'Tobie', genre='m', company='SurrealDB';
		CREATE person:jaime SET name = 'Jaime', genre='m', company='SurrealDB';
		DEFINE INDEX name ON TABLE person COLUMNS name UNIQUE;
		LET $name = 'Tobie';
		LET $nameObj = {{name:'Tobie'}};
		SELECT name FROM person WHERE name = $nameObj.name EXPLAIN;"
		.to_owned();
	let mut res = dbs.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
	skip_ok(&mut res, 5)?;
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
				{
					detail: {
						plan: {
							index: 'name',
							operator: '=',
							value: 'Tobie'
						},
						table: 'person'
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

const CONTAINS_CONTENT: &str = r#"
		CREATE student:1 CONTENT {
			marks: [
				{ subject: "maths", mark: 50 },
				{ subject: "english", mark: 40 },
				{ subject: "tamil", mark: 45 }
			]
		};
		CREATE student:2 CONTENT {
			marks: [
				{ subject: "maths", mark: 50 },
				{ subject: "english", mark: 35 },
				{ subject: "hindi", mark: 45 }
			]
		};
		CREATE student:3 CONTENT {
			marks: [
				{ subject: "maths", mark: 50 },
				{ subject: "hindi", mark: 30 },
				{ subject: "tamil", mark: 45 }
			]
		};"#;

const CONTAINS_TABLE_EXPLAIN: &str = r"[
				{
					detail: {
						table: 'student'
					},
					operation: 'Iterate Table'
					},
					{
						detail: {
							reason: 'NO INDEX FOUND'
						},
						operation: 'Fallback'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
				]";

async fn test_contains(
	dbs: &Datastore,
	sql: &str,
	index_explain: &str,
	result: &str,
) -> Result<(), Error> {
	let mut res = execute_test(dbs, sql, 5).await?;

	{
		let tmp = res.remove(0).result?;
		let val = Value::parse(CONTAINS_TABLE_EXPLAIN);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	{
		let tmp = res.remove(0).result?;
		let val = Value::parse(result);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	skip_ok(&mut res, 1)?;
	{
		let tmp = res.remove(0).result?;
		let val = Value::parse(index_explain);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	{
		let tmp = res.remove(0).result?;
		let val = Value::parse(result);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

#[tokio::test]
async fn select_contains() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, CONTAINS_CONTENT, 3).await?;
	skip_ok(&mut res, 3)?;

	const SQL: &str = r#"
		SELECT id FROM student WHERE marks.*.subject CONTAINS "english" EXPLAIN;
		SELECT id FROM student WHERE marks.*.subject CONTAINS "english";
		DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
		SELECT id FROM student WHERE marks.*.subject CONTAINS "english" EXPLAIN;
		SELECT id FROM student WHERE marks.*.subject CONTAINS "english";
	"#;

	const INDEX_EXPLAIN: &str = r"[
				{
					detail: {
						plan: {
							index: 'subject_idx',
							operator: '=',
							value: 'english'
						},
						table: 'student',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]";
	const RESULT: &str = r"[
		{
			id: student:1
		},
		{
			id: student:2
		}
	]";

	test_contains(&dbs, SQL, INDEX_EXPLAIN, RESULT).await
}

#[tokio::test]
async fn select_contains_all() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, CONTAINS_CONTENT, 3).await?;
	skip_ok(&mut res, 3)?;
	const SQL: &str = r#"
		SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"] EXPLAIN;
		SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];
		DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
		SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"] EXPLAIN;
		SELECT id FROM student WHERE marks.*.subject CONTAINSALL ["hindi", "maths"];
	"#;
	const INDEX_EXPLAIN: &str = r"[
				{
					detail: {
						plan: {
							index: 'subject_idx',
							operator: 'union',
							value: ['hindi', 'maths']
						},
						table: 'student',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]";
	const RESULT: &str = r"[
		{
			id: student:2
		},
		{
			id: student:3
		}
	]";

	test_contains(&dbs, SQL, INDEX_EXPLAIN, RESULT).await
}

#[tokio::test]
async fn select_contains_any() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, CONTAINS_CONTENT, 3).await?;
	skip_ok(&mut res, 3)?;
	const SQL: &str = r#"
		SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"] EXPLAIN;
		SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
		DEFINE INDEX subject_idx ON student COLUMNS marks.*.subject;
		SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"] EXPLAIN;
		SELECT id FROM student WHERE marks.*.subject CONTAINSANY ["tamil", "french"];
	"#;
	const INDEX_EXPLAIN: &str = r"[
				{
					detail: {
						plan: {
							index: 'subject_idx',
							operator: 'union',
							value: ['tamil', 'french']
						},
						table: 'student',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]";
	const RESULT: &str = r"[
		{
			id: student:1
		},
		{
			id: student:3
		}
	]";

	test_contains(&dbs, SQL, INDEX_EXPLAIN, RESULT).await
}

const CONTAINS_UNIQUE_CONTENT: &str = r#"
		CREATE student:1 CONTENT { subject: "maths", mark: 50 };
		CREATE student:2 CONTENT { subject: "english", mark: 35 };
		CREATE student:3 CONTENT { subject: "hindi", mark: 30 };"#;

#[tokio::test]
async fn select_unique_contains() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, CONTAINS_UNIQUE_CONTENT, 3).await?;
	skip_ok(&mut res, 3)?;

	const SQL: &str = r#"
		SELECT id FROM student WHERE subject CONTAINS "english" EXPLAIN;
		SELECT id FROM student WHERE subject CONTAINS "english";
		DEFINE INDEX subject_idx ON student COLUMNS subject UNIQUE;
		SELECT id FROM student WHERE subject CONTAINS "english" EXPLAIN;
		SELECT id FROM student WHERE subject CONTAINS "english";
	"#;

	const INDEX_EXPLAIN: &str = r"[
				{
					detail: {
						plan: {
							index: 'subject_idx',
							operator: '=',
							value: 'english'
						},
						table: 'student',
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]";
	const RESULT: &str = r"[
		{
			id: student:2
		}
	]";

	test_contains(&dbs, SQL, INDEX_EXPLAIN, RESULT).await
}

#[tokio::test]
// This test checks that:
// 1. Datetime are recognized by the query planner
// 2. we can take the value store in a variable
async fn select_with_datetime_value() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let sql = "
		DEFINE FIELD created_at ON TABLE test_user TYPE datetime;
		DEFINE INDEX createdAt ON TABLE test_user COLUMNS created_at;
		LET $now = d'2023-12-25T17:13:01.940183014Z';
		CREATE test_user:1 CONTENT { created_at: $now };
		SELECT * FROM test_user WHERE created_at = $now EXPLAIN;
		SELECT * FROM test_user WHERE created_at = d'2023-12-25T17:13:01.940183014Z' EXPLAIN;
		SELECT * FROM test_user WHERE created_at = $now;
		SELECT * FROM test_user WHERE created_at = d'2023-12-25T17:13:01.940183014Z';";
	let mut res = dbs.execute(sql, &ses, None).await?;

	assert_eq!(res.len(), 8);
	skip_ok(&mut res, 4)?;

	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			r#"[
				{
					detail: {
						plan: {
							index: 'createdAt',
							operator: '=',
							value: '2023-12-25T17:13:01.940183014Z'
						},
						table: 'test_user'
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]"#,
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			r#"[
				{
        			"created_at": "2023-12-25T17:13:01.940183014Z",
        			"id": test_user:1
    			}
			]"#,
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

#[tokio::test]
// This test checks that:
// 1. UUID are recognized by the query planner
// 2. we can take the value from a object stored as a variable
async fn select_with_uuid_value() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let sql = "
		DEFINE INDEX sessionUid ON sessions FIELDS sessionUid;
		CREATE sessions:1 CONTENT { sessionUid: u'00ad70db-f435-442e-9012-1cd853102084' };
		LET $sess = { uuid: u'00ad70db-f435-442e-9012-1cd853102084' };
		SELECT * FROM sessions WHERE sessionUid = u'00ad70db-f435-442e-9012-1cd853102084' EXPLAIN;
		SELECT * FROM sessions WHERE sessionUid = $sess.uuid EXPLAIN;
		SELECT * FROM sessions WHERE sessionUid = u'00ad70db-f435-442e-9012-1cd853102084';
		SELECT * FROM sessions WHERE sessionUid = $sess.uuid;
	";
	let mut res = dbs.execute(sql, &ses, None).await?;

	assert_eq!(res.len(), 7);
	skip_ok(&mut res, 3)?;

	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			r#"[
				{
					detail: {
						plan: {
							index: 'sessionUid',
							operator: '=',
							value: '00ad70db-f435-442e-9012-1cd853102084'
						},
						table: 'sessions'
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]"#,
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			r#"[
				{
               		"id": sessions:1,
 					"sessionUid": "00ad70db-f435-442e-9012-1cd853102084"
    			}
			]"#,
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	Ok(())
}

#[tokio::test]
async fn select_with_in_operator() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let sql = "
		DEFINE INDEX user_email_idx ON user FIELDS email;
		CREATE user:1 CONTENT { email: 'a@b' };
		CREATE user:2 CONTENT { email: 'c@d' };
		SELECT * FROM user WHERE email IN ['a@b', 'e@f'] EXPLAIN;
		SELECT * FROM user WHERE email INSIDE ['a@b', 'e@f'] EXPLAIN;
		SELECT * FROM user WHERE email IN ['a@b', 'e@f'];
		SELECT * FROM user WHERE email INSIDE ['a@b', 'e@f'];
		";
	let mut res = dbs.execute(sql, &ses, None).await?;

	assert_eq!(res.len(), 7);
	skip_ok(&mut res, 3)?;

	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			r#"[
				{
					detail: {
						plan: {
							index: 'user_email_idx',
							operator: 'union',
							value: ['a@b', 'e@f']
						},
						table: 'user'
					},
					operation: 'Iterate Index'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]"#,
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			r#"[
				{
               		'id': user:1,
 					'email': 'a@b'
    			}
			]"#,
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

#[tokio::test]
async fn select_with_in_operator_uniq_index() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let sql = r#"
		DEFINE INDEX apprenantUid ON apprenants FIELDS apprenantUid UNIQUE;
		CREATE apprenants:1 CONTENT { apprenantUid: "00013483-fedd-43e3-a94e-80728d896f6e" };
		SELECT apprenantUid FROM apprenants WHERE apprenantUid in [];
		SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["00013483-fedd-43e3-a94e-80728d896f6e"];
		SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["99999999-aaaa-1111-8888-abcdef012345", "00013483-fedd-43e3-a94e-80728d896f6e"];
		SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["00013483-fedd-43e3-a94e-80728d896f6e", "99999999-aaaa-1111-8888-abcdef012345"];
		SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["99999999-aaaa-1111-8888-abcdef012345", "00013483-fedd-43e3-a94e-80728d896f6e", "99999999-aaaa-1111-8888-abcdef012345"];
		SELECT apprenantUid FROM apprenants WHERE apprenantUid IN ["00013483-fedd-43e3-a94e-80728d896f6e"] EXPLAIN;
	"#;
	let mut res = dbs.execute(sql, &ses, None).await?;

	assert_eq!(res.len(), 8);
	skip_ok(&mut res, 2)?;

	let tmp = res.remove(0).result?;
	let val = Value::parse(r#"[]"#);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));

	for _ in 0..4 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			r#"[
			{
				apprenantUid: '00013483-fedd-43e3-a94e-80728d896f6e'
			}
		]"#,
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				detail: {
					plan: {
						index: 'apprenantUid',
						operator: 'union',
						value: [
							'00013483-fedd-43e3-a94e-80728d896f6e'
						]
					},
					table: 'apprenants'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
			}
		]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_with_in_operator_multiple_indexes() -> Result<(), Error> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	let sql = r#"
		DEFINE INDEX index_note_id ON TABLE notes COLUMNS id;
		DEFINE INDEX index_note_kind ON TABLE notes COLUMNS kind;
		DEFINE INDEX index_note_pubkey ON TABLE notes COLUMNS pubkey;
		DEFINE INDEX index_note_published ON TABLE notes COLUMNS published;
		CREATE notes:1 SET kind = 1, pubkey = 123, published=2021;
		CREATE notes:2 SET kind = 2, pubkey = 123, published=2022;
		CREATE notes:3 SET kind = 1, pubkey = 123, published=2023;
		CREATE notes:4 SET kind = 2, pubkey = 123, published=2024;
		CREATE notes:5 SET kind = 1, pubkey = 123, published=2025;
		SELECT * FROM notes WHERE (kind IN [1,2] OR pubkey IN [123]) AND published > 2022 EXPLAIN;
		SELECT * FROM notes WHERE (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
		SELECT * FROM notes WHERE published < 2024 AND (kind IN [1,2] OR pubkey IN [123]) AND published > 2022 EXPLAIN;
		SELECT * FROM notes WHERE published < 2024 AND (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
		SELECT * FROM notes WHERE published < 2022 OR (kind IN [1,2] OR pubkey IN [123]) AND published > 2022 EXPLAIN;
		SELECT * FROM notes WHERE published < 2022 OR (kind IN [1,2] OR pubkey IN [123]) AND published > 2022;
		SELECT * FROM notes WHERE (kind IN [1,2] AND published < 2022) OR (pubkey IN [123] AND published > 2022) EXPLAIN;
		SELECT * FROM notes WHERE (kind IN [1,2] AND published < 2022) OR (pubkey IN [123] AND published > 2022);
	"#;
	let mut res = dbs.execute(sql, &ses, None).await?;
	//
	assert_eq!(res.len(), 17);
	skip_ok(&mut res, 9)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
					{
						"detail": {
							"plan": {
								"index": "index_note_kind",
								"operator": "union",
								"value": [
									1,
									2
								]
							},
							"table": "notes"
						},
						"operation": "Iterate Index"
					},
					{
						"detail": {
							"plan": {
								"index": "index_note_pubkey",
								"operator": "union",
								"value": [
									123
								]
							},
							"table": "notes"
						},
						"operation": "Iterate Index"
					},
					{
						"detail": {
							plan: {
								from: {
									inclusive: false,
									value: 2022
								},
								index: 'index_note_published',
								to: {
									inclusive: false,
									value: None
								}
							},
							"table": "notes"
						},
						"operation": "Iterate Index"
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
				]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
				{
					id: notes:3,
					kind: 1,
					pubkey: 123,
					published: 2023
				},
				{
					id: notes:5,
					kind: 1,
					pubkey: 123,
					published: 2025
				},
				{
					id: notes:4,
					kind: 2,
					pubkey: 123,
					published: 2024
				}
			]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
					{
						"detail": {
							"plan": {
								"index": "index_note_kind",
								"operator": "union",
								"value": [
									1,
									2
								]
							},
							"table": "notes"
						},
						"operation": "Iterate Index"
					},
					{
						"detail": {
							"plan": {
								"index": "index_note_pubkey",
								"operator": "union",
								"value": [
									123
								]
							},
							"table": "notes"
						},
						"operation": "Iterate Index"
					},
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: 2022
								},
								index: 'index_note_published',
								to: {
									inclusive: false,
									value: 2024
								}
							},
							table: 'notes'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
				]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				id: notes:3,
				kind: 1,
				pubkey: 123,
				published: 2023
			}
		]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
					{
						"detail": {
							"plan": {
								"index": "index_note_kind",
								"operator": "union",
								"value": [
									1,
									2
								]
							},
							"table": "notes"
						},
						"operation": "Iterate Index"
					},
					{
						"detail": {
							"plan": {
								"index": "index_note_pubkey",
								"operator": "union",
								"value": [
									123
								]
							},
							"table": "notes"
						},
						"operation": "Iterate Index"
					},
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: None
								},
								index: 'index_note_published',
								to: {
									inclusive: false,
									value: 2022
								}
							},
							table: 'notes'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: 2022
								},
								index: 'index_note_published',
								to: {
									inclusive: false,
									value: None
								}
							},
							table: 'notes'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
				]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
				{
					id: notes:1,
					kind: 1,
					pubkey: 123,
					published: 2021
				},
				{
					id: notes:3,
					kind: 1,
					pubkey: 123,
					published: 2023
				},
				{
					id: notes:5,
					kind: 1,
					pubkey: 123,
					published: 2025
				},
				{
					id: notes:4,
					kind: 2,
					pubkey: 123,
					published: 2024
				}
			]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
					{
						"detail": {
							"plan": {
								"index": "index_note_kind",
								"operator": "union",
								"value": [
									1,
									2
								]
							},
							"table": "notes"
						},
						"operation": "Iterate Index"
					},
					{
						"detail": {
							"plan": {
								"index": "index_note_pubkey",
								"operator": "union",
								"value": [
									123
								]
							},
							"table": "notes"
						},
						"operation": "Iterate Index"
					},
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: None
								},
								index: 'index_note_published',
								to: {
									inclusive: false,
									value: 2022
								}
							},
							table: 'notes'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							plan: {
								from: {
									inclusive: false,
									value: 2022
								},
								index: 'index_note_published',
								to: {
									inclusive: false,
									value: None
								}
							},
							table: 'notes'
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
				]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
				{
					id: notes:1,
					kind: 1,
					pubkey: 123,
					published: 2021
				},
				{
					id: notes:3,
					kind: 1,
					pubkey: 123,
					published: 2023
				},
				{
					id: notes:5,
					kind: 1,
					pubkey: 123,
					published: 2025
				},
				{
					id: notes:4,
					kind: 2,
					pubkey: 123,
					published: 2024
				}
			]"#,
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}
