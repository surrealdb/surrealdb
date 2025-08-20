mod helpers;
use helpers::{new_ds, skip_ok};
use surrealdb::Result;
use surrealdb_core::dbs::{Response, Session};
use surrealdb_core::kvs::Datastore;
use surrealdb_core::syn;
use surrealdb_core::val::Value;

use crate::helpers::Test;

#[tokio::test]
async fn select_where_iterate_three_multi_index() -> Result<()> {
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &three_multi_index_query("", ""), 12).await?;
	skip_ok(&mut res, 8)?;
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' }]")?;
	// OR results
	check_result(&mut res, THREE_MULTI_INDEX_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_FT_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_parallel() -> Result<()> {
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &three_multi_index_query("", "PARALLEL"), 12).await?;
	skip_ok(&mut res, 8)?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' }]")?;
	check_result(&mut res, THREE_MULTI_INDEX_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_FT_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_with_all_index() -> Result<()> {
	let dbs = new_ds().await?;
	let mut res = execute_test(
		&dbs,
		&three_multi_index_query("WITH INDEX uniq_name,idx_genre,ft_company", ""),
		12,
	)
	.await?;
	skip_ok(&mut res, 8)?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' }]")?;
	check_result(&mut res, THREE_MULTI_INDEX_EXPLAIN)?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_FT_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_with_one_ft_index() -> Result<()> {
	let dbs = new_ds().await?;
	let mut res =
		execute_test(&dbs, &three_multi_index_query("WITH INDEX ft_company", ""), 12).await?;
	skip_ok(&mut res, 8)?;

	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' } ]")?;
	check_result(&mut res, &three_table_explain(false))?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_FT_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_with_one_index() -> Result<()> {
	let dbs = new_ds().await?;
	let mut res =
		execute_test(&dbs, &three_multi_index_query("WITH INDEX uniq_name", ""), 12).await?;
	skip_ok(&mut res, 8)?;

	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Lizzie' }, { name: 'Tobie' } ]")?;
	check_result(&mut res, &three_table_explain(false))?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, SINGLE_INDEX_UNIQ_EXPLAIN)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_two_multi_index() -> Result<()> {
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
async fn select_where_iterate_two_multi_index_with_one_index() -> Result<()> {
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
async fn select_where_iterate_two_multi_index_with_two_index() -> Result<()> {
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
async fn select_where_iterate_two_no_index() -> Result<()> {
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

async fn execute_test(dbs: &Datastore, sql: &str, expected_result: usize) -> Result<Vec<Response>> {
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), expected_result);
	Ok(res)
}

fn check_result(res: &mut Vec<Response>, expected: &str) -> Result<()> {
	let tmp = res.remove(0).result?;
	let val = syn::value(expected).unwrap();
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
		SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' ORDER BY name {parallel};
	    SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' ORDER BY name {parallel} EXPLAIN FULL;
		SELECT name FROM person {with} WHERE name = 'Jaime' AND genre = 'm' ORDER BY name {parallel};
	    SELECT name FROM person {with} WHERE name = 'Jaime' AND genre = 'm' ORDER BY name {parallel} EXPLAIN FULL;"
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
		SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name {parallel};
		SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' ORDER BY name {parallel} EXPLAIN FULL;
		SELECT name FROM person {with} WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name {parallel};
	    SELECT name FROM person {with} WHERE name = 'Jaime' AND genre = 'm' AND company @@ 'surrealdb' ORDER BY name {parallel} EXPLAIN FULL;")
}

fn table_explain(fetch_count: usize) -> String {
	format!(
		"[
			{{
				detail: {{
					direction: 'forward',
					table: 'person'
				}},
				operation: 'Iterate Table'
			}},
			{{
				detail: {{
					type: 'MemoryOrdered'
				}},
				operation: 'Collector'
			}},
			{{
				detail: {{
					type: 'KeysAndValues'
				}},
				operation: 'RecordStrategy'
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
					direction: 'forward',
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
					type: 'MemoryOrdered'
				}},
				operation: 'Collector'
			}},
			{{
				detail: {{
					type: 'KeysAndValues'
				}},
				operation: 'RecordStrategy'
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

fn three_table_explain(parallel: bool) -> String {
	let collector = if parallel {
		"AsyncMemoryOrdered"
	} else {
		"MemoryOrdered"
	};
	format!(
		"[
			{{
				detail: {{
					direction: 'forward',
					table: 'person'
				}},
				operation: 'Iterate Table'
			}},
			{{
				detail: {{
					type: '{collector}'
				}},
				operation: 'Collector'
			}},
			{{
				detail: {{
					type: 'KeysAndValues'
				}},
				operation: 'RecordStrategy'
			}},
			{{
				detail: {{
					count: 3
				}},
				operation: 'Fetch'
			}}
		]"
	)
}

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
						type: 'MemoryOrdered'
					},
					operation: 'Collector'
				},
				{
					detail: {
						type: 'KeysAndValues'
					},
					operation: 'RecordStrategy'
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
						type: 'MemoryOrdered'
					},
					operation: 'Collector'
				},
				{
					detail: {
						type: 'KeysAndValues'
					},
					operation: 'RecordStrategy'
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
						type: 'MemoryOrdered'
					},
					operation: 'Collector'
				},
				{
					detail: {
						type: 'KeysAndValues'
					},
					operation: 'RecordStrategy'
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
			type: 'MemoryOrdered'
		},
		operation: 'Collector'
	},
	{
		detail: {
			type: 'KeysAndValues'
		},
		operation: 'RecordStrategy'
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
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
				},
				{
					detail: {
						type: 'KeysAndValues'
					},
					operation: 'RecordStrategy'
				},
				{
					detail: {
						count: 2
					},
					operation: 'Fetch'
				}
			]";

#[tokio::test]
async fn select_with_no_index_unary_operator() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = dbs
		.execute(
			"DEFINE TABLE table; SELECT * FROM table WITH NOINDEX WHERE !param.subparam EXPLAIN",
			&ses,
			None,
		)
		.await?;
	assert_eq!(res.len(), 2);
	let tmp = res.remove(1).result?;
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_unsupported_unary_operator() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = dbs
		.execute(
			"DEFINE TABLE table; SELECT * FROM table WHERE !param.subparam EXPLAIN",
			&ses,
			None,
		)
		.await?;
	assert_eq!(res.len(), 2);
	let tmp = res.remove(1).result?;
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
						reason: 'Unsupported expression: !param.subparam'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_with_compound_index() -> Result<()> {
	let sql = r"
		DEFINE INDEX idxNameEvent ON event FIELDS name, event;
		DEFINE INDEX idxNameEventPath ON event FIELDS name, event, path;
		CREATE event:0 SET name = 'dummy', event = 'dummy', path = 'D/U/M';
		CREATE event:1 SET name = 'someName', event = 'event', path = 'A/B/C';
		SELECT * FROM event WITH INDEX idxNameEvent WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C' EXPLAIN;
		SELECT * FROM event WITH INDEX idxNameEventPath WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C' EXPLAIN;
		SELECT * FROM event WHERE name = 'someName' AND event = 'event' AND path= 'A/B/C' EXPLAIN;
		SELECT * FROM event WITH INDEX idxNameEvent WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
		SELECT * FROM event WITH INDEX idxNameEventPath WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
		SELECT * FROM event WHERE name = 'someName' AND event = 'event' AND path= 'A/B/C';";
	let mut t = Test::new(sql).await?;
	t.skip_ok(4)?;
	t.expect_val(
		"[
			{
				detail: {
					plan: {
						index: 'idxNameEvent',
						operator: '=',
						value: ['someName', 'event']
					},
					table: 'event'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
			}
		]",
	)?;
	for i in 0..2 {
		t.expect_val_info(
			"[
			{
				detail: {
					plan: {
						index: 'idxNameEventPath',
						operator: '=',
						value: ['someName', 'event', 'A/B/C']
					},
					table: 'event'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
			}
		]",
			i,
		)?;
	}
	for i in 0..3 {
		t.expect_val_info(
			"[
					{
						event: 'event',
						id: event:1,
						name: 'someName',
						path: 'A/B/C'
					}
				]",
			i,
		)?;
	}
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
) -> Result<()> {
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &range_test(unique, from_incl, to_incl), 8).await?;
	skip_ok(&mut res, 6)?;
	{
		let tmp = res.remove(0).result?;
		let val = syn::value(explain).unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	{
		let tmp = res.remove(0).result?;
		let val = syn::value(result).unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

const EXPLAIN_FROM_TO: &str = r"[
		{
			detail: {
				plan: {
				    direction: 'forward',
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
async fn select_index_range_from_to() -> Result<()> {
	select_range(false, false, false, EXPLAIN_FROM_TO, RESULT_FROM_TO).await
}

#[tokio::test]
async fn select_unique_range_from_to() -> Result<()> {
	select_range(true, false, false, EXPLAIN_FROM_TO, RESULT_FROM_TO).await
}

const EXPLAIN_FROM_INCL_TO: &str = r"[
		{
			detail: {
				plan: {
				    direction: 'forward',
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
async fn select_index_range_from_incl_to() -> Result<()> {
	select_range(false, true, false, EXPLAIN_FROM_INCL_TO, RESULT_FROM_INCL_TO).await
}

#[tokio::test]
async fn select_unique_range_from_incl_to() -> Result<()> {
	select_range(true, true, false, EXPLAIN_FROM_INCL_TO, RESULT_FROM_INCL_TO).await
}

const EXPLAIN_FROM_TO_INCL: &str = r"[
			{
				detail: {
					plan: {
					    direction: 'forward',
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
async fn select_index_range_from_to_incl() -> Result<()> {
	select_range(false, false, true, EXPLAIN_FROM_TO_INCL, RESULT_FROM_TO_INCL).await
}

#[tokio::test]
async fn select_unique_range_from_to_incl() -> Result<()> {
	select_range(true, false, true, EXPLAIN_FROM_TO_INCL, RESULT_FROM_TO_INCL).await
}

const EXPLAIN_FROM_INCL_TO_INCL: &str = r"[
			{
				detail: {
					plan: {
					    direction: 'forward',
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
async fn select_index_range_from_incl_to_incl() -> Result<()> {
	select_range(false, true, true, EXPLAIN_FROM_INCL_TO_INCL, RESULT_FROM_INCL_TO_INCL).await
}

#[tokio::test]
async fn select_unique_range_from_incl_to_incl() -> Result<()> {
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
) -> Result<()> {
	let dbs = new_ds().await?;
	let mut res = execute_test(&dbs, &single_range_operator_test(unique, op), 6).await?;
	skip_ok(&mut res, 4)?;
	{
		let tmp = res.remove(0).result?;
		let val = syn::value(explain).unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	{
		let tmp = res.remove(0).result?;
		let val = syn::value(result).unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

const EXPLAIN_LESS: &str = r"[
			{
				detail: {
					plan: {
					    direction: 'forward',
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
async fn select_index_single_range_operator_less() -> Result<()> {
	select_single_range_operator(false, "<", EXPLAIN_LESS, RESULT_LESS).await
}

#[tokio::test]
async fn select_unique_single_range_operator_less() -> Result<()> {
	select_single_range_operator(true, "<", EXPLAIN_LESS, RESULT_LESS).await
}

const EXPLAIN_LESS_OR_EQUAL: &str = r"[
			{
				detail: {
					plan: {
					    direction: 'forward',
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
async fn select_index_single_range_operator_less_or_equal() -> Result<()> {
	select_single_range_operator(false, "<=", EXPLAIN_LESS_OR_EQUAL, RESULT_LESS_OR_EQUAL).await
}

#[tokio::test]
async fn select_unique_single_range_operator_less_or_equal() -> Result<()> {
	select_single_range_operator(true, "<=", EXPLAIN_LESS_OR_EQUAL, RESULT_LESS_OR_EQUAL).await
}

const EXPLAIN_MORE: &str = r"[
			{
				detail: {
					plan: {
					    direction: 'forward',
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
async fn select_index_single_range_operator_more() -> Result<()> {
	select_single_range_operator(false, ">", EXPLAIN_MORE, RESULT_MORE).await
}

#[tokio::test]
async fn select_unique_single_range_operator_more() -> Result<()> {
	select_single_range_operator(true, ">", EXPLAIN_MORE, RESULT_MORE).await
}

const EXPLAIN_MORE_OR_EQUAL: &str = r"[
			{
				detail: {
					plan: {
					    direction: 'forward',
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
async fn select_index_single_range_operator_more_or_equal() -> Result<()> {
	select_single_range_operator(false, ">=", EXPLAIN_MORE_OR_EQUAL, RESULT_MORE_OR_EQUAL).await
}

#[tokio::test]
async fn select_unique_single_range_operator_more_or_equal() -> Result<()> {
	select_single_range_operator(true, ">=", EXPLAIN_MORE_OR_EQUAL, RESULT_MORE_OR_EQUAL).await
}

#[tokio::test]
async fn select_with_idiom_param_value() -> Result<()> {
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
	let val = syn::value(
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
	)
	.unwrap();
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
						direction: 'forward',
						table: 'student'
					},
					operation: 'Iterate Table'
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
) -> Result<()> {
	let mut res = execute_test(dbs, sql, 5).await?;

	{
		let tmp = res.remove(0).result?;
		let val = syn::value(CONTAINS_TABLE_EXPLAIN).unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	{
		let tmp = res.remove(0).result?;
		let val = syn::value(result).unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	skip_ok(&mut res, 1)?;
	{
		let tmp = res.remove(0).result?;
		let val = syn::value(index_explain).unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	{
		let tmp = res.remove(0).result?;
		let val = syn::value(result).unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

#[tokio::test]
async fn select_contains() -> Result<()> {
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
async fn select_contains_all() -> Result<()> {
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
async fn select_contains_any() -> Result<()> {
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
async fn select_unique_contains() -> Result<()> {
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
async fn select_with_datetime_value() -> Result<()> {
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
		let val = syn::value(
			r#"[
				{
					detail: {
						plan: {
							index: 'createdAt',
							operator: '=',
							value: d'2023-12-25T17:13:01.940183014Z'
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
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let val = syn::value(
			r#"[
				{
        			"created_at": d"2023-12-25T17:13:01.940183014Z",
        			"id": test_user:1
    			}
			]"#,
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

#[tokio::test]
// This test checks that:
// 1. UUID are recognized by the query planner
// 2. we can take the value from a object stored as a variable
async fn select_with_uuid_value() -> Result<()> {
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
		let val = syn::value(
			r#"[
				{
					detail: {
						plan: {
							index: 'sessionUid',
							operator: '=',
							value: u'00ad70db-f435-442e-9012-1cd853102084'
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
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let val = syn::value(
			r#"[
				{
               		"id": sessions:1,
 					"sessionUid": u"00ad70db-f435-442e-9012-1cd853102084"
    			}
			]"#,
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	Ok(())
}

#[tokio::test]
async fn select_with_in_operator() -> Result<()> {
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
		let val = syn::value(
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
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	for _ in 0..2 {
		let tmp = res.remove(0).result?;
		let val = syn::value(
			r#"[
				{
               		'id': user:1,
 					'email': 'a@b'
    			}
			]"#,
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	Ok(())
}

#[tokio::test]
async fn select_with_in_operator_uniq_index() -> Result<()> {
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
	let val = syn::value(r#"[]"#).unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));

	for _ in 0..4 {
		let tmp = res.remove(0).result?;
		let val = syn::value(
			r#"[
			{
				apprenantUid: '00013483-fedd-43e3-a94e-80728d896f6e'
			}
		]"#,
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}

	let tmp = res.remove(0).result?;
	let val = syn::value(
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_with_in_and_not_valid_compound_index() -> Result<()> {
	let sql = "
		DEFINE INDEX idx_account_type ON test FIELDS account, type;
		DEFINE INDEX idx_type_value ON test FIELDS type, value;
		CREATE test:1 CONTENT {
			account: accounts:1,
			type: 'password',
			value: '$argon2id$v=19$m=19456,t=2,p=1$qP0Y6ApAaFyeDsGySVD0DQ$Bp/Jv2uwapFUl5NKYk3uMsT5DZV0XPpMMVm3F2ZVfSM'
		};
		CREATE test:2 CONTENT {
			account: accounts:2,
			type: 'password',
			value: '$argon2id$v=19$m=19456,t=2,p=1$nwwBkmNrUjjfD1ny3W50Ow$ZyZcp9GaH5iQx3k8YUmR/R3nguJR9feFO1cewEGGjcM'
		};
		CREATE test:3 CONTENT {
			account: accounts:3,
			type: 'password',
			value: 'notAValidPassword'
		};
		SELECT * FROM test WITH INDEX idx_type_value WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'] EXPLAIN;
		SELECT * FROM test WITH INDEX idx_account_type WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'] EXPLAIN;
		SELECT * FROM test WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'] EXPLAIN;
		SELECT * FROM test WITH INDEX idx_type_value WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
		SELECT * FROM test WITH INDEX idx_account_type WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
		SELECT * FROM test WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
		SELECT * FROM test WITH NOINDEX WHERE account = accounts:1 AND type IN ['password', 'firebasePassword'];
	";
	let mut t = Test::new(sql).await?;
	t.expect_size(12)?;
	t.skip_ok(5)?;
	t.expect_val(
		"[
			{
				detail: {
					plan: {
						index: 'idx_type_value',
						operator: 'union',
						value: [
							'password',
							'firebasePassword'
						]
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
		]",
	)?;
	for i in 0..2 {
		t.expect_val_info(
			"[
				{
					detail: {
						plan: {
							index: 'idx_account_type',
							operator: 'union',
							value: [
								[
									accounts:1,
									'password'
								],
								[
									accounts:1,
									'firebasePassword'
								]
							]
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
			]",
			i,
		)?;
	}
	for i in 0..4 {
		t.expect_val_info("[
			{
				account: accounts:1,
				id: test:1,
				type: 'password',
				value: '$argon2id$v=19$m=19456,t=2,p=1$qP0Y6ApAaFyeDsGySVD0DQ$Bp/Jv2uwapFUl5NKYk3uMsT5DZV0XPpMMVm3F2ZVfSM'
			}
		]", i)?;
	}
	Ok(())
}

#[tokio::test]
async fn select_with_record_id_link_no_index() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	//
	let sql = "
		DEFINE FIELD name ON TABLE t TYPE string;
		DEFINE FIELD t ON TABLE i TYPE record<t>;
		CREATE t:1 SET name = 'h';
		CREATE t:2 SET name = 'h';
		CREATE i:A SET t = t:1;
		CREATE i:B SET t = t:2;
		SELECT * FROM i WHERE t.name = 'h';
		SELECT * FROM i WHERE t.name = 'h' EXPLAIN;
	";
	let mut res = dbs.execute(sql, &ses, None).await?;
	//
	assert_eq!(res.len(), 8);
	skip_ok(&mut res, 6)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
				{ "id": i:A, "t": t:1 },
				{ "id": i:B, "t": t:2 }
			]"#,
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
				{
					detail: {
						direction: 'forward',
						table: 'i'
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]"#,
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	Ok(())
}

#[tokio::test]
async fn select_with_record_id_link_index() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	//
	let sql = "
		DEFINE INDEX i_t_id ON TABLE i COLUMNS t;
		DEFINE INDEX t_name_idx ON TABLE t COLUMNS name;
		DEFINE FIELD name ON TABLE t TYPE string;
		DEFINE FIELD t ON TABLE i TYPE record<t>;
		CREATE t:1 SET name = 'h';
		CREATE t:2 SET name = 'h';
		CREATE i:A SET t = t:1;
		CREATE i:B SET t = t:2;
		SELECT * FROM i WHERE t.name = 'h' EXPLAIN;
		SELECT * FROM i WHERE t.name = 'h';
	";
	let mut res = dbs.execute(sql, &ses, None).await?;
	//
	assert_eq!(res.len(), 10);
	skip_ok(&mut res, 8)?;
	//
	let expected = syn::value(
		r#"[
				{ "id": i:A, "t": t:1 },
				{ "id": i:B, "t": t:2 }
			]"#,
	)
	.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
					{
						detail: {
							plan: {
								index: 'i_t_id',
								joins: [
									{
										index: 't_name_idx',
										operator: '=',
										value: 'h'
									}
								],
								operator: 'join'
							},
							table: 'i'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", expected));
	//
	Ok(())
}

#[tokio::test]
async fn select_with_record_id_link_unique_index() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	//
	let sql = "
		DEFINE INDEX i_t_unique_id ON TABLE i COLUMNS t UNIQUE;
		DEFINE INDEX t_name_idx ON TABLE t COLUMNS name;
		DEFINE FIELD name ON TABLE t TYPE string;
		DEFINE FIELD t ON TABLE i TYPE record<t>;
		CREATE t:1 SET name = 'h';
		CREATE t:2 SET name = 'h';
		CREATE i:A SET t = t:1;
		CREATE i:B SET t = t:2;
		SELECT * FROM i WHERE t.name = 'h' EXPLAIN;
		SELECT * FROM i WHERE t.name = 'h';
	";
	let mut res = dbs.execute(sql, &ses, None).await?;
	//
	assert_eq!(res.len(), 10);
	skip_ok(&mut res, 8)?;
	//
	let expected = syn::value(
		r#"[
				{ "id": i:A, "t": t:1 },
				{ "id": i:B, "t": t:2 }
			]"#,
	)
	.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
					{
						detail: {
							plan: {
								index: 'i_t_unique_id',
								joins: [
									{
										index: 't_name_idx',
										operator: '=',
										value: 'h'
									}
								],
								operator: 'join'
							},
							table: 'i'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", expected));
	//
	Ok(())
}
#[tokio::test]
async fn select_with_record_id_link_unique_remote_index() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	//
	let sql = "
		DEFINE INDEX i_t_id ON TABLE i COLUMNS t;
		DEFINE INDEX t_name_unique_idx ON TABLE t COLUMNS name UNIQUE;
		DEFINE FIELD name ON TABLE t TYPE string;
		DEFINE FIELD t ON TABLE i TYPE record<t>;
		CREATE t:1 SET name = 'a';
		CREATE t:2 SET name = 'b';
		CREATE i:A SET t = t:1;
		CREATE i:B SET t = t:2;
		SELECT * FROM i WHERE t.name IN ['a', 'b'] EXPLAIN;
		SELECT * FROM i WHERE t.name IN ['a', 'b'];
	";
	let mut res = dbs.execute(sql, &ses, None).await?;
	//
	assert_eq!(res.len(), 10);
	skip_ok(&mut res, 8)?;
	//
	let expected = syn::value(
		r#"[
				{ "id": i:A, "t": t:1 },
				{ "id": i:B, "t": t:2 }
			]"#,
	)
	.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
					{
						detail: {
							plan: {
								index: 'i_t_id',
								joins: [
									{
										index: 't_name_unique_idx',
										operator: 'union',
										value: [
											'a',
											'b'
										]
									}
								],
								operator: 'join'
							},
							table: 'i'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", expected));
	//
	Ok(())
}

#[tokio::test]
async fn select_with_record_id_link_full_text_index() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	//
	let sql = "
		DEFINE ANALYZER name TOKENIZERS class FILTERS lowercase,ngram(1,128);
		DEFINE INDEX t_name_search_idx ON TABLE t COLUMNS name SEARCH ANALYZER name BM25 HIGHLIGHTS;
		DEFINE INDEX i_t_id ON TABLE i COLUMNS t;
		DEFINE FIELD name ON TABLE t TYPE string;
		DEFINE FIELD t ON TABLE i TYPE record<t>;
		CREATE t:1 SET name = 'Hello World';
		CREATE i:A SET t = t:1;
		SELECT * FROM i WHERE t.name @@ 'world' EXPLAIN;
		SELECT * FROM i WHERE t.name @@ 'world';
	";
	let mut res = dbs.execute(sql, &ses, None).await?;

	assert_eq!(res.len(), 9);
	skip_ok(&mut res, 7)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
				{
					detail: {
						plan: {
							index: 'i_t_id',
							joins: [
								{
									index: 't_name_search_idx',
									operator: '@@',
									value: 'world'
								}
							],
							operator: 'join'
						},
						table: 'i'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(r#"[{ "id": i:A, "t": t:1}]"#).unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	Ok(())
}

#[tokio::test]
async fn select_with_record_id_link_full_text_no_record_index() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	//
	let sql = "
		DEFINE ANALYZER name TOKENIZERS class FILTERS lowercase,ngram(1,128);
		DEFINE INDEX t_name_search_idx ON TABLE t COLUMNS name SEARCH ANALYZER name BM25 HIGHLIGHTS;
		DEFINE FIELD name ON TABLE t TYPE string;
		DEFINE FIELD t ON TABLE i TYPE record<t>;
		CREATE t:1 SET name = 'Hello World';
		CREATE i:A SET t = t:1;
		SELECT * FROM i WHERE t.name @@ 'world' EXPLAIN;
		SELECT * FROM i WHERE t.name @@ 'world';
	";
	let mut res = dbs.execute(sql, &ses, None).await?;

	assert_eq!(res.len(), 8);
	skip_ok(&mut res, 6)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
					{
						detail: {
							direction: 'forward',
							table: 'i'
						},
						operation: 'Iterate Table'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					}
			]"#,
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(r#"[{ "id": i:A, "t": t:1}]"#).unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	Ok(())
}

#[tokio::test]
async fn select_with_record_id_index() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	//
	let sql = "
		CREATE t:1 SET links = [a:2, a:1];
		CREATE t:2 SET links = [a:3, a:4];
		SELECT * FROM t WHERE links CONTAINS a:2;
		SELECT * FROM t WHERE links CONTAINS a:2 EXPLAIN;
		SELECT * FROM t WHERE links CONTAINSANY [a:2];
		SELECT * FROM t WHERE links CONTAINSANY [a:2] EXPLAIN;
		SELECT * FROM t WHERE a:2 IN links;
		SELECT * FROM t WHERE a:2 IN links EXPLAIN;
		DEFINE INDEX idx ON t FIELDS links;
		SELECT * FROM t WHERE links CONTAINS a:2;
		SELECT * FROM t WHERE links CONTAINS a:2 EXPLAIN;
		SELECT * FROM t WHERE links CONTAINSANY [a:2];
		SELECT * FROM t WHERE links CONTAINSANY [a:2] EXPLAIN;
		SELECT * FROM t WHERE a:2 IN links;
		SELECT * FROM t WHERE a:2 IN links EXPLAIN;
	";
	let mut res = dbs.execute(sql, &ses, None).await?;

	let expected = syn::value(
		r#"[
			{
				id: t:1,
				links: [ a:2, a:1 ]
			}
		]"#,
	)
	.unwrap();
	//
	assert_eq!(res.len(), 15);
	skip_ok(&mut res, 2)?;
	//
	for t in ["CONTAINS", "CONTAINSANY", "IN"] {
		let tmp = res.remove(0).result?;
		assert_eq!(format!("{:#}", tmp), format!("{:#}", expected), "{t}");
		//
		let tmp = res.remove(0).result?;
		let val = syn::value(
			r#"[
				{
					detail: {
						direction: 'forward',
						table: 't'
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]"#,
		)
		.unwrap();
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	}
	//
	skip_ok(&mut res, 1)?;
	// CONTAINS
	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", expected));
	// CONTAINS EXPLAIN
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
				{
					detail: {
						plan: {
							index: 'idx',
							operator: '=',
							value: a:2
						},
						table: 't'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	// CONTAINSANY
	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", expected));
	// CONTAINSANY EXPLAIN
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
				{
					detail: {
						plan: {
							index: 'idx',
							operator: 'union',
							value: [
								a:2
							]
						},
						table: 't'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	// IN
	let tmp = res.remove(0).result?;
	assert_eq!(format!("{:#}", tmp), format!("{:#}", expected));
	// IN EXPLAIN
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
				{
					detail: {
						plan: {
							index: 'idx',
							operator: '=',
							value: a:2
						},
						table: 't'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_with_exact_operator() -> Result<()> {
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	//
	let sql = "
		DEFINE INDEX idx ON TABLE t COLUMNS b;
		DEFINE INDEX uniq ON TABLE t COLUMNS i;
		CREATE t:1 set b = true, i = 1;
		CREATE t:2 set b = false, i = 2;
		SELECT * FROM t WHERE b == true;
		SELECT * FROM t WHERE b == true EXPLAIN;
		SELECT * FROM t WHERE i == 2;
		SELECT * FROM t WHERE i == 2 EXPLAIN;
	";
	let mut res = dbs.execute(sql, &ses, None).await?;
	//
	assert_eq!(res.len(), 8);
	skip_ok(&mut res, 4)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				b: true,
				i: 1,
				id: t:1
			}
		]"#,
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
				{
					detail: {
						plan: {
							index: 'idx',
							operator: '=',
							value: true
						},
						table: 't'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				b: false,
				i: 2,
				id: t:2
			}
		]"#,
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
				{
					detail: {
						plan: {
							index: 'uniq',
							operator: '=',
							value: 2
						},
						table: 't'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	Ok(())
}

async fn select_composite_index(unique: bool) -> Result<()> {
	//
	let sql = format!(
		"
		DEFINE INDEX t_idx ON TABLE t COLUMNS on, value {};
		CREATE t:1 SET on = true, value = 1;
		CREATE t:2 SET on = false, value = 1;
		CREATE t:3 SET on = true, value = 2;
		CREATE t:4 SET on = false, value = 2;
		SELECT * FROM t WHERE on = true EXPLAIN;
		SELECT * FROM t WHERE on = true;
		SELECT * FROM t WHERE on = false AND value = 2 EXPLAIN;
		SELECT * FROM t WHERE on = false AND value = 2;
	",
		if unique {
			"UNIQUE"
		} else {
			""
		}
	);
	let mut t = Test::new(&sql).await?;
	//
	t.expect_size(9)?;
	t.skip_ok(5)?;
	//
	t.expect_vals(&[
		"[
			{
				detail: {
					plan: {
							index: 't_idx',
							operator: '=',
							value: true
					},
					table: 't'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
			}
		]",
		"[
			{
				id: t:1,
				on: true,
				value: 1
			},
			{
				id: t:3,
				on: true,
				value: 2
			}
		]",
		"[
			{
				detail: {
					plan: {
						index: 't_idx',
						operator: '=',
						value: [false, 2]
					},
					table: 't'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					type: 'Memory'
				},
				operation: 'Collector'
			}
		]",
		"[
			{
				id: t:4,
				on: false,
				value: 2
			}
		]",
	])?;
	//
	Ok(())
}

#[tokio::test]
async fn select_composite_standard_index() -> Result<()> {
	select_composite_index(false).await
}

#[tokio::test]
async fn select_composite_unique_index() -> Result<()> {
	select_composite_index(true).await
}

#[tokio::test]
async fn select_where_index_boolean_behaviour() -> Result<()> {
	let sql = r"
		DEFINE INDEX flagIndex ON TABLE test COLUMNS flag;
		CREATE test:t CONTENT { flag:true };
		CREATE test:f CONTENT { flag:false };
		SELECT * FROM test;
		SELECT * FROM test WITH NOINDEX WHERE (true OR flag=true);
		SELECT * FROM test WITH NOINDEX WHERE (true OR flag==true);
		SELECT * FROM test WHERE (true OR flag=true);
		SELECT * FROM test WHERE (true OR flag==true);";
	let mut t = Test::new(sql).await?;
	t.expect_size(8)?;
	t.skip_ok(3)?;
	for i in 0..5 {
		t.expect_val_info(
			"[
					{
						flag: false,
						id: test:f
					},
					{
						flag: true,
						id: test:t
					}
				]",
			i,
		)?;
	}
	Ok(())
}

#[tokio::test]
async fn select_memory_ordered_collector() -> Result<()> {
	let sql = r"
		CREATE |i:1500| SET v = rand::guid() RETURN NONE;
		SELECT v FROM i ORDER BY RAND() EXPLAIN;
		SELECT v FROM i ORDER BY v EXPLAIN;
		SELECT v FROM i ORDER BY RAND() PARALLEL EXPLAIN;
		SELECT v FROM i ORDER BY v PARALLEL EXPLAIN;
		SELECT v FROM i ORDER BY v;
		SELECT v FROM i ORDER BY v PARALLEL;
		SELECT v FROM i ORDER BY RAND();
		SELECT v FROM i ORDER BY RAND() PARALLEL;";
	let mut t = Test::new(sql).await?;
	t.expect_size(9)?;
	t.skip_ok(1)?;
	// Check explain plans
	for _ in 0..2 {
		t.expect_val(
			"[
				{
					detail: {
						direction: 'forward',
						table: 'i'
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						type: 'MemoryRandom'
					},
					operation: 'Collector'
				}
			]",
		)?;
		t.expect_val(
			"[
				{
					detail: {
						direction: 'forward',
						table: 'i'
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						type: 'MemoryOrdered'
					},
					operation: 'Collector'
				}
			]",
		)?;
	}

	// Extract the array from a value
	let mut get_array = || -> Result<_> {
		let v = t.next_value()?;
		if let Value::Array(a) = &v {
			assert_eq!(a.len(), 1500);
			Ok(a.to_vec())
		} else {
			panic!("Expected a Value::Array but get: {v}");
		}
	};

	// Check that the values are sorted `ORDER BY v`
	for _ in 0..2 {
		let a = get_array()?;
		assert!(a.windows(2).all(|w| w[0] <= w[1]), "Values are not sorted: {a:?}");
	}

	// Check that the values are not sorted `ORDER BY RAND()`
	for _ in 0..2 {
		let a = get_array()?;
		assert!(a.windows(2).any(|w| w[0] <= w[1]), "Values are not random: {a:?}");
	}
	// With an array of 1500, there is a probability of factorial 1500! that `ORDER
	// BY RAND()` returns a sorted array At a rate of one test per minute, we're
	// SURE that approximately 10^4,104.8 years from now a test WILL fail.
	// For perspective, this time frame is far longer than the age of the universe,
	// but well, my apologies if that even happen ¯\_(ツ)_/¯
	Ok(())
}

#[tokio::test]
async fn select_limit_start() -> Result<()> {
	let sql = r"
		CREATE |item:1000|;
		SELECT * FROM item LIMIT 10 START 2 PARALLEL EXPLAIN FULL;
		SELECT * FROM item LIMIT 10 START 2 PARALLEL;
		SELECT * FROM item LIMIT 10 START 2 EXPLAIN FULL;
		SELECT * FROM item LIMIT 10 START 2;";
	let mut t = Test::new(sql).await?;
	t.expect_size(5)?;
	t.skip_ok(1)?;
	for _ in 0..2 {
		t.expect_val(
			"[
					{
						detail: {
							direction: 'forward',
							table: 'item'
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
							type: 'KeysAndValues'
						},
						operation: 'RecordStrategy'
					},
					{
						detail: {
							CancelOnLimit: 10,
							SkipStart: 2
						},
						operation: 'StartLimitStrategy'
					},
					{
						detail: {
							count: 10
						},
						operation: 'Fetch'
					}
				]",
		)?;
		let r = t.next()?.result?;
		if let Value::Array(a) = r {
			assert_eq!(a.len(), 10);
		} else {
			panic!("Unexpected value: {r:#}");
		}
	}
	Ok(())
}

#[tokio::test]
async fn select_limit_start_order() -> Result<()> {
	let sql = r"
		CREATE |item:1000| RETURN NONE;
		SELECT * FROM item ORDER BY id LIMIT 10 START 2 PARALLEL EXPLAIN;
		SELECT * FROM item ORDER BY id LIMIT 10 START 2 PARALLEL;
		SELECT * FROM item ORDER BY id LIMIT 10 START 2 EXPLAIN;
		SELECT * FROM item ORDER BY id LIMIT 10 START 2;";
	let mut t = Test::new(sql).await?;
	t.expect_size(5)?;
	t.skip_ok(1)?;
	for _ in 0..2 {
		t.expect_val(
			"[
					{
						detail: {
							direction: 'forward',
							table: 'item'
						},
						operation: 'Iterate Table'
					},
					{
						detail: {
							limit: 12,
							type: 'MemoryOrderedLimit'
						},
						operation: 'Collector'
					}
				]",
		)?;
		let r = t.next()?.result?;
		if let Value::Array(a) = r {
			assert_eq!(a.len(), 10);
		} else {
			panic!("Unexpected value: {r:#}");
		}
	}
	Ok(())
}

#[tokio::test]
async fn select_count_group_all_with_or_without_index() -> Result<()> {
	let sql = r"
		FOR $i IN 0..10000 {
			 CREATE indexPerformance3 CONTENT {
				something: $i
			};
		};
		DEFINE INDEX somethingIndex ON TABLE indexPerformance3 COLUMNS something;
		SELECT count() FROM indexPerformance3 WITH NOINDEX WHERE something >= 5000 GROUP ALL EXPLAIN;
		SELECT count() FROM indexPerformance3 WHERE something >= 5000 GROUP ALL EXPLAIN;
		SELECT count() FROM indexPerformance3 WITH NOINDEX WHERE something >= 5000 GROUP ALL;
		SELECT count() FROM indexPerformance3 WHERE something >= 5000 GROUP ALL;";
	let mut t = Test::new(sql).await?;
	t.expect_size(6)?;
	t.skip_ok(2)?;
	t.expect_val(
		"[
			{
				detail: {
					direction: 'forward',
					table: 'indexPerformance3'
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
					idioms: {
						count: [
							'count'
						]
					},
					type: 'Group'
				},
				operation: 'Collector'
			}
		]",
	)?;
	t.expect_val(
		"[
			{
				detail: {
					plan: {
						direction: 'forward',
						from: {
							inclusive: true,
							value: 5000
						},
						index: 'somethingIndex',
						to: {
							inclusive: false,
							value: NONE
						}
					},
					table: 'indexPerformance3'
				},
				operation: 'Iterate Index Count'
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
		]",
	)?;
	for i in 0..2 {
		t.expect_val_info(
			"[
					{
						count: 5000
					}
				]",
			i,
		)?;
	}
	Ok(())
}

#[tokio::test]
async fn select_limit_start_array() -> Result<()> {
	Test::new("SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 0;")
		.await?
		.expect_val("[1, 2, 3, 4, 5]")?;

	Test::new("SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 4;")
		.await?
		.expect_val("[5, 6, 7, 8, 9]")?;

	Test::new("SELECT * FROM [1,2,3,4,5,6,7,8,9,10] LIMIT 5 START 7;")
		.await?
		.expect_val("[8, 9, 10]")?;

	Ok(())
}
