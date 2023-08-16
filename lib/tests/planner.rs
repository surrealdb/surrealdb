mod parse;

use parse::Parse;
use surrealdb::dbs::{Response, Session};
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_where_iterate_three_multi_index() -> Result<(), Error> {
	let mut res = execute_test(&three_multi_index_query("", ""), 12).await?;
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
	let mut res = execute_test(&three_multi_index_query("", "PARALLEL"), 12).await?;
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
	let mut res =
		execute_test(&three_multi_index_query("WITH INDEX uniq_name,idx_genre,ft_company", ""), 12)
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
	let mut res = execute_test(&three_multi_index_query("WITH INDEX ft_company", ""), 12).await?;
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
	let mut res = execute_test(&three_multi_index_query("WITH INDEX uniq_name", ""), 12).await?;
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
	let mut res = execute_test(&two_multi_index_query("", ""), 9).await?;
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
	let mut res = execute_test(&two_multi_index_query("WITH INDEX idx_genre", ""), 9).await?;
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
		execute_test(&two_multi_index_query("WITH INDEX idx_genre,uniq_name", ""), 9).await?;
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
	let mut res = execute_test(&two_multi_index_query("WITH NOINDEX", ""), 9).await?;
	// OR results
	check_result(&mut res, "[{ name: 'Jaime' }, { name: 'Tobie' }]")?;
	check_result(&mut res, &table_explain(2))?;
	// AND results
	check_result(&mut res, "[{name: 'Jaime'}]")?;
	check_result(&mut res, &table_explain(1))?;
	Ok(())
}

async fn execute_test(sql: &str, expected_result: usize) -> Result<Vec<Response>, Error> {
	let dbs = Datastore::new("memory").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), expected_result);
	// Check that the setup is ok
	for _ in 0..(expected_result - 4) {
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
