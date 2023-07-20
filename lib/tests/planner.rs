mod parse;

use parse::Parse;
use surrealdb::dbs::{Response, Session};
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

const TWO_TABLE_ITERATOR: &str = "[
	{
		detail: {
			table: 'person'
		},
		operation: 'Iterate Table'
	},
	{
		detail: {
			count: 2
		},
		operation: 'Fetch'
	}
]";

const THREE_TABLE_ITERATOR: &str = "[
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
	    SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' {parallel} EXPLAIN FULL;")
}

const THREE_MULTI_INDEX_RESULT: &str = "[
			{
				name: 'Jaime'
			},
            {
				name: 'Tobie'
			},
			{
				name: 'Lizzie'
			}
		]";

const THREE_MULTI_INDEX_ITERATORS: &str = "[
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

fn two_multi_index_query(with: &str, parallel: &str) -> String {
	format!(
		"CREATE person:tobie SET name = 'Tobie', genre='m', company='SurrealDB';
		CREATE person:jaime SET name = 'Jaime', genre='m', company='SurrealDB';
		CREATE person:lizzie SET name = 'Lizzie', genre='f', company='SurrealDB';
		DEFINE INDEX uniq_name ON TABLE person COLUMNS name UNIQUE;
		DEFINE INDEX idx_genre ON TABLE person COLUMNS genre;
		SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' {parallel};
	    SELECT name FROM person {with} WHERE name = 'Jaime' OR genre = 'm' {parallel} EXPLAIN FULL;"
	)
}

const TWO_MULTI_INDEX_RESULT: &str = "[
			{
				name: 'Jaime'
			},
            {
				name: 'Tobie'
			}
		]";

const TWO_MULTI_INDEX_ITERATORS: &str = "[
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

async fn execute_test(sql: &str, expected_result: usize) -> Result<Vec<Response>, Error> {
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let mut res = dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), expected_result);
	//
	for _ in 0..(expected_result - 2) {
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

#[tokio::test]
async fn select_where_iterate_two_multi_index() -> Result<(), Error> {
	let mut res = execute_test(&two_multi_index_query("", ""), 7).await?;
	check_result(&mut res, TWO_MULTI_INDEX_RESULT)?;
	check_result(&mut res, TWO_MULTI_INDEX_ITERATORS)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_two_multi_index_with_two_index() -> Result<(), Error> {
	let mut res =
		execute_test(&two_multi_index_query("WITH INDEX idx_genre,uniq_name", ""), 7).await?;
	check_result(&mut res, TWO_MULTI_INDEX_RESULT)?;
	check_result(&mut res, TWO_MULTI_INDEX_ITERATORS)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_two_no_index() -> Result<(), Error> {
	let mut res = execute_test(&two_multi_index_query("WITH NOINDEX", ""), 7).await?;
	check_result(&mut res, TWO_MULTI_INDEX_RESULT)?;
	check_result(&mut res, TWO_TABLE_ITERATOR)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_two_multi_index_with_one_index() -> Result<(), Error> {
	let mut res = execute_test(&two_multi_index_query("WITH INDEX idx_genre", ""), 7).await?;
	check_result(&mut res, TWO_MULTI_INDEX_RESULT)?;
	check_result(&mut res, TWO_TABLE_ITERATOR)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index() -> Result<(), Error> {
	let mut res = execute_test(&three_multi_index_query("", ""), 10).await?;
	check_result(&mut res, THREE_MULTI_INDEX_RESULT)?;
	check_result(&mut res, THREE_MULTI_INDEX_ITERATORS)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_with_one_index() -> Result<(), Error> {
	let mut res = execute_test(&three_multi_index_query("WITH INDEX ft_company", ""), 10).await?;
	check_result(
		&mut res,
		"[
			{
				name: 'Jaime'
			},
			{
				name: 'Lizzie'
			},
			{
				name: 'Tobie'
			}
		]",
	)?;
	check_result(&mut res, THREE_TABLE_ITERATOR)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_with_all_index() -> Result<(), Error> {
	let mut res =
		execute_test(&three_multi_index_query("WITH INDEX uniq_name,idx_genre,ft_company", ""), 10)
			.await?;
	check_result(&mut res, THREE_MULTI_INDEX_RESULT)?;
	check_result(&mut res, THREE_MULTI_INDEX_ITERATORS)?;
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_three_multi_index_parallel() -> Result<(), Error> {
	let mut res = execute_test(&three_multi_index_query("", "PARALLEL"), 10).await?;
	check_result(&mut res, THREE_MULTI_INDEX_RESULT)?;
	check_result(&mut res, THREE_MULTI_INDEX_ITERATORS)?;
	Ok(())
}
