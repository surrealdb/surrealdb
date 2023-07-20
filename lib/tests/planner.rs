mod parse;

use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

async fn test_select_where_iterate_multi_index(parallel: bool) -> Result<(), Error> {
	let parallel = if parallel {
		"PARALLEL"
	} else {
		""
	};
	let sql = format!(
		"
		CREATE person:tobie SET name = 'Tobie', genre='m', company='SurrealDB';
		CREATE person:jaime SET name = 'Jaime', genre='m', company='SurrealDB';
		CREATE person:lizzie SET name = 'Lizzie', genre='f', company='SurrealDB';
		CREATE person:neytiry SET name = 'Neytiri', genre='f', company='Metkayina';
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX ft_company ON person FIELDS company SEARCH ANALYZER simple BM25;
		DEFINE INDEX uniq_name ON TABLE person COLUMNS name UNIQUE;
		DEFINE INDEX idx_genre ON TABLE person COLUMNS genre;
		SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' {parallel};
	    SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' OR company @@ 'surrealdb' {parallel} EXPLAIN FULL;"
	);
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	//
	for _ in 0..8 {
		let _ = res.remove(0).result?;
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: 'Jaime'
			},
            {
				name: 'Tobie'
			},
			{
				name: 'Lizzie'
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
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
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_multi_index() -> Result<(), Error> {
	test_select_where_iterate_multi_index(false).await
}

#[tokio::test]
async fn select_where_iterate_multi_index_parallel() -> Result<(), Error> {
	test_select_where_iterate_multi_index(true).await
}
