mod parse;

use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

async fn test_select_where_iterate_indexes(parallel: bool) -> Result<(), Error> {
	let parallel = if parallel {
		"PARALLEL"
	} else {
		""
	};
	let sql = format!(
		"
		CREATE person:tobie SET name = 'Tobie', genre='m';
		CREATE person:jaime SET name = 'Jaime', genre='m';
		CREATE person:lizzie SET name = 'Lizzie', genre='f';
		DEFINE INDEX ft_name ON TABLE person COLUMNS name UNIQUE;
		DEFINE INDEX idx_genre ON TABLE person COLUMNS genre;
		SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' {parallel};
	    SELECT name FROM person WHERE name = 'Jaime' OR genre = 'm' {parallel} EXPLAIN FULL;"
	);
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	for _ in 0..5 {
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
							index: 'ft_name',
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
				]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_iterate_indexes() -> Result<(), Error> {
	test_select_where_iterate_indexes(false).await
}

#[tokio::test]
async fn select_where_iterate_indexes_parallel() -> Result<(), Error> {
	test_select_where_iterate_indexes(true).await
}
