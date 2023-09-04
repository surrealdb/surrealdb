mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_where_mtree_knn() -> Result<(), Error> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:3 SET point = [8,9,10,11];
		DEFINE INDEX mt_pts ON pts FIELDS point MTREE DIMENSION 4;
		LET $pt = [2,3,4,5];
		SELECT id, vector::distance::euclidean(point, $pt) AS dist FROM pts WHERE point <2> $pt;
		SELECT id FROM pts WHERE point <2> $pt EXPLAIN;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	for _ in 0..5 {
		let _ = res.remove(0).result?;
	}
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: pts:1,
				dist: 1.5f
			},
			{
				id: pts:2,
				dist: 1.8f
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
					{
						detail: {
							plan: {
								index: 'mt_pts',
								operator: '<2>',
								value: [2,3,4,5]
							},
							table: 'vec',
						},
						operation: 'Iterate Index'
					}
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}
