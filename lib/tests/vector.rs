mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_where_ball_tree_knn() -> Result<(), Error> {
	let sql = r"
		CREATE vec:1 SET point = [1,2,3,4];
		CREATE vec:2 SET point = [4,5,6,7];
		CREATE vec:3 SET point = [8,9,10,11];
		DEFINE INDEX bt_vec ON point FIELDS point BALLTREE DIMENSION 4;
		LET $pt = RETURN [2,3,4,5];
		SELECT id FROM vec WHERE point <2> $pt EXPLAIN;
		SELECT id, vector::distance::euclidean(point, $pt) AS dist FROM vec WHERE point <2> $pt;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
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
						detail: {
							plan: {
								index: 'bt_vec',
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: vec:1,
				dist: 1.5f
			},
			{
				id: vec:2,
				dist: 1.8f
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}
