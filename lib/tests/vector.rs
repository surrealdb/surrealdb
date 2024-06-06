#![cfg(feature = "sql2")]

mod helpers;
mod parse;
use crate::helpers::{new_ds, skip_ok};
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_where_mtree_knn() -> Result<(), Error> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:3 SET point = [8,9,10,11];
		DEFINE INDEX mt_pts ON pts FIELDS point MTREE DIMENSION 4;
		LET $pt = [2,3,4,5];
		SELECT id, vector::distance::euclidean(point, $pt) AS dist FROM pts WHERE point <|2|> $pt;
		SELECT id FROM pts WHERE point <|2|> $pt EXPLAIN;
	";
	let dbs = new_ds().await?;
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
				dist: 2f
			},
			{
				id: pts:2,
				dist: 4f
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
							table: 'pts',
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'Memory'
						},
						operation: 'Collector'
					},
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn delete_update_mtree_index() -> Result<(), Error> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:3 SET point = [2,3,4,5];
		DEFINE INDEX mt_pts ON pts FIELDS point MTREE DIMENSION 4;
		CREATE pts:4 SET point = [8,9,10,11];
		DELETE pts:2;
		UPDATE pts:3 SET point = [12,13,14,15];
		LET $pt = [2,3,4,5];
		SELECT id, vector::distance::euclidean(point, $pt) AS dist FROM pts WHERE point <|5|> $pt ORDER BY dist;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
	//
	for _ in 0..8 {
		let _ = res.remove(0).result?;
	}
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				dist: 2f,
				id: pts:1
			},
			{
				dist: 12f,
				id: pts:4
			},
			{
				dist: 20f,
				id: pts:3
			}
		]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn index_embedding() -> Result<(), Error> {
	let sql = r#"
		DEFINE INDEX idx_mtree_embedding_manhattan ON Document FIELDS items.embedding MTREE DIMENSION 4 DIST MANHATTAN;
		DEFINE INDEX idx_mtree_embedding_cosine ON Document FIELDS items.embedding MTREE DIMENSION 4 DIST COSINE;
		CREATE ONLY Document:1 CONTENT {
  			"items": [
  				{
					"content": "apple",
					"embedding": [
						0.009953570552170277, -0.02680361643433571, -0.018817437812685966,
						-0.08697346597909927
					]
 				}
  			]
		};
		"#;

	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let _ = res.remove(0).result?;
	let _ = res.remove(0).result?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			id: Document:1,
			items: [
				{
					content: 'apple',
					embedding: [
						0.009953570552170277f,
						-0.02680361643433571f,
						-0.018817437812685966f,
						-0.08697346597909927f
					]
				}
			]
		}",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_brute_force_knn() -> Result<(), Error> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:3 SET point = [8,9,10,11];
		LET $pt = [2,3,4,5];
		SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> $pt EXPLAIN;
		SELECT id, vector::distance::euclidean(point, $pt) AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt;
		SELECT id, vector::distance::euclidean(point, $pt) AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt PARALLEL;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	skip_ok(res, 4)?;
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
				{
					detail: {
						table: 'pts',
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
				},
			]",
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	for i in 0..2 {
		let tmp = res.remove(0).result?;
		let val = Value::parse(
			"[
			{
				id: pts:1,
				dist: 2f
			},
			{
				id: pts:2,
				dist: 4f
			}
		]",
		);
		assert_eq!(format!("{:#}", tmp), format!("{:#}", val), "{i}");
	}
	Ok(())
}

#[tokio::test]
async fn select_where_hnsw_knn() -> Result<(), Error> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:3 SET point = [8,9,10,11];
		DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
		LET $pt = [2,3,4,5];
		SELECT id, vector::distance::euclidean(point, $pt) AS dist FROM pts WHERE point <|2,100|> $pt;
		SELECT id FROM pts WHERE point <|2,100|> $pt EXPLAIN;
	";
	let dbs = new_ds().await?;
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
				dist: 2f
			},
			{
				id: pts:2,
				dist: 4f
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
								index: 'hnsw_pts',
								operator: '<2,100>',
								value: [2,3,4,5]
							},
							table: 'pts',
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
	);
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}
