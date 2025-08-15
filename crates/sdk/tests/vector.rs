mod helpers;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;

use crate::helpers::{Test, new_ds, skip_ok};

#[tokio::test]
async fn select_where_mtree_knn() -> Result<()> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:3;
		DEFINE INDEX mt_pts ON pts FIELDS point MTREE DIMENSION 4 TYPE F32;
		UPDATE pts:3 SET point = [8,9,10,11];
		LET $pt = [2,3,4,5];
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2|> $pt;
		SELECT id FROM pts WHERE point <|2|> $pt EXPLAIN;
		UPDATE pts:3 set point = NONE;
	";
	let mut t = Test::new(sql).await?;
	t.expect_size(9)?;
	//
	t.skip_ok(6)?;
	//
	t.expect_val(
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
	)?;
	//
	t.expect_val(
		"[
					{
						detail: {
							plan: {
								index: 'mt_pts',
								operator: '<|2|>',
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
	)?;
	//
	t.skip_ok(1)?;
	Ok(())
}

#[tokio::test]
async fn delete_update_mtree_index() -> Result<()> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:3 SET point = [2,3,4,5];
		DEFINE INDEX mt_pts ON pts FIELDS point MTREE DIMENSION 4 TYPE I32;
		CREATE pts:4 SET point = [8,9,10,11];
		DELETE pts:2;
		UPDATE pts:3 SET point = [12,13,14,15];
		LET $pt = [2,3,4,5];
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|5|> $pt ORDER BY dist;
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
	let val = syn::value(
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn index_embedding() -> Result<()> {
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
	let val = syn::value(
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	Ok(())
}

#[tokio::test]
async fn select_where_brute_force_knn() -> Result<()> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:3 SET point = [8,9,10,11];
		CREATE pts:4;
		LET $pt = [2,3,4,5];
		SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> $pt EXPLAIN;
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt ORDER BY dist;
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt ORDER BY dist PARALLEL;
	";
	let mut t = Test::new(sql).await?;
	//
	t.expect_size(8)?;
	//
	t.skip_ok(5)?;
	//
	t.expect_val(
		"[
				{
					detail: {
                        direction: 'forward',
						table: 'pts',
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				},
			]",
	)?;
	//
	for i in 0..2 {
		t.expect_val_info(
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
			i,
		)?;
	}
	Ok(())
}

#[tokio::test]
async fn select_where_hnsw_knn() -> Result<()> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:3;
		DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
		UPDATE pts:3 SET point = [8,9,10,11];
		LET $pt = [2,3,4,5];
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt;
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> $pt EXPLAIN;
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt;
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,EUCLIDEAN|> $pt EXPLAIN;
		DELETE pts:3;
	";
	let mut t = Test::new(sql).await?;
	t.expect_size(11)?;
	t.skip_ok(6)?;
	// KNN result with HNSW index
	t.expect_val(
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
	)?;
	// Explains KNN with HNSW index
	t.expect_val(
		"[
					{
						detail: {
							plan: {
								index: 'hnsw_pts',
								operator: '<|2,100|>',
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
	)?;
	// KNN result with brute force
	t.expect_val(
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
	)?;
	// Explain KNN with brute force
	t.expect_val(
		"[
				{
					detail: {
                        direction: 'forward',
						table: 'pts'
					},
					operation: 'Iterate Table'
				},
				{
					detail: {
						type: 'Memory'
					},
					operation: 'Collector'
				}
			]",
	)?;
	t.skip_ok(1)?;
	Ok(())
}

#[tokio::test]
async fn select_mtree_knn_with_condition() -> Result<()> {
	let sql = r"
		DEFINE INDEX mt_pt1 ON pts FIELDS point MTREE DIMENSION 1;
		INSERT INTO pts [
			{ id: pts:1, point: [ 10f ], flag: true },
			{ id: pts:2, point: [ 20f ], flag: false },
			{ id: pts:3, point: [ 30f ], flag: true },
			{ id: pts:4, point: [ 40f ], flag: false },
			{ id: pts:5, point: [ 50f ], flag: true },
			{ id: pts:6, point: [ 60f ], flag: false },
			{ id: pts:7, point: [ 70f ], flag: true }
		];
		LET $pt = [44f];
		SELECT id, flag, vector::distance::knn() AS distance FROM pts
			WHERE flag = true && point <|2|> $pt
			ORDER BY distance EXPLAIN;
		SELECT id, flag, vector::distance::knn() AS distance FROM pts
			WHERE flag = true && point <|2|> $pt
			ORDER BY distance;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	skip_ok(res, 3)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
					{
						detail: {
							plan: {
								index: 'mt_pt1',
								operator: '<|2|>',
								value: [44f]
							},
							table: 'pts',
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
			]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					id: pts:5,
					flag: true,
					distance: 6f
				},
				{
					id: pts:3,
					flag: true,
					distance: 14f
				}
			]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	Ok(())
}

#[test_log::test(tokio::test)]
async fn select_hnsw_knn_with_condition() -> Result<()> {
	let sql = r"
		DEFINE INDEX hn_pt1 ON pts FIELDS point HNSW DIMENSION 1;
		INSERT INTO pts [
			{ id: pts:1, point: [ 10f ], flag: true },
			{ id: pts:2, point: [ 20f ], flag: false },
			{ id: pts:3, point: [ 30f ], flag: true },
			{ id: pts:4, point: [ 40f ], flag: false },
			{ id: pts:5, point: [ 50f ], flag: true },
			{ id: pts:6, point: [ 60f ], flag: false },
			{ id: pts:7, point: [ 70f ], flag: true }
		];
		LET $pt = [44f];
		SELECT id, flag, vector::distance::knn() AS distance FROM pts
			WHERE flag = true AND point <|2,40|> $pt
			ORDER BY distance EXPLAIN;
		SELECT id, flag, vector::distance::knn() AS distance FROM pts
			WHERE flag = true AND point <|2,40|> $pt
			ORDER BY distance;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	skip_ok(res, 3)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
					{
						detail: {
							plan: {
								index: 'hn_pt1',
								operator: '<|2,40|>',
								value: [44f]
							},
							table: 'pts',
						},
						operation: 'Iterate Index'
					},
					{
						detail: {
							type: 'MemoryOrdered'
						},
						operation: 'Collector'
					}
			]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					distance: 6f,
					flag: true,
					id: pts:5
				},
				{
					distance: 14f,
					flag: true,
					id: pts:3
				}
			]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	Ok(())
}

#[test_log::test(tokio::test)]
async fn select_bruteforce_knn_with_condition() -> Result<()> {
	let sql = r"
		INSERT INTO pts [
			{ id: pts:1, point: [ 10f ], flag: true },
			{ id: pts:2, point: [ 20f ], flag: false },
			{ id: pts:3, point: [ 30f ], flag: true },
			{ id: pts:4, point: [ 40f ], flag: false },
			{ id: pts:5, point: [ 50f ], flag: true },
			{ id: pts:6, point: [ 60f ], flag: false },
			{ id: pts:7, point: [ 70f ], flag: true }
		];
		LET $pt = [44f];
		SELECT id, flag, vector::distance::knn() AS distance FROM pts
			WHERE flag = true AND point <|2,EUCLIDEAN|> $pt
			ORDER BY distance EXPLAIN;
		SELECT id, flag, vector::distance::knn() AS distance FROM pts
			WHERE flag = true AND point <|2,EUCLIDEAN|> $pt
			ORDER BY distance;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	skip_ok(res, 2)?;
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					detail: {
                        direction: 'forward',
						table: 'pts'
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
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
				{
					distance: 6f,
					flag: true,
					id: pts:5
				},
				{
					distance: 14f,
					flag: true,
					id: pts:3
				}
			]",
	)
	.unwrap();
	assert_eq!(format!("{:#}", tmp), format!("{:#}", val));
	//
	Ok(())
}

#[tokio::test]
async fn check_hnsw_persistence() -> Result<()> {
	let sql = r"
		CREATE pts:1 SET point = [1,2,3,4];
		CREATE pts:2 SET point = [4,5,6,7];
		CREATE pts:4 SET point = [12,13,14,15];
		DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
		CREATE pts:3 SET point = [8,9,10,11];
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> [2,3,4,5];
		DELETE pts:4;
		SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> [2,3,4,5];
	";

	// Ingest the data in the datastore.
	let mut t = Test::new(sql).await?;
	t.skip_ok(5)?;
	t.expect_val(
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
	)?;
	t.skip_ok(1)?;
	t.expect_val(
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
	)?;

	// Restart the datastore and execute the SELECT query
	let sql =
		"SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> [2,3,4,5];";
	let mut t = t.restart(sql).await?;

	// We should find results
	t.expect_val(
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
	)?;
	Ok(())
}
