mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;

#[tokio::test]
async fn geometry_point() -> Result<()> {
	let sql = "
		UPSERT city:london SET centre = (-0.118092, 51.509865);
		SELECT * FROM city:london;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"centre": {
					"type": "Point",
					"coordinates": [-0.118092, 51.509865]
				},
				"id": city:london
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"centre": {
					"type": "Point",
					"coordinates": [-0.118092, 51.509865]
				},
				"id": city:london
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_polygon() -> Result<()> {
	let sql = "
		UPSERT city:london SET area = {
			type: 'Polygon',
			coordinates: [[
				[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
				[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
				[-0.38314819, 51.37692386]
			]]
		};
		UPSERT city:london SET area = {
			type: 'Polygon',
			coordinates: [[
				[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
				[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
				[-0.38314819, 51.37692386],
			]],
		};
		SELECT * FROM city:london;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"area": {
					"type": "Polygon",
					"coordinates": [
						[
							[-0.38314819, 51.37692386],
							[0.1785278, 51.37692386],
							[0.1785278, 51.6146057],
							[-0.38314819, 51.6146057],
							[-0.38314819, 51.37692386]
						]
					]
				},
				"id": city:london
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"area": {
					"type": "Polygon",
					"coordinates": [
						[
							[-0.38314819, 51.37692386],
							[0.1785278, 51.37692386],
							[0.1785278, 51.6146057],
							[-0.38314819, 51.6146057],
							[-0.38314819, 51.37692386]
						]
					]
				},
				"id": city:london
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"area": {
					"type": "Polygon",
					"coordinates": [
						[
							[-0.38314819, 51.37692386],
							[0.1785278, 51.37692386],
							[0.1785278, 51.6146057],
							[-0.38314819, 51.6146057],
							[-0.38314819, 51.37692386]
						]
					]
				},
				"id": city:london
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_multipoint() -> Result<()> {
	let sql = "
		UPSERT city:london SET points = {
			type: 'MultiPoint',
			coordinates: [
				[-0.118092, 51.509865],
				[-0.118092, 51.509865]
			]
		};
		UPSERT city:london SET points = {
			type: 'MultiPoint',
			coordinates: [
				[-0.118092, 51.509865],
				[-0.118092, 51.509865],
			],
		};
		SELECT * FROM city:london;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"points": {
					"type": "MultiPoint",
					"coordinates": [
						[-0.118092, 51.509865],
						[-0.118092, 51.509865]
					]
				},
				"id": city:london
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"points": {
					"type": "MultiPoint",
					"coordinates": [
						[-0.118092, 51.509865],
						[-0.118092, 51.509865]
					]
				},
				"id": city:london
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"points": {
					"type": "MultiPoint",
					"coordinates": [
						[-0.118092, 51.509865],
						[-0.118092, 51.509865]
					]
				},
				"id": city:london
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_multipolygon() -> Result<()> {
	let sql = "
		UPSERT university:oxford SET area = {
			type: 'MultiPolygon',
			coordinates: [
				[[ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ]],
				[[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]]
			]
		};
		UPSERT university:oxford SET area = {
			type: 'MultiPolygon',
			coordinates: [
				[[ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ]],
				[[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]],
			],
		};
		SELECT * FROM university:oxford;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"area": {
					"type": "MultiPolygon",
					"coordinates": [
						[
							[[10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2]]
						],
						[
							[[9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2]]
						]
					]
				},
				"id": university:oxford
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"area": {
					"type": "MultiPolygon",
					"coordinates": [
						[
							[[10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2]]
						],
						[
							[[9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2]]
						]
					]
				},
				"id": university:oxford
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				"area": {
					"type": "MultiPolygon",
					"coordinates": [
						[
							[[10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2]]
						],
						[
							[[9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2]]
						]
					]
				},
				"id": university:oxford
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_inner_access() -> Result<()> {
	let sql = "
		SELECT type, coordinates[0] as lng, coordinates[1] AS lat FROM type::point([-0.118092, 51.509865]);
		SELECT type, coordinates[0] as lng, coordinates[1] AS lat FROM (-0.118092, 51.509865);
		SELECT coordinates FROM {
			type: 'Polygon',
			coordinates: [
				[
					[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
					[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
					[-0.38314819, 51.37692386],
				]
			],
		};
		SELECT coordinates FROM {
			type: 'Polygon',
			coordinates: [
				[
					[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
					[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
					[-0.38314819, 51.37692386],
				],
				[
					[-0.38314819, 51.37692386], [-0.38314819, 51.61460570],
					[-0.38314819, 51.37692386],
				],
				[
					[110.38314819, 110.37692386], [110.38314819, 110.61460570],
					[110.38314819, 110.37692386],
				]
			],
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				lat: 51.509865,
				lng: -0.118092,
				type: 'Point'
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				lat: 51.509865,
				lng: -0.118092,
				type: 'Point'
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				coordinates: [
					[
						[
							-0.38314819,
							51.37692386
						],
						[
							0.1785278,
							51.37692386
						],
						[
							0.1785278,
							51.6146057
						],
						[
							-0.38314819,
							51.6146057
						],
						[
							-0.38314819,
							51.37692386
						]
					]
				]
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[
			{
				coordinates: [
					[
						[
							-0.38314819,
							51.37692386
						],
						[
							0.1785278,
							51.37692386
						],
						[
							0.1785278,
							51.6146057
						],
						[
							-0.38314819,
							51.6146057
						],
						[
							-0.38314819,
							51.37692386
						]
					],
					[
						[
							-0.38314819,
							51.37692386
						],
						[
							-0.38314819,
							51.6146057
						],
						[
							-0.38314819,
							51.37692386
						]
					],
					[
						[
							110.38314819,
							110.37692386
						],
						[
							110.38314819,
							110.6146057
						],
						[
							110.38314819,
							110.37692386
						]
					]
				]
			}
		]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}
