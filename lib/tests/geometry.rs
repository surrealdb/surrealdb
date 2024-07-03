mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn geometry_point() -> Result<(), Error> {
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
	let val = Value::parse(
		r#"[
			{
				"centre": {
					"type": "Point",
					"coordinates": [-0.118092, 51.509865]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"centre": {
					"type": "Point",
					"coordinates": [-0.118092, 51.509865]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_polygon() -> Result<(), Error> {
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
	let val = Value::parse(
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
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_multipoint() -> Result<(), Error> {
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
	let val = Value::parse(
		r#"[
			{
				"points": {
					"type": "MultiPoint",
					"coordinates": [
						[-0.118092, 51.509865],
						[-0.118092, 51.509865]
					]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"points": {
					"type": "MultiPoint",
					"coordinates": [
						[-0.118092, 51.509865],
						[-0.118092, 51.509865]
					]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"points": {
					"type": "MultiPoint",
					"coordinates": [
						[-0.118092, 51.509865],
						[-0.118092, 51.509865]
					]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_multipolygon() -> Result<(), Error> {
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
	let val = Value::parse(
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
				"id": r"university:oxford"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
				"id": r"university:oxford"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
				"id": r"university:oxford"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_inner_access() -> Result<(), Error> {
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
	let val = Value::parse(
		r#"[
			{
				lat: 51.509865,
				lng: -0.118092,
				type: 'Point'
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				lat: 51.509865,
				lng: -0.118092,
				type: 'Point'
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_point_with_params() -> Result<(), Error> {
	let sql = "
		LET $x = -0.118092;
		LET $y = 51.509865;

		UPDATE city:london SET centre = {
			type: 'Point',
			coordinates: [$x, $y]
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	res.remove(0);
	res.remove(0);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"centre": {
					"type": "Point",
					"coordinates": [-0.118092, 51.509865]
				},
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_polygon_with_params() -> Result<(), Error> {
	let sql = "
		LET $coords = [
			[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
			[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
			[-0.38314819, 51.37692386]
		];

		UPDATE city:london SET area = {
			type: 'Polygon',
			coordinates: [$coords]
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	res.remove(0);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
				"id": r"city:london"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_multipolygon_with_params() -> Result<(), Error> {
	let sql = "
		LET $line1 = [ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ];
		LET $polygon2 = [[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]];

		UPDATE university:oxford SET area = {
			type: 'MultiPolygon',
			coordinates: [
				[$line1],
				$polygon2
			]
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	res.remove(0);
	res.remove(0);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
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
				"id": r"university:oxford"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn geometry_collection_with_params() -> Result<(), Error> {
	let sql = r#"
		LET $geometry1 = {
			type: "MultiPoint",
			coordinates: [
				[10.0, 11.2],
				[10.5, 11.9]
			],
		};

		LET $geometry2 = {
			type: "Polygon",
			coordinates: [[
				[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
				[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
				[-0.38314819, 51.37692386]
			]]
		};

		LET $geometry3 = {
			type: "MultiPolygon",
			coordinates: [
				[
					[ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ]
				],
				[
					[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]
				]
			]
		};

		UPDATE university:oxford SET buildings = {
			type: "GeometryCollection",
			geometries: [
				$geometry1,
				$geometry2,
				$geometry3
			]
		};
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	res.remove(0);
	res.remove(0);
	res.remove(0);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		r#"[
			{
				"buildings": {
					type: "GeometryCollection",
					geometries: [
						{
							type: "MultiPoint",
							coordinates: [
								[10.0, 11.2],
								[10.5, 11.9]
							],
						},
						{
							type: "Polygon",
							coordinates: [[
								[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
								[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
								[-0.38314819, 51.37692386]
							]]
						},
						{
							type: "MultiPolygon",
							coordinates: [
								[
									[ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ]
								],
								[
									[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]
								]
							]
						}
					]
				},
				"id": r"university:oxford"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
