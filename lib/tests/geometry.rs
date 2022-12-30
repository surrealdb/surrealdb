mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn geometry_point() -> Result<(), Error> {
	let sql = "
		UPDATE city:london SET centre = (-0.118092, 51.509865);
		SELECT * FROM city:london;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
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
				"id": "city:london"
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
				"id": "city:london"
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
		UPDATE city:london SET area = {
			type: 'Polygon',
			coordinates: [[
				[-0.38314819, 51.37692386], [0.1785278, 51.37692386],
				[0.1785278, 51.61460570], [-0.38314819, 51.61460570],
				[-0.38314819, 51.37692386]
			]]
		};
		SELECT * FROM city:london;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
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
				"id": "city:london"
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
				"id": "city:london"
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
		UPDATE city:london SET points = {
			type: 'MultiPoint',
			coordinates: [
				[-0.118092, 51.509865],
				[-0.118092, 51.509865]
			]
		};
		SELECT * FROM city:london;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
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
				"id": "city:london"
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
				"id": "city:london"
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
		UPDATE university:oxford SET area = {
			type: 'MultiPolygon',
			coordinates: [
				[[ [10.0, 11.2], [10.5, 11.9], [10.8, 12.0], [10.0, 11.2] ]],
				[[ [9.0, 11.2], [10.5, 11.9], [10.3, 13.0], [9.0, 11.2] ]]
			]
		};
		SELECT * FROM university:oxford;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 2);
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
				"id": "university:oxford"
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
				"id": "university:oxford"
			}
		]"#,
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
