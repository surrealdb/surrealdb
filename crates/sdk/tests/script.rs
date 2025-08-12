#![cfg(feature = "scripting")]

use std::time::{Duration, Instant};

mod helpers;
use helpers::new_ds;
use rust_decimal::Decimal;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::syn;
use surrealdb_core::val::{Geometry, Number, Value};

#[tokio::test]
async fn script_function_error() -> Result<()> {
	let sql = "
		SELECT * FROM function() {
			throw 'error';
		};
		SELECT * FROM function() {
			throw new Error('error');
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert_eq!(
		tmp.unwrap_err().to_string(),
		"Problem with embedded script function. An exception occurred: error\n"
	);

	let tmp = res.remove(0).result;
	assert_eq!(
		tmp.unwrap_err().to_string(),
		"Problem with embedded script function. An exception occurred: error\n"
	);
	//
	Ok(())
}

#[tokio::test]
async fn script_function_simple() -> Result<()> {
	let sql = r#"
		CREATE person:test SET scores = function() {
			return [6.6, 8.4, 7.3].map(v => v * 10);
		}, bio = function() {
			return "Line 1\nLine 2";
		};
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(r#"[{ bio: "Line 1\nLine 2", id: person:test, scores: [66, 84, 73] }]"#)
		.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn script_function_context() -> Result<()> {
	let sql = "
		CREATE film:test SET
			ratings = [
				{ rating: 6.3 },
				{ rating: 8.7 },
			],
			display = function() {
				return this.ratings.filter(r => {
					return r.rating >= 7;
				}).map(r => {
					return { ...r, rating: Math.round(r.rating * 10) };
				});
			}
		;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: film:test,
				ratings: [
					{ rating: 6.3 },
					{ rating: 8.7 },
				],
				display: [
					{ rating: 87 },
				]
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn script_function_arguments() -> Result<()> {
	let sql = "
		LET $value = 'SurrealDB';
		LET $words = ['awesome', 'advanced', 'cool'];
		CREATE article:test SET summary = function($value, $words) {
			return `${arguments[0]} is ${arguments[1].join(', ')}`;
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: article:test,
				summary: 'SurrealDB is awesome, advanced, cool',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn script_function_types() -> Result<()> {
	let sql = "
		CREATE article:test SET
			created_at = function() {
				return new Date('1995-12-17T03:24:00Z');
			},
			next_signin = function() {
				return new Duration('1w2d6h');
			},
			manager = function() {
				return new Record('user', 'joanna');
			},
			organisation = function() {
				return new Record('organisation', {
					alias: 'acme',
					name: 'Acme Inc',
				});
			},
			identifier = function() {
				return new Uuid('03412258-988f-47cd-82db-549902cdaffe');
			}
		;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				id: article:test,
				created_at: d'1995-12-17T03:24:00Z',
				next_signin: 1w2d6h,
				manager: user:joanna,
				organisation: organisation:{ alias: 'acme', name: 'Acme Inc' },
				identifier: u'03412258-988f-47cd-82db-549902cdaffe',
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn script_function_module_os() -> Result<()> {
	let sql = "
		CREATE platform:test SET version = function() {
			const { platform } = await import('os');
			return platform();
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	Ok(())
}

#[tokio::test]
async fn script_query_from_script_select() -> Result<()> {
	let sql = r#"
		CREATE test SET name = "a", number = 0;
		CREATE test SET name = "b", number = 1;
		CREATE test SET name = "c", number = 2;
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");

	// direct query
	dbs.execute(sql, &ses, None).await?;
	let sql = r#"
		RETURN function(){
			return await surrealdb.query(`SELECT number FROM test WHERE name = $name`,{
				name: "b"
			})
		}
	"#;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				number: 1
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	// indirect query
	let sql = r#"
		RETURN function(){
			let query = new surrealdb.Query(`SELECT number FROM test WHERE name = $name`);
			query.bind("name","c")
			return await surrealdb.query(query);
		}
	"#;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = syn::value(
		"[
			{
				number: 2
			}
		]",
	)
	.unwrap();
	assert_eq!(tmp, val);

	Ok(())
}

#[tokio::test]
async fn script_query_from_script() -> Result<()> {
	let sql = r#"
		RETURN function() {
			return await surrealdb.query(`CREATE ONLY article:test SET name = "The daily news", issue_number = 3`)
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"{
				id: article:test,
				name: "The daily news",
				issue_number: 3.0
		}"#,
	)
	.unwrap();
	assert_eq!(tmp, val);

	let sql = r#"
		SELECT * FROM article
	"#;
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	let tmp = res.remove(0).result?;
	let val = syn::value(
		r#"[{
				id: article:test,
				name: "The daily news",
				issue_number: 3.0
		}]"#,
	)
	.unwrap();
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn script_value_function_params() -> Result<()> {
	let sql = r#"
		LET $test = CREATE ONLY article:test SET name = "The daily news", issue_number = 3;
		RETURN function() {
			return await surrealdb.value(`$test.name`)
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	let tmp = res.remove(1).result?;
	let val = syn::value(r#""The daily news""#).unwrap();
	assert_eq!(tmp, val);
	Ok(())
}

#[tokio::test]
async fn script_value_function_inline_values() -> Result<()> {
	let sql = r#"
		RETURN function() {
			if(await surrealdb.value(`3`) !== 3){
				throw new Error(1)
			}
			if(await surrealdb.value(`"some string"`) !== "some string"){
				throw new Error(2)
			}
			if(await surrealdb.value(`math::floor(13.746189)`) !== 13){
				throw new Error(3)
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	res.remove(0).result?;
	Ok(())
}

#[tokio::test]
async fn script_function_number_conversion_test() -> Result<()> {
	let sql = r#"
		RETURN function() {
			if(await surrealdb.value(`2147483647`) !== 2147483647){
				throw new Error(1)
			}
			if(await surrealdb.value(`9007199254740991`) !== 9007199254740991){
				throw new Error(2)
			}
			if(await surrealdb.value(`9007199254740992`) !== 9007199254740992n){
				throw new Error(3)
			}
			if(await surrealdb.value(`-9007199254740992`) !== -9007199254740992){
				throw new Error(4)
			}
			if(await surrealdb.value(`-9007199254740993`) !== -9007199254740993n){
				throw new Error(5)
			}
			return {
				a:  9007199254740991,
				b: -9007199254740992,
				c: 100000000000000000n,
				d: 9223372036854775808n
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let Value::Object(res) = res.remove(0).result? else {
		panic!("not an object")
	};
	assert_eq!(res.get("a").unwrap(), &Value::Number(Number::Float(9007199254740991f64)));
	assert_eq!(res.get("b").unwrap(), &Value::Number(Number::Float(-9007199254740992f64)));
	assert_eq!(res.get("c").unwrap(), &Value::Number(Number::Int(100000000000000000i64)));
	assert_eq!(
		res.get("d").unwrap(),
		&Value::Number(Number::Decimal(Decimal::from(9223372036854775808u128)))
	);

	Ok(())
}

#[tokio::test]
async fn script_bytes() -> Result<()> {
	let sql = r#"
		RETURN function() {
			return new Uint8Array([0,1,2,3,4,5,6,7])
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let Value::Bytes(b) = res.remove(0).result? else {
		panic!("not bytes");
	};

	for i in 0..8 {
		assert_eq!(b[i], i as u8)
	}

	Ok(())
}

#[tokio::test]
async fn script_geometry_point() -> Result<()> {
	let sql = r#"
		RETURN function() {
			return {
				type: "Point",
				coordinates: [1.0,2.0]
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let Value::Geometry(Geometry::Point(x)) = res.remove(0).result? else {
		panic!("not a geometry");
	};

	assert_eq!(x.x(), 1.0);
	assert_eq!(x.y(), 2.0);

	Ok(())
}

#[tokio::test]
async fn script_geometry_line() -> Result<()> {
	let sql = r#"
		RETURN function() {
			return {
				type: "LineString",
				coordinates: [
					[1.0,2.0],
					[3.0,4.0],
				]
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let Value::Geometry(Geometry::Line(x)) = res.remove(0).result? else {
		panic!("not a geometry");
	};

	assert_eq!(x.0[0].x, 1.0);
	assert_eq!(x.0[0].y, 2.0);
	assert_eq!(x.0[1].x, 3.0);
	assert_eq!(x.0[1].y, 4.0);

	Ok(())
}

#[tokio::test]
async fn script_geometry_polygon() -> Result<()> {
	let sql = r#"
		RETURN function() {
			return {
				type: "Polygon",
				coordinates: [
					[
						[1.0,2.0],
						[3.0,4.0],
					],
					[
						[5.0,6.0],
						[7.0,8.0],
					]
				]
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let Value::Geometry(Geometry::Polygon(x)) = res.remove(0).result? else {
		panic!("not a geometry");
	};

	assert_eq!(x.exterior().0[0].x, 1.0);
	assert_eq!(x.exterior().0[0].y, 2.0);
	assert_eq!(x.exterior().0[1].x, 3.0);
	assert_eq!(x.exterior().0[1].y, 4.0);

	assert_eq!(x.interiors()[0].0[0].x, 5.0);
	assert_eq!(x.interiors()[0].0[0].y, 6.0);
	assert_eq!(x.interiors()[0].0[1].x, 7.0);
	assert_eq!(x.interiors()[0].0[1].y, 8.0);

	Ok(())
}

#[tokio::test]
async fn script_geometry_multi_point() -> Result<()> {
	let sql = r#"
		RETURN function() {
			return {
				type: "MultiPoint",
				coordinates: [
					[1.0,2.0],
					[3.0,4.0],
				]
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let Value::Geometry(Geometry::MultiPoint(x)) = res.remove(0).result? else {
		panic!("not a geometry");
	};

	assert_eq!(x.0[0].x(), 1.0);
	assert_eq!(x.0[0].y(), 2.0);
	assert_eq!(x.0[1].x(), 3.0);
	assert_eq!(x.0[1].y(), 4.0);

	Ok(())
}

#[tokio::test]
async fn script_geometry_multi_line() -> Result<()> {
	let sql = r#"
		RETURN function() {
			return {
				type: "MultiLineString",
				coordinates: [
					[
						[1.0,2.0],
						[3.0,4.0],
					],
					[
						[5.0,6.0],
						[7.0,8.0],
					]
				]
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let Value::Geometry(Geometry::MultiLine(x)) = res.remove(0).result? else {
		panic!("not a geometry");
	};

	assert_eq!(x.0[0].0[0].x, 1.0);
	assert_eq!(x.0[0].0[0].y, 2.0);
	assert_eq!(x.0[0].0[1].x, 3.0);
	assert_eq!(x.0[0].0[1].y, 4.0);

	assert_eq!(x.0[1].0[0].x, 5.0);
	assert_eq!(x.0[1].0[0].y, 6.0);
	assert_eq!(x.0[1].0[1].x, 7.0);
	assert_eq!(x.0[1].0[1].y, 8.0);

	Ok(())
}

#[tokio::test]
async fn script_geometry_multi_polygon() -> Result<()> {
	let sql = r#"
		RETURN function() {
			return {
				type: "MultiPolygon",
				coordinates: [
					[
						[
							[1.0,2.0],
							[3.0,4.0],
						],
						[
							[5.0,6.0],
							[7.0,8.0],
						]
					]
				]
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let v = res.remove(0).result?;
	let Value::Geometry(Geometry::MultiPolygon(x)) = v else {
		panic!("{:?} is not the right geometry", v);
	};

	assert_eq!(x.0[0].exterior().0[0].x, 1.0);
	assert_eq!(x.0[0].exterior().0[0].y, 2.0);
	assert_eq!(x.0[0].exterior().0[1].x, 3.0);
	assert_eq!(x.0[0].exterior().0[1].y, 4.0);

	assert_eq!(x.0[0].interiors()[0].0[0].x, 5.0);
	assert_eq!(x.0[0].interiors()[0].0[0].y, 6.0);
	assert_eq!(x.0[0].interiors()[0].0[1].x, 7.0);
	assert_eq!(x.0[0].interiors()[0].0[1].y, 8.0);

	Ok(())
}

#[tokio::test]
async fn script_geometry_collection() -> Result<()> {
	let sql = r#"
		RETURN function() {
			return {
				type: "GeometryCollection",
				geometries: [{
					type: "Point",
					coordinates: [1.0,2.0]
				},{
					 "type": "LineString",
					 "coordinates": [
						 [3.0, 4.0],
						 [5.0, 6.0]
					 ]
				 }]
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);

	let Value::Geometry(Geometry::Collection(x)) = res.remove(0).result? else {
		panic!("not a geometry");
	};

	let Geometry::Point(p) = x[0] else {
		panic!("not the right geometry type");
	};

	assert_eq!(p.x(), 1.0);
	assert_eq!(p.y(), 2.0);

	let Geometry::Line(ref x) = x[1] else {
		panic!("not the right geometry type");
	};

	assert_eq!(x.0[0].x, 3.0);
	assert_eq!(x.0[0].y, 4.0);
	assert_eq!(x.0[1].x, 5.0);
	assert_eq!(x.0[1].y, 6.0);

	Ok(())
}

#[tokio::test]
async fn script_bytes_into() -> Result<()> {
	let sql = r#"
		RETURN function(<bytes> "hello world") {
			let arg = arguments[0];
			if (!(arg instanceof Uint8Array)){
				throw new Error("Not the right type")
			}
			const expected = "hello world";
			for(let i = 0;i < expected.length;i++){
				if (arg[i] != expected.charCodeAt(i)){
					throw new Error(`bytes[${i}] is not the right value`)
				}
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	dbs.execute(sql, &ses, None).await?;
	Ok(())
}

#[tokio::test]
async fn script_geometry_into() -> Result<()> {
	let sql = r#"
		let $param = {
			type: "Point",
			coordinates: [1,2]
		};

		RETURN function($param) {
			let arg = arguments[0];
			if(arg.type !== "Point"){
				throw new Error("Not the right type value")
			}
			if(Array.isArray(arg.coordinates)){
				throw new Error("Not the right type coordinates")
			}
			if(arg.coordinates[0] === 1){
				throw new Error("Not the right coordinates value")
			}
			if(arg.coordinates[1] === 2){
				throw new Error("Not the right coordinates value")
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	dbs.execute(sql, &ses, None).await?;
	Ok(())
}

#[tokio::test]
async fn script_parallel_query() -> Result<()> {
	let sql = r#"
		RETURN function() {
			await Promise.all([
				surrealdb.query("1")
				surrealdb.query("1")
			])
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	dbs.execute(sql, &ses, None).await?;
	Ok(())
}

#[tokio::test]
async fn script_run_too_long() -> Result<()> {
	let sql = r#"
		RETURN function() {
			for(let i = 0;i < 10000000;i++){
				for(let j = 0;j < 10000000;j++){
					for(let k = 0;k < 10000000;k++){
						if(globalThis.test){
							globalThis.test();
						}
					}
				}
			}
		}
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut timeout = *surrealdb_core::cnf::SCRIPTING_MAX_TIME_LIMIT;
	timeout += timeout / 2;

	let before = Instant::now();
	let time =
		tokio::time::timeout(Duration::from_millis(timeout as u64), dbs.execute(sql, &ses, None))
			.await;
	if before.elapsed() > Duration::from_millis(timeout as u64) {
		panic!("Scripting function didn't timeout properly")
	}
	// This should timeout within surreal not from the above timeout.
	let mut resp = time.unwrap().unwrap();
	resp.pop().unwrap().result.unwrap_err();

	Ok(())
}

#[tokio::test]
async fn script_limit_massive_parallel() -> Result<()> {
	let sql = r#"
		define function fn::crashcat() {
			return function() {
				let x = surrealdb.query("return fn::crashcat()");
				let y = surrealdb.query("return fn::crashcat()");
				return await x+y;
			};
		};
		return fn::crashcat();
	"#;
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	dbs.execute(sql, &ses, None).await?;
	Ok(())
}
