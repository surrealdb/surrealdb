mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn spread_operator_object() -> Result<(), Error> {
	let sql = "
		{
			...{ a: 1 },
			b: 2
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			a: 1,
			b: 2
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn spread_operator_object_overwrite_from_spread() -> Result<(), Error> {
	let sql = "
		{
			...{ a: 1 },
			a: 2
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			a: 2
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn spread_operator_object_complex_values() -> Result<(), Error> {
	let sql = "
		CREATE a:1 set a = 1;
		LET $b = { b: 2 };
		CREATE c:3;

		{
			...a:1,
			...$b,
			...(SELECT * FROM ONLY c LIMIT 1),
			d: 4,
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 4);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"{
			a: 1,
			b: 2,
			id: c:3,
			d: 4
		}",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn spread_operator_object_only_objects() -> Result<(), Error> {
	let sql = "
		{
			...123
		};
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let _tmp = res.remove(0).result;
	let expected: Result<Value, Error> = Err(Error::InvalidSpreadValue {
		expected: "an Object".into(),
	});
	assert!(matches!(expected, _tmp));
	//
	Ok(())
}

#[tokio::test]
async fn spread_operator_array() -> Result<(), Error> {
	let sql = "
		[ 1, ...[2], 3, ...[4], 5 ];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[ 1, 2, 3, 4, 5 ]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn spread_operator_array_complex_values() -> Result<(), Error> {
	let sql = "
		CREATE a:1, a:2, a:3;
		[ 'three records follow', ...(SELECT VALUE id FROM a) ];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[ 'three records follow', a:1, a:2, a:3 ]");
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn spread_operator_array_only_arrays() -> Result<(), Error> {
	let sql = "
		[
			...123
		];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 1);
	//
	let _tmp = res.remove(0).result;
	let expected: Result<Value, Error> = Err(Error::InvalidSpreadValue {
		expected: "an Array".into(),
	});
	assert!(matches!(expected, _tmp));
	//
	Ok(())
}
