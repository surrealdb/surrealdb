mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;

#[tokio::test]
async fn closures() -> Result<(), Error> {
	let sql = "
		LET $double = |$n: number| $n * 2;
		$double(2);

		LET $pipe = |$arg| $arg;
		$pipe('abc');

		LET $rettype = |$arg| -> string { $arg };
		$rettype('works');
		$rettype(123);

		LET $argtype = |$arg: string| $arg;
		$argtype('works');
		$argtype(123);
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 10);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("4");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'abc'");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'works'");
	assert_eq!(tmp, val);
	//
	match res.remove(0).result {
		Err(Error::InvalidFunction { name, message }) if name == "ANONYMOUS" && message == "Expected this closure to return a value of type 'string', but found 'int'" => (),
		_ => panic!("Invocation should have failed with error: There was a problem running the ANONYMOUS() function. Expected this closure to return a value of type 'string', but found 'int'")
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'works'");
	assert_eq!(tmp, val);
	//
	match res.remove(0).result {
		Err(Error::InvalidArguments { name, message }) if name == "ANONYMOUS" && message == "Expected a value of type 'string' for argument $arg" => (),
		_ => panic!("Invocation should have failed with error: There was a problem running the ANONYMOUS() function. Expected a value of type 'string' for argument $arg")
	}
	//
	let tmp = res.remove(0).result?;
	let val = Value::None;
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("'done'");
	assert_eq!(tmp, val);
	//
	match res.remove(0).result {
		Err(Error::ComputationDepthExceeded) => (),
		_ => panic!("Invocation should have failed with error: Reached excessive computation depth due to functions, subqueries, or futures")
	}
	//
	Ok(())
}

#[tokio::test]
async fn closures_inline() -> Result<(), Error> {
	let sql = "
		(||1)();
		{||2}();
		{ a: ||3 }.a();
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 3);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("1");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("2");
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("3");
	assert_eq!(tmp, val);
	//
	Ok(())
}
