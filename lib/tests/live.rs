mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql;
use surrealdb::sql::{Number, Value};

#[tokio::test]
async fn live_evaluates_param_before_storage() -> Result<(), Error> {
	let sql = r#"
		LET $number = 100;
		INSERT INTO blarf {"number": "whatever, we just need a created table"};
		LIVE SELECT * FROM blarf WHERE number = $number;
	"#;
	// TODO we should introduce a table parameter. Currently as of writing that seems difficult
	// LIVE SELECT * FROM $table WHERE number = $number;
	let dbs = new_ds().await?;
	let ns = "0e2f09def6c5449db63c81493e50f2a7";
	let db = "9ac6fad4b41540ea87e6b84a18ef2a9c";
	let ses = Session::owner().with_ns(ns).with_db(db).with_rt(true);
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 3);
	let tmp = res.remove(0).result.unwrap();
	assert_eq!(tmp, Value::None);
	res.remove(0).result.unwrap(); // We don't care about the insert
	let tmp = res.remove(0).result.unwrap();
	let live_query_id = match tmp {
		Value::Uuid(uid) => uid,
		_ => {
			panic!("Expected a Uuid")
		}
	};

	// Create some data under a different param
	let sql = r#"
		LET $number = 20; -- Set deliberately so that we try to change the context of the LQ compute
		INSERT INTO blarf {"number": 100};
	"#;
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 2);
	let tmp = res.remove(0).result.unwrap();
	assert_eq!(tmp, Value::None);
	let tmp = res.remove(0).result.unwrap();
	match tmp {
		Value::Array(sql::Array(vec)) if vec.len() == 1 => {
			if let Value::Object(obj) = &vec[0] {
				assert_eq!(obj.len(), 2); // id and number
				assert_eq!(obj["number"], Value::Number(Number::from(100)));
			} else {
				panic!("Expected an object")
			}
		}
		_ => {
			panic!("Expected an object: {:?}", tmp)
		}
	}

	// Verify the lq notifications
	let chan = dbs.notifications().unwrap();
	let not = chan.try_recv().unwrap();
	assert_eq!(not.id, live_query_id);
	let not_obj = match not.result {
		Value::Object(obj) => obj,
		_ => {
			panic!("Expected an object: {:?}", not.result)
		}
	};
	assert_eq!(not_obj["number"], Value::Number(Number::Int(100)));

	// Verify true negative
	let sql = r#"
		LET $number = 20; -- Set deliberately so that we try to change the context of the LQ compute
		INSERT INTO blarf {"number": $number};
	"#;
	let res = &mut dbs.execute(sql, &ses, None).await.unwrap();
	assert_eq!(res.len(), 2);
	let tmp = res.remove(0).result.unwrap();
	assert_eq!(tmp, Value::None);
	let tmp = res.remove(0).result.unwrap();
	match tmp {
		Value::Object(obj) => {
			assert_eq!(obj.len(), 1);
			assert_eq!(obj["number"], Value::Number(Number::Int(20)));
		}
		_ => {
			panic!("Expected an object")
		}
	}

	// Verify the lq notifications
	let not = dbs.notifications().unwrap().try_recv();
	assert!(not.is_err());

	Ok(())
}
