mod helpers;
use helpers::new_ds;
use surrealdb::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::val::Value;
use surrealdb_core::{strand, syn};

#[tokio::test]
async fn return_subquery_only() -> Result<()> {
	let sql = "
		USE NS test DB test;
		CREATE person:tobie SET name = 'Tobie';
		CREATE person:jaime SET name = 'Jaime';
		LET $single = person:tobie;
		--
		SELECT name FROM person;
		SELECT VALUE name FROM person;
		SELECT name FROM ONLY person;
		SELECT VALUE name FROM ONLY person;
		SELECT name FROM person:tobie;
		SELECT VALUE name FROM person:tobie;
		SELECT name FROM ONLY person:tobie;
		SELECT VALUE name FROM ONLY person:tobie;
		SELECT name FROM $single;
		SELECT VALUE name FROM $single;
		SELECT name FROM ONLY $single;
		SELECT VALUE name FROM ONLY $single;
		--
		RETURN SELECT name FROM person;
		RETURN SELECT VALUE name FROM person;
		RETURN SELECT name FROM ONLY person;
		RETURN SELECT VALUE name FROM ONLY person;
		RETURN SELECT name FROM person:tobie;
		RETURN SELECT VALUE name FROM person:tobie;
		RETURN SELECT name FROM ONLY person:tobie;
		RETURN SELECT VALUE name FROM ONLY person:tobie;
		RETURN SELECT name FROM $single;
		RETURN SELECT VALUE name FROM $single;
		RETURN SELECT name FROM ONLY $single;
		RETURN SELECT VALUE name FROM ONLY $single;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 28);

	// USE NS test DB test;
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// CREATE person:tobie SET name = 'Tobie';
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// CREATE person:jaime SET name = 'Jaime';
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// LET $single = person:tobie;
	let tmp = res.remove(0).result;
	tmp.unwrap();
	// SELECT name FROM person;
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ name: 'Jaime' }, { name: 'Tobie' }]").unwrap();
	assert_eq!(tmp, val);
	// SELECT VALUE name FROM person;
	let tmp = res.remove(0).result?;
	let val = syn::value("['Jaime', 'Tobie']").unwrap();
	assert_eq!(tmp, val);
	// SELECT name FROM ONLY person;
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Expected a single result output when using the ONLY keyword"#
	));
	// SELECT VALUE name FROM ONLY person;
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Expected a single result output when using the ONLY keyword"#
	));
	// SELECT name FROM person:tobie;
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ name: 'Tobie' }]").unwrap();
	assert_eq!(tmp, val);
	// SELECT VALUE name FROM person:tobie;
	let tmp = res.remove(0).result?;
	let val = syn::value("['Tobie']").unwrap();
	assert_eq!(tmp, val);
	// SELECT name FROM ONLY person:tobie;
	let tmp = res.remove(0).result?;
	let val = syn::value("{ name: 'Tobie' }").unwrap();
	assert_eq!(tmp, val);
	// SELECT VALUE name FROM person:tobie;
	let tmp = res.remove(0).result?;
	let val = Value::from(strand!("Tobie").to_owned());
	assert_eq!(tmp, val);
	// SELECT name FROM $single;
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ name: 'Tobie' }]").unwrap();
	assert_eq!(tmp, val);
	// SELECT VALUE name FROM ONLY $single;
	let tmp = res.remove(0).result?;
	let val = syn::value("['Tobie']").unwrap();
	assert_eq!(tmp, val);
	// SELECT VALUE name FROM $single;
	let tmp = res.remove(0).result?;
	let val = syn::value("{ name: 'Tobie' }").unwrap();
	assert_eq!(tmp, val);
	// SELECT name FROM ONLY $single;
	let tmp = res.remove(0).result?;
	let val = Value::from(strand!("Tobie").to_owned());
	assert_eq!(tmp, val);
	// SELECT VALUE name FROM ONLY $single;
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ name: 'Jaime' }, { name: 'Tobie' }]").unwrap();
	assert_eq!(tmp, val);
	// RETURN SELECT name FROM ONLY person;
	let tmp = res.remove(0).result?;
	let val = syn::value("['Jaime', 'Tobie']").unwrap();
	assert_eq!(tmp, val);
	// RETURN SELECT name FROM person;
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Expected a single result output when using the ONLY keyword"#
	));
	// RETURN SELECT VALUE name FROM person;
	let tmp = res.remove(0).result;
	assert!(matches!(
		tmp.err(),
		Some(e) if e.to_string() == r#"Expected a single result output when using the ONLY keyword"#
	));
	// RETURN SELECT name FROM person:tobie;
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ name: 'Tobie' }]").unwrap();
	assert_eq!(tmp, val);
	// RETURN SELECT VALUE name FROM person:tobie;
	let tmp = res.remove(0).result?;
	let val = syn::value("['Tobie']").unwrap();
	assert_eq!(tmp, val);
	// RETURN SELECT name FROM $single;
	let tmp = res.remove(0).result?;
	let val = syn::value("{ name: 'Tobie' }").unwrap();
	assert_eq!(tmp, val);
	// RETURN SELECT name FROM ONLY person:tobie;
	let tmp = res.remove(0).result?;
	let val = Value::from(strand!("Tobie").to_owned());
	assert_eq!(tmp, val);
	// RETURN SELECT VALUE name FROM ONLY person:tobie;
	let tmp = res.remove(0).result?;
	let val = syn::value("[{ name: 'Tobie' }]").unwrap();
	assert_eq!(tmp, val);
	// RETURN SELECT VALUE name FROM $single;
	let tmp = res.remove(0).result?;
	let val = syn::value("['Tobie']").unwrap();
	assert_eq!(tmp, val);
	// RETURN SELECT name FROM ONLY $single;
	let tmp = res.remove(0).result?;
	let val = syn::value("{ name: 'Tobie' }").unwrap();
	assert_eq!(tmp, val);
	// RETURN SELECT VALUE name FROM ONLY $single;
	let tmp = res.remove(0).result?;
	let val = Value::from(strand!("Tobie").to_owned());
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn return_breaks_nested_execution() -> Result<()> {
	let sql = "
		DEFINE FUNCTION fn::test() {
		    {
				RETURN 1;
			};
			RETURN 2;
		};

		RETURN fn::test();

		BEGIN;
		CREATE ONLY a:1;
		RETURN 1;
		CREATE ONLY a:2;
		COMMIT;

		{
            RETURN 1;
        };

        SELECT VALUE {
            IF $this % 2 == 0 {
            RETURN $this;
            } ELSE {
                RETURN $this + 1;
            }
        } FROM [1, 2, 3, 4];
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result;
	tmp.unwrap();
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("1").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("1").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("1").unwrap();
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = syn::value("[2, 2, 4, 4]").unwrap();
	assert_eq!(tmp, val);
	//
	Ok(())
}
