mod parse;
use parse::Parse;
mod helpers;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Thing;
use surrealdb::sql::Value;

#[tokio::test]
async fn field_definition_value_assert_failure() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD age ON person TYPE number ASSERT $value > 0;
		DEFINE FIELD email ON person TYPE string ASSERT string::is::email($value);
		DEFINE FIELD name ON person TYPE option<string> VALUE $value OR 'No name';
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore';
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = NONE;
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = NULL;
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = 0;
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = 13;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 9);
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
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Found NONE for field `age`, with record `person:test`, but expected a number",

		),
		"{}",
		tmp.unwrap_err().to_string()
	);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Found NONE for field `age`, with record `person:test`, but expected a number"
		),
		"{}",
		tmp.unwrap_err().to_string()
	);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Found NULL for field `age`, with record `person:test`, but expected a number"
		),
		"{}",
		tmp.unwrap_err().to_string()
	);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Found 0 for field `age`, with record `person:test`, but field must conform to: $value > 0"
		),
		"{}",
		tmp.unwrap_err().to_string()
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				age: 13,
				email: 'info@surrealdb.com',
				id: person:test,
				name: 'No name',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_value_assert_success() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD age ON person TYPE number ASSERT $value > 0;
		DEFINE FIELD email ON person TYPE string ASSERT string::is::email($value);
		DEFINE FIELD name ON person TYPE option<string> VALUE $value OR 'No name';
		CREATE person:test SET email = 'info@surrealdb.com', other = 'ignore', age = 22;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
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
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				email: 'info@surrealdb.com',
				age: 22,
				name: 'No name',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_empty_nested_objects() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD settings on person TYPE object;
		UPDATE person:test CONTENT {
		    settings: {
		        nested: {
		            object: {
						thing: 'test'
					}
		        }
		    }
		};
		SELECT * FROM person;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				settings: {},
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				settings: {},
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_empty_nested_arrays() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD settings on person TYPE object;
		UPDATE person:test CONTENT {
		    settings: {
		        nested: [
					1,
					2,
					3,
					4,
					5
				]
		    }
		};
		SELECT * FROM person;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				settings: {},
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				settings: {},
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_empty_nested_flexible() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE person SCHEMAFULL;
		DEFINE FIELD settings on person FLEXIBLE TYPE object;
		UPDATE person:test CONTENT {
		    settings: {
				nested: {
		            object: {
						thing: 'test'
					}
		        }
		    }
		};
		SELECT * FROM person;
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
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				settings: {
					nested: {
			            object: {
							thing: 'test'
						}
			        }
				},
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				settings: {
					nested: {
			            object: {
							thing: 'test'
						}
			        }
				},
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_selection_variable_field_projection() -> Result<(), Error> {
	let sql = "
		CREATE person:test SET title = 'Mr', name.first = 'Tobie', name.last = 'Morgan Hitchcock';
		LET $param = 'name.first';
		SELECT type::field($param), type::field('name.last') FROM person;
		SELECT VALUE { 'firstname': type::field($param), lastname: type::field('name.last') } FROM person;
		SELECT VALUE [type::field($param), type::field('name.last')] FROM person;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				title: 'Mr',
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				firstname: 'Tobie',
				lastname: 'Morgan Hitchcock',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			['Tobie', 'Morgan Hitchcock']
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_selection_variable_fields_projection() -> Result<(), Error> {
	let sql = "
		CREATE person:test SET title = 'Mr', name.first = 'Tobie', name.last = 'Morgan Hitchcock';
		LET $param = ['name.first', 'name.last'];
		SELECT type::fields($param), type::fields(['title']) FROM person;
		SELECT VALUE { 'names': type::fields($param) } FROM person;
		SELECT VALUE type::fields($param) FROM person;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 5);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: person:test,
				title: 'Mr',
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				title: 'Mr',
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				}
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				names: ['Tobie', 'Morgan Hitchcock']
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			['Tobie', 'Morgan Hitchcock']
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_default_value() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE product SCHEMAFULL;
		DEFINE FIELD primary ON product TYPE number VALUE 123.456;
		DEFINE FIELD secondary ON product TYPE bool DEFAULT true VALUE $value;
		DEFINE FIELD tertiary ON product TYPE string DEFAULT 'hello' VALUE 'tester';
		--
		CREATE product:test SET primary = NULL;
		CREATE product:test SET secondary = 'oops';
		CREATE product:test SET tertiary = 123;
		CREATE product:test;
		--
		UPDATE product:test SET primary = 654.321;
		UPDATE product:test SET secondary = false;
		UPDATE product:test SET tertiary = 'something';
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 11);
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
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Found NULL for field `primary`, with record `product:test`, but expected a number"
		),
		"{}",
		tmp.unwrap_err().to_string()
	);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Found 'oops' for field `secondary`, with record `product:test`, but expected a bool"
		),
		"{}",
		tmp.unwrap_err().to_string()
	);
	//
	let tmp = res.remove(0).result;
	assert!(
		matches!(
			&tmp,
			Err(e) if e.to_string() == "Found 123 for field `tertiary`, with record `product:test`, but expected a string"
		),
		"{}",
		tmp.unwrap_err().to_string()
	);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:test,
				primary: 123.456,
				secondary: true,
				tertiary: 'tester',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:test,
				primary: 123.456,
				secondary: true,
				tertiary: 'tester',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:test,
				primary: 123.456,
				secondary: false,
				tertiary: 'tester',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:test,
				primary: 123.456,
				secondary: false,
				tertiary: 'tester',
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_value_reference() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE product;
		DEFINE FIELD subproducts ON product VALUE ->contains->product;
		CREATE product:one, product:two;
		RELATE product:one->contains:test->product:two;
		SELECT * FROM product;
		UPDATE product;
		SELECT * FROM product;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: contains:test,
				in: product:one,
				out: product:two,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_value_reference_with_future() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE product;
		DEFINE FIELD subproducts ON product VALUE <future> { ->contains->product };
		CREATE product:one, product:two;
		RELATE product:one->contains:test->product:two;
		SELECT * FROM product;
		UPDATE product;
		SELECT * FROM product;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: contains:test,
				in: product:one,
				out: product:two,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:one,
				subproducts: [
					product:two,
				],
			},
			{
				id: product:two,
				subproducts: [],
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn field_definition_edge_permissions() -> Result<(), Error> {
	let sql = "
		DEFINE TABLE user SCHEMAFULL;
		DEFINE TABLE business SCHEMAFULL;
		DEFINE FIELD owner ON TABLE business TYPE record<user>;
		DEFINE TABLE contact SCHEMAFULL PERMISSIONS FOR create WHERE in.owner.id = $auth.id;
		INSERT INTO user (id, name) VALUES (user:one, 'John'), (user:two, 'Lucy');
		INSERT INTO business (id, owner) VALUES (business:one, user:one), (business:two, user:two);
	";
	let dbs = new_ds().await?.with_auth_enabled(true);
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 6);
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
	let tmp = res.remove(0).result;
	assert!(tmp.is_ok());
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:one,
			},
			{
				id: user:two,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: business:one,
				owner: user:one,
			},
			{
				id: business:two,
				owner: user:two,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let sql = "
		RELATE business:one->contact:one->business:two;
		RELATE business:two->contact:two->business:one;
	";
	let ses = Session::for_scope("test", "test", "test", Thing::from(("user", "one")).into());
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 2);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: contact:one,
				in: business:one,
				out: business:two,
			},
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse("[]");
	assert_eq!(tmp, val);
	//
	Ok(())
}
