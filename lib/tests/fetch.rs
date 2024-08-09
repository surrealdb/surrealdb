mod parse;
use parse::Parse;
mod helpers;
use crate::helpers::skip_ok;
use helpers::new_ds;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::sql::Value;
use surrealdb_core::sql::Number;

#[tokio::test]
async fn create_relate_select() -> Result<(), Error> {
	let sql = "
		CREATE user:tobie SET name = 'Tobie';
		CREATE user:jaime SET name = 'Jaime';
		CREATE product:phone SET price = 1000;
		CREATE product:laptop SET price = 3000;
		RELATE user:tobie->bought->product:phone SET id = bought:1, payment_method = 'VISA';
		RELATE user:tobie->bought->product:laptop SET id = bought:2, payment_method = 'VISA';
		RELATE user:jaime->bought->product:laptop SET id = bought:3, payment_method = 'VISA';
		SELECT *, ->bought AS purchases FROM user;
		SELECT *, ->bought.out.* AS products FROM user;
		SELECT *, ->bought->product.* AS products FROM user;
		SELECT *, ->bought AS products FROM user FETCH products;
		SELECT *, ->(bought AS purchases) FROM user FETCH purchases, purchases.out;
		LET $param1 = 'purchases';
		SELECT *, ->(bought AS purchases) FROM user FETCH $param1, purchases.out;
		SELECT *, ->(bought AS purchases) FROM user FETCH type::field('purchases'), purchases.out;
		SELECT *, ->(bought AS purchases) FROM user FETCH type::fields([$param1, 'purchases.out']);
		LET $faultyparam = 1.0f;
		SELECT *, ->(bought AS purchases) FROM user FETCH $faultyparam, purchases.out;
	";
	let dbs = new_ds().await?;
	let ses = Session::owner().with_ns("test").with_db("test");
	let res = &mut dbs.execute(sql, &ses, None).await?;
	assert_eq!(res.len(), 18);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:tobie,
				name: 'Tobie'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:jaime,
				name: 'Jaime'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:phone,
				price: 1000
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: product:laptop,
				price: 3000
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				'id': bought:1,
				'in': user:tobie,
				'out': product:phone,
				'payment_method': 'VISA'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				'id': bought:2,
				'in': user:tobie,
				'out': product:laptop,
				'payment_method': 'VISA'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				'id': bought:3,
				'in': user:jaime,
				'out': product:laptop,
				'payment_method': 'VISA'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:jaime,
				name: 'Jaime',
				purchases: [
					bought:3
				]
			},
			{
				id: user:tobie,
				name: 'Tobie',
				purchases: [
					bought:1,
					bought:2
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:jaime,
				name: 'Jaime',
				products: [
					{
						id: product:laptop,
						price: 3000
					}
				]
			},
			{
				id: user:tobie,
				name: 'Tobie',
				products: [
					{
						id: product:phone,
						price: 1000
					},
					{
						id: product:laptop,
						price: 3000
					}
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:jaime,
				name: 'Jaime',
				products: [
					{
						id: product:laptop,
						price: 3000
					}
				]
			},
			{
				id: user:tobie,
				name: 'Tobie',
				products: [
					{
						id: product:phone,
						price: 1000
					},
					{
						id: product:laptop,
						price: 3000
					}
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:jaime,
				name: 'Jaime',
				products: [
					{
						id: bought:3,
						in: user:jaime,
						out: product:laptop,
						payment_method: 'VISA'
					}
				]
			},
			{
				id: user:tobie,
				name: 'Tobie',
				products: [
					{
						id: bought:1,
						in: user:tobie,
						out: product:phone,
						payment_method: 'VISA'
					},
					{
						id: bought:2,
						in: user:tobie,
						out: product:laptop,
						payment_method: 'VISA'
					}
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: user:jaime,
				name: 'Jaime',
				purchases: [
					{
						id: bought:3,
						in: user:jaime,
						out: {
							id: product:laptop,
							price: 3000
						},
						payment_method: 'VISA'
					}
				]
			},
			{
				id: user:tobie,
				name: 'Tobie',
				purchases: [
					{
						id: bought:1,
						in: user:tobie,
						out: {
							id: product:phone,
							price: 1000
						},
						payment_method: 'VISA'
					},
					{
						id: bought:2,
						in: user:tobie,
						out: {
							id: product:laptop,
							price: 3000
						},
						payment_method: 'VISA'
					}
				]
			}
		]",
	);
	assert_eq!(tmp, val);
	// Skip the LET $param statements
	skip_ok(res, 1)?;
	//
	for i in 0..3 {
		let tmp = res.remove(0).result.unwrap_or_else(|e| panic!("{i} {e}"));
		let val = Value::parse(
			"[
			{
				id: user:jaime,
				name: 'Jaime',
				purchases: [
					{
						id: bought:3,
						in: user:jaime,
						out: {
							id: product:laptop,
							price: 3000
						},
						payment_method: 'VISA'
					}
				]
			},
			{
				id: user:tobie,
				name: 'Tobie',
				purchases: [
					{
						id: bought:1,
						in: user:tobie,
						out: {
							id: product:phone,
							price: 1000
						},
						payment_method: 'VISA'
					},
					{
						id: bought:2,
						in: user:tobie,
						out: {
							id: product:laptop,
							price: 3000
						},
						payment_method: 'VISA'
					}
				]
			}
		]",
		);
		assert_eq!(tmp, val, "{i}");
	}
	// Ignore LET statement result
	res.remove(0);
	match res.remove(0).result {
		Err(Error::InvalidFetch {
			value: Value::Number(Number::Float(1.0)),
		}) => {}
		found => panic!("Expected Err(Error::InvalidFetch), found '{found:?}'"),
	};
	assert_eq!(tmp, val);
	//
	Ok(())
}
