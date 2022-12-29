mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

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
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 12);
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
	//
	Ok(())
}
