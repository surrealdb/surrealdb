mod parse;
use parse::Parse;
use surrealdb::dbs::Session;
use surrealdb::err::Error;
use surrealdb::kvs::Datastore;
use surrealdb::sql::Value;

#[tokio::test]
async fn select_limit_fetch() -> Result<(), Error> {
	let sql = "
		CREATE temperature:1 SET country = 'GBP', time = '2020-01-01T08:00:00Z';
		CREATE temperature:2 SET country = 'GBP', time = '2020-02-01T08:00:00Z';
		CREATE temperature:3 SET country = 'GBP', time = '2020-03-01T08:00:00Z';
		CREATE temperature:4 SET country = 'GBP', time = '2021-01-01T08:00:00Z';
		CREATE temperature:5 SET country = 'GBP', time = '2021-01-01T08:00:00Z';
		CREATE temperature:6 SET country = 'EUR', time = '2021-01-01T08:00:00Z';
		CREATE temperature:7 SET country = 'USD', time = '2021-01-01T08:00:00Z';
		CREATE temperature:8 SET country = 'AUD', time = '2021-01-01T08:00:00Z';
		CREATE temperature:9 SET country = 'CHF', time = '2023-01-01T08:00:00Z';
		SELECT *, time::year(time) AS year FROM temperature;
		SELECT count(), time::year(time) AS year, country FROM temperature GROUP BY country;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 11);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:1,
				time: '2020-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:2,
				time: '2020-02-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:3,
				time: '2020-03-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:4,
				time: '2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:5,
				time: '2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'EUR',
				id: temperature:6,
				time: '2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'USD',
				id: temperature:7,
				time: '2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'AUD',
				id: temperature:8,
				time: '2021-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'CHF',
				id: temperature:9,
				time: '2023-01-01T08:00:00Z'
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				country: 'GBP',
				id: temperature:1,
				time: '2020-01-01T08:00:00Z',
				year: 2020
			},
			{
				country: 'GBP',
				id: temperature:2,
				time: '2020-02-01T08:00:00Z',
				year: 2020
			},
			{
				country: 'GBP',
				id: temperature:3,
				time: '2020-03-01T08:00:00Z',
				year: 2020
			},
			{
				country: 'GBP',
				id: temperature:4,
				time: '2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'GBP',
				id: temperature:5,
				time: '2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'EUR',
				id: temperature:6,
				time: '2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'USD',
				id: temperature:7,
				time: '2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'AUD',
				id: temperature:8,
				time: '2021-01-01T08:00:00Z',
				year: 2021
			},
			{
				country: 'CHF',
				id: temperature:9,
				time: '2023-01-01T08:00:00Z',
				year: 2023
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				count: 1,
				country: 'AUD',
				year: 2021
			},
			{
				count: 1,
				country: 'CHF',
				year: 2023
			},
			{
				count: 1,
				country: 'EUR',
				year: 2021
			},
			{
				count: 5,
				country: 'GBP',
				year: 2020
			},
			{
				count: 1,
				country: 'USD',
				year: 2021
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
