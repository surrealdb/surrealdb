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
		SELECT count(), time::year(time) AS year, country FROM temperature GROUP BY country, year;
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
				count: 3,
				country: 'GBP',
				year: 2020
			},
			{
				count: 2,
				country: 'GBP',
				year: 2021
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

#[tokio::test]
async fn select_multi_aggregate() -> Result<(), Error> {
	let sql = "
		CREATE test:1 SET group = 1, one = 1.7, two = 2.4;
		CREATE test:2 SET group = 1, one = 4.7, two = 3.9;
		CREATE test:3 SET group = 2, one = 3.2, two = 9.7;
		CREATE test:4 SET group = 2, one = 4.4, two = 3.0;
		SELECT group, math::sum(one) AS one, math::sum(two) AS two FROM test GROUP BY group;
		SELECT group, math::sum(two) AS two, math::sum(one) AS one FROM test GROUP BY group;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 6);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:1,
				group: 1,
				one: 1.7,
				two: 2.4,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:2,
				group: 1,
				one: 4.7,
				two: 3.9,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:3,
				group: 2,
				one: 3.2,
				two: 9.7,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:4,
				group: 2,
				one: 4.4,
				two: 3.0,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				group: 1,
				one: 6.4,
				two: 6.3,
			},
			{
				group: 2,
				one: 7.6,
				two: 12.7,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				group: 1,
				one: 6.4,
				two: 6.3,
			},
			{
				group: 2,
				one: 7.6,
				two: 12.7,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}

#[tokio::test]
async fn select_multi_aggregate_composed() -> Result<(), Error> {
	let sql = "
		CREATE test:1 SET group = 1, one = 1.7, two = 2.4;
		CREATE test:2 SET group = 1, one = 4.7, two = 3.9;
		CREATE test:3 SET group = 2, one = 3.2, two = 9.7;
		CREATE test:4 SET group = 2, one = 4.4, two = 3.0;
		SELECT group, math::sum(math::floor(one)) AS one, math::sum(math::floor(two)) AS two FROM test GROUP BY group;
		SELECT group, math::sum(math::round(one)) AS one, math::sum(math::round(two)) AS two FROM test GROUP BY group;
		SELECT group, math::sum(math::ceil(one)) AS one, math::sum(math::ceil(two)) AS two FROM test GROUP BY group;
	";
	let dbs = Datastore::new("memory").await?;
	let ses = Session::for_kv().with_ns("test").with_db("test");
	let res = &mut dbs.execute(&sql, &ses, None, false).await?;
	assert_eq!(res.len(), 7);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:1,
				group: 1,
				one: 1.7,
				two: 2.4,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:2,
				group: 1,
				one: 4.7,
				two: 3.9,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:3,
				group: 2,
				one: 3.2,
				two: 9.7,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				id: test:4,
				group: 2,
				one: 4.4,
				two: 3.0,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				group: 1,
				one: 5,
				two: 5,
			},
			{
				group: 2,
				one: 7,
				two: 12,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				group: 1,
				one: 7,
				two: 6,
			},
			{
				group: 2,
				one: 7,
				two: 13,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	let tmp = res.remove(0).result?;
	let val = Value::parse(
		"[
			{
				group: 1,
				one: 7,
				two: 7,
			},
			{
				group: 2,
				one: 9,
				two: 14,
			}
		]",
	);
	assert_eq!(tmp, val);
	//
	Ok(())
}
