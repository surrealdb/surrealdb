use std::sync::Arc;

use uuid::Uuid;

use crate::dbs::node::Timestamp;
use crate::dbs::{Response, Session};
use crate::kvs::clock::{FakeClock, SizedClock};
use crate::kvs::tests::CreateDs;
use crate::syn;
use crate::val::Value;

async fn test(new_ds: impl CreateDs, index: &str) -> Vec<Response> {
	// Create a new datastore
	let node_id = Uuid::parse_str("056804f2-b379-4397-9ceb-af8ebd527beb").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;

	let sql = format!(
		"USE NS test;
		USE DB test;
		{index};
		FOR $i IN 1..=1500 {{ CREATE i:[$i] SET v = $i; }};
        SELECT v FROM i WHERE v > 500 ORDER BY v DESC LIMIT 3 EXPLAIN;
		SELECT v FROM i WHERE v > 500 ORDER BY v DESC LIMIT 3;
		SELECT v FROM i ORDER BY v DESC LIMIT 3 EXPLAIN;
		SELECT v FROM i ORDER BY v DESC LIMIT 3;
		SELECT v FROM i ORDER BY v DESC EXPLAIN;
		SELECT v FROM i ORDER BY v DESC;"
	);

	let mut r = ds.execute(&sql, &Session::owner(), None).await.unwrap();
	assert_eq!(r.len(), 10);
	// Check the first statements are successful
	for _ in 0..4 {
		r.remove(0).result.unwrap();
	}
	r
}

fn check(r: &mut Vec<Response>, tmp: &str) {
	let tmp = syn::value(tmp).unwrap();
	let val = match r.remove(0).result {
		Ok(v) => v,
		Err(err) => panic!("{err}"),
	};
	assert_eq!(format!("{val:#}"), format!("{tmp:#}"));
}

/// Extract the array from a value
fn check_array_is_sorted(v: &Value, expected_len: usize) {
	if let Value::Array(a) = v {
		assert_eq!(a.len(), expected_len);
		assert!(a.windows(2).all(|w| w[0] > w[1]), "Values are not sorted: {a:?}");
	} else {
		panic!("Expected a Value::Array but get: {v}");
	}
}

pub async fn standard(new_ds: impl CreateDs) {
	let r = &mut (test(new_ds, "DEFINE INDEX idx ON TABLE i COLUMNS v").await);
	check(
		r,
		"[
			{
				detail: {
					plan: {
						direction: 'backward',
						from: {
							inclusive: false,
							value: 500
						},
						index: 'idx',
						to: {
							inclusive: false,
							value: NONE
						}
					},
					table: 'i'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					limit: 3,
					type: 'MemoryOrderedLimit'
				},
				operation: 'Collector'
			}
		]",
	);
	check_array_is_sorted(&r.remove(0).result.unwrap(), 3);
	check(
		r,
		"[
			{
				detail: {
					plan: {
						index: 'idx',
						operator: 'ReverseOrder'
					},
					table: 'i'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					limit: 3,
					type: 'MemoryOrderedLimit'
				},
				operation: 'Collector'
			}
		]",
	);
	check_array_is_sorted(&r.remove(0).result.unwrap(), 3);
	check(
		r,
		"[
			{
				detail: {
					plan: {
						index: 'idx',
						operator: 'ReverseOrder'
					},
					table: 'i'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					type: 'MemoryOrdered'
				},
				operation: 'Collector'
			}
		]",
	);
	check_array_is_sorted(&r.remove(0).result.unwrap(), 1500);
}

pub async fn unique(new_ds: impl CreateDs) {
	let r = &mut (test(new_ds, "DEFINE INDEX idx ON TABLE i COLUMNS v UNIQUE").await);
	check(
		r,
		"[
			{
				detail: {
					plan: {
						direction: 'backward',
						from: {
							inclusive: false,
							value: 500
						},
						index: 'idx',
						to: {
							inclusive: false,
							value: NONE
						}
					},
					table: 'i'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					limit: 3,
					type: 'MemoryOrderedLimit'
				},
				operation: 'Collector'
			}
		]",
	);
	check_array_is_sorted(&r.remove(0).result.unwrap(), 3);
	check(
		r,
		"[
			{
				detail: {
					plan: {
						index: 'idx',
						operator: 'ReverseOrder'
					},
					table: 'i'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					limit: 3,
					type: 'MemoryOrderedLimit'
				},
				operation: 'Collector'
			}
		]",
	);
	check_array_is_sorted(&r.remove(0).result.unwrap(), 3);
	check(
		r,
		"[
			{
				detail: {
					plan: {
						index: 'idx',
						operator: 'ReverseOrder'
					},
					table: 'i'
				},
				operation: 'Iterate Index'
			},
			{
				detail: {
					type: 'MemoryOrdered'
				},
				operation: 'Collector'
			}
		]",
	);
	check_array_is_sorted(&r.remove(0).result.unwrap(), 1500);
}

pub async fn range(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("056804f2-b379-4397-9ceb-af8ebd527beb").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;

	// Run the test
	let sql = "
		USE NS test; USE DB test;
		FOR $i IN 1..1500 { CREATE t:[$i]; };
		SELECT * FROM t:[500]..=[550] ORDER BY id DESC LIMIT 3;
		SELECT * FROM t:[500]..[550] ORDER BY id DESC LIMIT 3;
		SELECT * FROM t:[500]..=[550] ORDER BY id DESC LIMIT 3 EXPLAIN;
	";
	let mut r = ds.execute(sql, &Session::owner(), None).await.unwrap();
	//Check the result
	for _ in 0..3 {
		check(&mut r, "NONE");
	}
	check(&mut r, "[{ id: t:[550]},{ id: t:[549] },{ id: t:[548] }]");
	check(&mut r, "[{ id: t:[549]},{ id: t:[548] },{ id: t:[547] }]");
	check(
		&mut r,
		"[
				{
					detail: {
						direction: 'backward',
						range: [
							500
						]..=[
							550
						],
						table: 't'
					},
					operation: 'Iterate Range'
				},
				{
					detail: {
						limit: 3,
						type: 'MemoryOrderedLimit'
					},
					operation: 'Collector'
				}
			]",
	);
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn reverse_iterator_standard() {
			super::reverse_iterator::standard($new_ds).await;
		}
		#[tokio::test]
		#[serial_test::serial]
		async fn reverse_iterator_unique() {
			super::reverse_iterator::unique($new_ds).await;
		}
		#[tokio::test]
		#[serial_test::serial]
		async fn reverse_iterator_range() {
			super::reverse_iterator::range($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
