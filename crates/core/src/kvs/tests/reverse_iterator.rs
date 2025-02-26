use crate::dbs::node::Timestamp;
use crate::dbs::{Response, Session};
use crate::kvs::clock::{FakeClock, SizedClock};
use crate::kvs::tests::CreateDs;
use std::sync::Arc;
use uuid::Uuid;

async fn test(new_ds: impl CreateDs, index: &str) -> Vec<Response> {
	// Create a new datastore
	let node_id = Uuid::parse_str("056804f2-b379-4397-9ceb-af8ebd527beb").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;

	let sql = format!(
		"USE NS test;
		USE DB test;
		{index};
		CREATE |i:1500| SET v = rand::uuid::v7() RETURN NONE;
		SELECT v FROM i ORDER BY v DESC LIMIT 3 EXPLAIN;
		SELECT v FROM i ORDER BY v DESC LIMIT 3;
		SELECT v FROM i ORDER BY v DESC EXPLAIN;
		SELECT v FROM i ORDER BY v DESC;"
	);

	let mut r = ds.execute(&sql, &Session::owner(), None).await.unwrap();
	assert_eq!(r.len(), 8);
	// Check the first statements are successful
	for _ in 0..4 {
		r.remove(0).result.unwrap();
	}
	r
}

fn check(r: &mut Vec<Response>, tmp: &str) {
	let tmp = Value::parse(tmp);
	let val = r.remove(0).result.unwrap();
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
	let ref mut r = test(new_ds, "DEFINE INDEX idx ON TABLE i COLUMNS v").await;
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
	let ref mut r = test(new_ds, "DEFINE INDEX idx ON TABLE i COLUMNS v UNIQUE").await;
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
	};
}
use crate::sql::Value;
use crate::syn::Parse;
pub(crate) use define_tests;
