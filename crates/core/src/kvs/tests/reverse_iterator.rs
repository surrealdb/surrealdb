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
		CREATE session:1 SET time = d'2024-07-01T01:00:00Z';
		CREATE session:2 SET time = d'2024-06-30T23:00:00Z';
		CREATE session:3 SET other = 'test';
		CREATE session:4 SET time = null;
		CREATE session:5 SET time = d'2024-07-01T02:00:00Z';
		CREATE session:6 SET time = d'2024-06-30T23:30:00Z';
		SELECT * FROM session ORDER BY time DESC LIMIT 3 EXPLAIN;
		SELECT * FROM session ORDER BY time DESC LIMIT 3;
		SELECT * FROM session ORDER BY time DESC EXPLAIN;
		SELECT * FROM session ORDER BY time DESC;"
	);

	let mut r = ds.execute(&sql, &Session::owner(), None).await.unwrap();
	assert_eq!(r.len(), 13);
	// Check the first 7 statements are successful
	for _ in 0..9 {
		r.remove(0).result.unwrap();
	}
	r
}

fn check(r: &mut Vec<Response>, tmp: &str) {
	let tmp = Value::parse(tmp);
	let val = r.remove(0).result.unwrap();
	assert_eq!(format!("{val:#}"), format!("{tmp:#}"));
}

pub async fn standard(new_ds: impl CreateDs) {
	let ref mut r = test(new_ds, "DEFINE INDEX time ON TABLE session COLUMNS time").await;
	check(
		r,
		"[
			{
				detail: {
					plan: {
						index: 'time',
						operator: 'ReverseOrder'
					},
					table: 'session'
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
	check(
		r,
		"[
			{
				id: session:5,
				time: d'2024-07-01T02:00:00Z'
			},
			{
				id: session:1,
				time: d'2024-07-01T01:00:00Z'
			},
			{
				id: session:6,
				time: d'2024-06-30T23:30:00Z'
			}
		]",
	);
	check(
		r,
		"[
			{
				detail: {
					plan: {
						index: 'time',
						operator: 'ReverseOrder'
					},
					table: 'session'
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
	check(
		r,
		"[
			{
				id: session:5,
				time: d'2024-07-01T02:00:00Z'
			},
			{
				id: session:1,
				time: d'2024-07-01T01:00:00Z'
			},
			{
				id: session:6,
				time: d'2024-06-30T23:30:00Z'
			},
			{
				id: session:2,
				time: d'2024-06-30T23:00:00Z'
			}
		]",
	);
}

pub async fn unique(new_ds: impl CreateDs) {
	let ref mut r = test(new_ds, "DEFINE INDEX time ON TABLE session COLUMNS time UNIQUE").await;
	check(
		r,
		"[
			{
				detail: {
					plan: {
						index: 'time',
						operator: 'ReverseOrder'
					},
					table: 'session'
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
	check(
		r,
		"[
			{
				id: session:5,
				time: d'2024-07-01T02:00:00Z'
			},
			{
				id: session:1,
				time: d'2024-07-01T01:00:00Z'
			},
			{
				id: session:6,
				time: d'2024-06-30T23:30:00Z'
			}
		]",
	);
	check(
		r,
		"[
			{
				detail: {
					plan: {
						index: 'time',
						operator: 'ReverseOrder'
					},
					table: 'session'
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
	check(
		r,
		"[
			{
				id: session:5,
				time: d'2024-07-01T02:00:00Z'
			},
			{
				id: session:1,
				time: d'2024-07-01T01:00:00Z'
			},
			{
				id: session:6,
				time: d'2024-06-30T23:30:00Z'
			},
			{
				id: session:2,
				time: d'2024-06-30T23:00:00Z'
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
	};
}
use crate::sql::Value;
use crate::syn::Parse;
pub(crate) use define_tests;
