use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use bytes::{BufMut, BytesMut};
use surrealdb_core::kvs::Datastore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

async fn start_test_pg_server() -> (std::net::SocketAddr, CancellationToken) {
	let ds = Datastore::new("memory").await.expect("Failed to create datastore");
	let ready = Arc::new(AtomicBool::new(true));
	let shutdown = CancellationToken::new();

	let bind_addr: std::net::SocketAddr =
		"127.0.0.1:0".parse().expect("Failed to parse socket address");
	let addr = surrealdb_pgwire::start(bind_addr, ds, ready, shutdown.clone(), None, None)
		.await
		.expect("Failed to start pgwire server");

	(addr, shutdown)
}

async fn connect_and_handshake(addr: std::net::SocketAddr) -> TcpStream {
	let mut stream = TcpStream::connect(addr).await.expect("Failed to connect to pgwire server");

	// Send StartupMessage (Protocol 3.0)
	let mut body = BytesMut::new();
	body.put_i32(196608); // Protocol 3.0 (3 << 16)
	body.put_slice(b"user\0root\0");
	body.put_slice(b"database\0test/test\0");
	body.put_u8(0);

	let len = (body.len() + 4) as i32;
	let mut packet = BytesMut::new();
	packet.put_i32(len);
	packet.put(body);

	stream.write_all(&packet).await.expect("Failed to send startup message");

	// Drain messages until ReadyForQuery ('Z')
	let mut ready_for_query = false;
	let res = timeout(Duration::from_secs(5), async {
		while !ready_for_query {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			if msg_type == b'Z' {
				ready_for_query = true;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for ReadyForQuery");
	stream
}

async fn run_simple_query(stream: &mut TcpStream, sql: &str) -> (Vec<Vec<u8>>, u8) {
	let mut query = sql.to_string();
	query.push('\0');

	let mut q_pkt = BytesMut::new();
	q_pkt.put_u8(b'Q');
	q_pkt.put_i32((query.len() + 4) as i32);
	q_pkt.put_slice(query.as_bytes());

	stream.write_all(&q_pkt).await.expect("Failed to send query");

	let mut rows = Vec::new();
	let mut last_type = 0u8;

	let res = timeout(Duration::from_secs(5), async {
		loop {
			let msg_type = stream.read_u8().await?;
			let msg_len = stream.read_i32().await? as usize;
			let mut payload = vec![0u8; msg_len - 4];
			stream.read_exact(&mut payload).await?;

			last_type = msg_type;
			if msg_type == b'D' {
				rows.push(payload);
			} else if msg_type == b'Z' {
				break;
			}
		}
		Ok::<_, std::io::Error>(())
	})
	.await;

	assert!(res.is_ok(), "Timed out waiting for query completion: {sql}");
	(rows, last_type)
}

#[tokio::test]
async fn test_pgwire_e2e_simple_query_lifecycle() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// 1. SELECT 1 AS num
	let (rows, _) = run_simple_query(&mut stream, "SELECT 1 AS num;").await;
	assert_eq!(rows.len(), 1, "Expected 1 row from SELECT 1");

	// 2. CREATE records
	let _ = run_simple_query(&mut stream, "CREATE person SET name = 'Tobie';").await;
	let _ = run_simple_query(&mut stream, "CREATE person SET name = 'Jaime';").await;

	// 3. SELECT from table
	let (rows, _) = run_simple_query(&mut stream, "SELECT name FROM person;").await;
	assert_eq!(rows.len(), 2, "Expected 2 rows from person table");

	// 4. Terminate cleanly
	let mut term_pkt = BytesMut::new();
	term_pkt.put_u8(b'X');
	term_pkt.put_i32(4);
	stream.write_all(&term_pkt).await.expect("Failed to send terminate");

	let mut buf = [0u8; 1];
	let n = stream.read(&mut buf).await.unwrap_or_default();
	assert_eq!(n, 0, "Connection should be closed after Terminate");

	shutdown.cancel();
}

#[tokio::test]
async fn test_pgwire_e2e_transactions() {
	let (addr, shutdown) = start_test_pg_server().await;
	let mut stream = connect_and_handshake(addr).await;

	// Begin transaction
	let _ = run_simple_query(&mut stream, "BEGIN;").await;
	let _ = run_simple_query(&mut stream, "CREATE item SET title = 'To Rollback';").await;
	let _ = run_simple_query(&mut stream, "ROLLBACK;").await;

	// Verify rollback
	let (rows, _) = run_simple_query(&mut stream, "SELECT * FROM item;").await;
	assert_eq!(rows.len(), 0, "Item should have been rolled back");

	shutdown.cancel();
}
