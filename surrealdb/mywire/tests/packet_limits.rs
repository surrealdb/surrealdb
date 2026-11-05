//! Reassembly limits of the MySQL packet reader.

use surrealdb_mywire::codec::{MAX_ALLOWED_PACKET, MAX_PACKET_SIZE, PacketReader, PacketTooLarge};
use tokio::io::{AsyncWriteExt, DuplexStream, duplex};

/// Sends `chunks` full-size continuation packets, then a final packet of `tail`
/// bytes. Stops quietly once the reader has gone away.
async fn feed(mut tx: DuplexStream, chunks: usize, tail: usize) {
	let body = vec![0u8; MAX_PACKET_SIZE];
	for seq in 0..chunks {
		let header = [0xff, 0xff, 0xff, seq as u8];
		if tx.write_all(&header).await.is_err() || tx.write_all(&body).await.is_err() {
			return;
		}
	}
	let header = [(tail & 0xff) as u8, ((tail >> 8) & 0xff) as u8, ((tail >> 16) & 0xff) as u8];
	let _ = tx.write_all(&[header[0], header[1], header[2], chunks as u8]).await;
	let _ = tx.write_all(&vec![0u8; tail]).await;
}

/// Reassembles `chunks` full-size packets and a `tail`-byte final packet.
async fn reassemble(chunks: usize, tail: usize) -> anyhow::Result<(u8, Vec<u8>)> {
	let (tx, rx) = duplex(1 << 20);
	tokio::spawn(feed(tx, chunks, tail));
	PacketReader::new(rx).read_packet().await
}

#[tokio::test]
async fn reassembles_a_payload_of_exactly_the_limit() {
	let chunks = MAX_ALLOWED_PACKET / MAX_PACKET_SIZE;
	let tail = MAX_ALLOWED_PACKET - chunks * MAX_PACKET_SIZE;
	let (seq, payload) = reassemble(chunks, tail).await.expect("exactly the limit is accepted");
	assert_eq!(seq, 0);
	assert_eq!(payload.len(), MAX_ALLOWED_PACKET);
}

#[tokio::test]
async fn rejects_a_payload_one_byte_over_the_limit() {
	let chunks = MAX_ALLOWED_PACKET / MAX_PACKET_SIZE;
	let tail = MAX_ALLOWED_PACKET - chunks * MAX_PACKET_SIZE + 1;
	let err = reassemble(chunks, tail).await.expect_err("one byte over the limit");
	let too_large = err.downcast_ref::<PacketTooLarge>().expect("a PacketTooLarge error");
	assert_eq!(too_large.seq as usize, chunks, "the error names the packet that crossed the limit");
}

#[tokio::test]
async fn rejects_a_payload_far_over_the_limit_before_reading_it() {
	let chunks = MAX_ALLOWED_PACKET / MAX_PACKET_SIZE + 1;
	let err = reassemble(chunks, 0).await.expect_err("over the limit");
	assert!(err.downcast_ref::<PacketTooLarge>().is_some(), "unexpected error: {err}");
}
