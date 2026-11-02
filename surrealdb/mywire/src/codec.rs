//! Packet encoder and decoder for the MySQL wire protocol.

pub mod bit;
pub mod year;

use anyhow::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub mod geometry;

pub const MAX_PACKET_SIZE: usize = 16_777_215; // 2^24 - 1

/// The largest packet payload accepted once continuation packets are
/// reassembled, in bytes. It is the value the server reports as
/// `@@max_allowed_packet`.
pub const MAX_ALLOWED_PACKET: usize = 64 * 1024 * 1024;

/// Maximum packet payload accepted during the pre-authentication handshake.
pub const MAX_HANDSHAKE_PACKET_SIZE: usize = 64 * 1024;

/// A peer sent a packet larger than [`MAX_ALLOWED_PACKET`]. `seq` is the sequence
/// id of the continuation packet that crossed the limit; the error reply
/// follows it.
#[derive(Debug)]
pub struct PacketTooLarge {
	pub seq: u8,
}

impl std::fmt::Display for PacketTooLarge {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "packet exceeds max_allowed_packet ({MAX_ALLOWED_PACKET} bytes)")
	}
}

impl std::error::Error for PacketTooLarge {}

/// Read a MySQL length-encoded integer from a slice cursor.
pub fn read_lenenc_int(cursor: &mut &[u8]) -> Option<u64> {
	if cursor.is_empty() {
		return None;
	}
	let first = cursor[0];
	*cursor = &cursor[1..];
	match first {
		0..=250 => Some(first as u64),
		0xfb => None, // NULL value
		0xfc => {
			if cursor.len() < 2 {
				return None;
			}
			let val = u16::from_le_bytes([cursor[0], cursor[1]]) as u64;
			*cursor = &cursor[2..];
			Some(val)
		}
		0xfd => {
			if cursor.len() < 3 {
				return None;
			}
			let val = (cursor[0] as u64) | ((cursor[1] as u64) << 8) | ((cursor[2] as u64) << 16);
			*cursor = &cursor[3..];
			Some(val)
		}
		0xfe => {
			if cursor.len() < 8 {
				return None;
			}
			let mut bytes = [0u8; 8];
			bytes.copy_from_slice(&cursor[..8]);
			let val = u64::from_le_bytes(bytes);
			*cursor = &cursor[8..];
			Some(val)
		}
		0xff => None, // Error header
	}
}

/// Write a MySQL length-encoded integer to a buffer.
pub fn write_lenenc_int(buf: &mut Vec<u8>, val: u64) {
	if val < 251 {
		buf.push(val as u8);
	} else if val <= 0xffff {
		buf.push(0xfc);
		buf.extend_from_slice(&(val as u16).to_le_bytes());
	} else if val <= 0xffffff {
		buf.push(0xfd);
		let b0 = (val & 0xff) as u8;
		let b1 = ((val >> 8) & 0xff) as u8;
		let b2 = ((val >> 16) & 0xff) as u8;
		buf.extend_from_slice(&[b0, b1, b2]);
	} else {
		buf.push(0xfe);
		buf.extend_from_slice(&val.to_le_bytes());
	}
}

/// Read a length-encoded string from a slice cursor.
pub fn read_lenenc_str<'a>(cursor: &mut &'a [u8]) -> Option<&'a str> {
	let len = read_lenenc_int(cursor)? as usize;
	if cursor.len() < len {
		return None;
	}
	let bytes = &cursor[..len];
	*cursor = &cursor[len..];
	std::str::from_utf8(bytes).ok()
}

/// Read length-encoded raw bytes from a slice cursor.
pub fn read_lenenc_bytes<'a>(cursor: &mut &'a [u8]) -> Option<&'a [u8]> {
	let len = read_lenenc_int(cursor)? as usize;
	if cursor.len() < len {
		return None;
	}
	let bytes = &cursor[..len];
	*cursor = &cursor[len..];
	Some(bytes)
}

/// Write a length-encoded string to a buffer.
pub fn write_lenenc_str(buf: &mut Vec<u8>, s: &str) {
	write_lenenc_int(buf, s.len() as u64);
	buf.extend_from_slice(s.as_bytes());
}

/// Write length-encoded raw bytes to a buffer.
pub fn write_lenenc_bytes(buf: &mut Vec<u8>, b: &[u8]) {
	write_lenenc_int(buf, b.len() as u64);
	buf.extend_from_slice(b);
}

/// Read a null-terminated string from a slice cursor.
pub fn read_null_terminated_str<'a>(cursor: &mut &'a [u8]) -> Option<&'a str> {
	let null_pos = cursor.iter().position(|&b| b == 0)?;
	let bytes = &cursor[..null_pos];
	*cursor = &cursor[null_pos + 1..];
	std::str::from_utf8(bytes).ok()
}

/// Read null-terminated bytes from a slice cursor.
pub fn read_null_terminated_bytes<'a>(cursor: &mut &'a [u8]) -> Option<&'a [u8]> {
	let null_pos = cursor.iter().position(|&b| b == 0)?;
	let bytes = &cursor[..null_pos];
	*cursor = &cursor[null_pos + 1..];
	Some(bytes)
}

/// Write a null-terminated string to a buffer.
pub fn write_null_terminated_str(buf: &mut Vec<u8>, s: &str) {
	buf.extend_from_slice(s.as_bytes());
	buf.push(0x00);
}

/// Async reader for MySQL wire packets.
pub struct PacketReader<R> {
	reader: R,
}

impl<R: AsyncRead + Unpin> PacketReader<R> {
	pub fn new(reader: R) -> Self {
		Self {
			reader,
		}
	}

	/// Read a MySQL packet, reassembling multi-packet sequences when payloads
	/// reach or exceed MAX_PACKET_SIZE (16,777,215 bytes).
	/// Returns (sequence_id, payload) where sequence_id is the ID of the first packet.
	/// A reassembled payload larger than [`MAX_ALLOWED_PACKET`] fails with [`PacketTooLarge`].
	pub async fn read_packet(&mut self) -> Result<(u8, Vec<u8>)> {
		self.read_packet_with_limit(MAX_ALLOWED_PACKET).await
	}

	/// Read a pre-authentication handshake packet, rejecting any payload larger than
	/// [`MAX_HANDSHAKE_PACKET_SIZE`] before allocating.
	pub async fn read_handshake_packet(&mut self) -> Result<(u8, Vec<u8>)> {
		self.read_packet_with_limit(MAX_HANDSHAKE_PACKET_SIZE).await
	}

	/// Read a MySQL packet whose total payload must not exceed `max_allowed` bytes.
	pub async fn read_packet_with_limit(&mut self, max_allowed: usize) -> Result<(u8, Vec<u8>)> {
		let mut header = [0u8; 4];
		self.reader.read_exact(&mut header).await?;

		let payload_len =
			(header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
		let seq = header[3];

		if payload_len > max_allowed {
			return Err(PacketTooLarge {
				seq,
			}
			.into());
		}

		let mut payload = vec![0u8; payload_len];
		self.reader.read_exact(&mut payload).await?;

		if payload_len < MAX_PACKET_SIZE {
			return Ok((seq, payload));
		}

		// Continuation: when payload_len == MAX_PACKET_SIZE, read following chunks until chunk <
		// MAX_PACKET_SIZE
		loop {
			let mut next_header = [0u8; 4];
			self.reader.read_exact(&mut next_header).await?;
			let next_len = (next_header[0] as usize)
				| ((next_header[1] as usize) << 8)
				| ((next_header[2] as usize) << 16);
			let start = payload.len();
			if start.saturating_add(next_len) > max_allowed {
				return Err(PacketTooLarge {
					seq: next_header[3],
				}
				.into());
			}
			payload.resize(start + next_len, 0);
			self.reader.read_exact(&mut payload[start..]).await?;
			if next_len < MAX_PACKET_SIZE {
				break;
			}
		}

		Ok((seq, payload))
	}
}

/// Async writer for MySQL wire packets.
pub struct PacketWriter<W> {
	writer: W,
}

impl<W: AsyncWrite + Unpin> PacketWriter<W> {
	pub fn new(writer: W) -> Self {
		Self {
			writer,
		}
	}

	/// Write a MySQL packet with the given sequence ID and payload.
	/// If the payload exceeds MAX_PACKET_SIZE (16,777,215 bytes), it is automatically
	/// fragmented across multiple packets as mandated by the MySQL protocol.
	pub async fn write_packet(&mut self, mut seq: u8, payload: &[u8]) -> Result<()> {
		if payload.len() < MAX_PACKET_SIZE {
			let len = payload.len();
			let header =
				[(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];

			// Buffer small-to-medium packets to avoid split TCP packets
			if payload.len() <= 16384 {
				let mut buf = Vec::with_capacity(4 + payload.len());
				buf.extend_from_slice(&header);
				buf.extend_from_slice(payload);
				self.writer.write_all(&buf).await?;
			} else {
				self.writer.write_all(&header).await?;
				self.writer.write_all(payload).await?;
			}
			self.writer.flush().await?;
			return Ok(());
		}

		// Payload >= MAX_PACKET_SIZE: split into MAX_PACKET_SIZE chunks
		let mut offset = 0;
		while offset < payload.len() {
			let chunk_len = (payload.len() - offset).min(MAX_PACKET_SIZE);
			let header = [
				(chunk_len & 0xff) as u8,
				((chunk_len >> 8) & 0xff) as u8,
				((chunk_len >> 16) & 0xff) as u8,
				seq,
			];
			self.writer.write_all(&header).await?;
			self.writer.write_all(&payload[offset..offset + chunk_len]).await?;
			offset += chunk_len;
			seq = seq.wrapping_add(1);
		}

		// If payload length is an exact multiple of MAX_PACKET_SIZE, an empty terminating packet is
		// required
		if payload.len().is_multiple_of(MAX_PACKET_SIZE) {
			let header = [0u8, 0u8, 0u8, seq];
			self.writer.write_all(&header).await?;
		}

		self.writer.flush().await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_lenenc_int_roundtrip() {
		let test_values =
			[0u64, 1, 250, 251, 252, 0xffff, 0x10000, 0xffffff, 0x1000000, 0x123456789abcdef0];
		for &val in &test_values {
			let mut buf = Vec::new();
			write_lenenc_int(&mut buf, val);
			let mut cursor = &buf[..];
			let read = read_lenenc_int(&mut cursor).expect("failed to read lenenc int");
			assert_eq!(read, val);
			assert!(cursor.is_empty());
		}
	}

	#[test]
	fn test_lenenc_str_roundtrip() {
		let strings = ["", "hello", "mysql protocol", "🦀 Rust & SurrealDB 🚀"];
		for s in strings {
			let mut buf = Vec::new();
			write_lenenc_str(&mut buf, s);
			let mut cursor = &buf[..];
			let read = read_lenenc_str(&mut cursor).expect("failed to read lenenc str");
			assert_eq!(read, s);
			assert!(cursor.is_empty());
		}
	}

	#[test]
	fn test_null_terminated_str_roundtrip() {
		let strings = ["", "utf8mb4_0900_ai_ci", "caching_sha2_password"];
		for s in strings {
			let mut buf = Vec::new();
			write_null_terminated_str(&mut buf, s);
			let mut cursor = &buf[..];
			let read = read_null_terminated_str(&mut cursor).expect("failed to read null str");
			assert_eq!(read, s);
			assert!(cursor.is_empty());
		}
	}

	use tokio::io::duplex;

	#[tokio::test]
	async fn test_packet_writer_and_reader_roundtrip() {
		let (client, server) = duplex(4096);
		let mut writer = PacketWriter::new(client);
		let mut reader = PacketReader::new(server);

		let payload = b"SELECT 1 + 1;";
		writer.write_packet(42, payload).await.unwrap();

		let (seq, read_payload) = reader.read_packet().await.unwrap();
		assert_eq!(seq, 42);
		assert_eq!(&read_payload, payload);
	}

	#[tokio::test]
	async fn read_handshake_packet_rejects_oversized_header_before_reading_body() {
		let oversized_len = MAX_HANDSHAKE_PACKET_SIZE + 1;
		let header = [
			(oversized_len & 0xff) as u8,
			((oversized_len >> 8) & 0xff) as u8,
			((oversized_len >> 16) & 0xff) as u8,
			1,
		];
		let mut reader = PacketReader::new(&header[..]);
		let err = reader
			.read_handshake_packet()
			.await
			.expect_err("oversized handshake packet must be rejected");
		let too_large =
			err.downcast_ref::<PacketTooLarge>().expect("error must downcast to PacketTooLarge");
		assert_eq!(too_large.seq, 1);
	}
}
