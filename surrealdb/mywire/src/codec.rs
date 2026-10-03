//! Packet encoder and decoder for the MySQL wire protocol.

use anyhow::{Result, bail, ensure};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_PACKET_SIZE: usize = 16_777_215; // 2^24 - 1

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

	/// Read a single MySQL packet, returning (sequence_id, payload).
	pub async fn read_packet(&mut self) -> Result<(u8, Vec<u8>)> {
		let mut header = [0u8; 4];
		self.reader.read_exact(&mut header).await?;

		let payload_len =
			(header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
		let seq = header[3];

		ensure!(payload_len <= MAX_PACKET_SIZE, "packet length exceeds maximum allowed");

		let mut payload = vec![0u8; payload_len];
		self.reader.read_exact(&mut payload).await?;

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

	/// Write a single MySQL packet with the given sequence ID and payload.
	pub async fn write_packet(&mut self, seq: u8, payload: &[u8]) -> Result<()> {
		let len = payload.len();
		if len > MAX_PACKET_SIZE {
			bail!("packet size {} exceeds maximum supported size {}", len, MAX_PACKET_SIZE);
		}

		let header =
			[(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, ((len >> 16) & 0xff) as u8, seq];

		self.writer.write_all(&header).await?;
		self.writer.write_all(payload).await?;
		self.writer.flush().await?;
		Ok(())
	}
}
