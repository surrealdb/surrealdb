use bytes::{BufMut, BytesMut};

use super::error::PgError;
use super::typing::PgColumn;

/// Maximum accepted startup-packet size, matching Postgres.
pub(super) const MAX_STARTUP_PACKET_SIZE: usize = 10_000;
/// Maximum accepted regular frontend message size.
pub(super) const MAX_MESSAGE_SIZE: usize = 16 << 20;

/// Protocol version 3.0, the only version currently accepted.
pub(super) const PROTOCOL_VERSION_3_0: i32 = 0x0003_0000;
const SSL_REQUEST_CODE: i32 = 80877103;
const GSSENC_REQUEST_CODE: i32 = 80877104;
const CANCEL_REQUEST_CODE: i32 = 80877102;

/// A message received during the startup phase, before the protocol switches
/// to tagged framing.
#[derive(Debug)]
pub(super) enum StartupMessage {
	SslRequest,
	GssEncRequest,
	CancelRequest {
		pid: i32,
		secret: i32,
	},
	Startup {
		version: i32,
		params: Vec<(String, String)>,
	},
}

/// What a Describe/Close message targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DescribeTarget {
	Statement,
	Portal,
}

/// A tagged frontend message.
#[derive(Debug)]
pub(super) enum Frontend {
	// Simple query protocol.
	Query(String),
	Password(String),
	// Extended query protocol.
	Parse {
		name: String,
		query: String,
		param_types: Vec<i32>,
	},
	Bind {
		portal: String,
		statement: String,
		param_formats: Vec<i16>,
		params: Vec<Option<Vec<u8>>>,
		result_formats: Vec<i16>,
	},
	Describe {
		target: DescribeTarget,
		name: String,
	},
	Execute {
		portal: String,
		max_rows: i32,
	},
	Close {
		target: DescribeTarget,
		name: String,
	},
	Sync,
	Flush,
	Terminate,
	Unknown(u8),
}

fn read_cstr(buf: &mut &[u8]) -> Result<String, PgError> {
	let Some(pos) = buf.iter().position(|b| *b == 0) else {
		return Err(PgError::protocol("unterminated string in message"));
	};
	let (raw, rest) = buf.split_at(pos);
	*buf = &rest[1..];
	String::from_utf8(raw.to_vec()).map_err(|_| PgError::protocol("invalid UTF-8 in message"))
}

fn read_i16(buf: &mut &[u8]) -> Result<i16, PgError> {
	if buf.len() < 2 {
		return Err(PgError::protocol("message truncated"));
	}
	let (raw, rest) = buf.split_at(2);
	*buf = rest;
	Ok(i16::from_be_bytes(raw.try_into().expect("split_at(2) yields two bytes")))
}

fn read_i32(buf: &mut &[u8]) -> Result<i32, PgError> {
	if buf.len() < 4 {
		return Err(PgError::protocol("message truncated"));
	}
	let (raw, rest) = buf.split_at(4);
	*buf = rest;
	Ok(i32::from_be_bytes(raw.try_into().expect("split_at(4) yields four bytes")))
}

/// Read an i16-prefixed count, guarded so a bogus negative or huge count can
/// never drive an over-large allocation.
fn read_count(buf: &mut &[u8]) -> Result<usize, PgError> {
	let n = read_i16(buf)?;
	if n < 0 {
		return Err(PgError::protocol("negative count in message"));
	}
	Ok(n as usize)
}

fn describe_target(kind: u8) -> Result<DescribeTarget, PgError> {
	match kind {
		b'S' => Ok(DescribeTarget::Statement),
		b'P' => Ok(DescribeTarget::Portal),
		other => {
			Err(PgError::protocol(format!("invalid describe/close target '{}'", char::from(other))))
		}
	}
}

/// Parse the payload of a startup-phase packet (the length prefix has already
/// been consumed).
pub(super) fn parse_startup(mut payload: &[u8]) -> Result<StartupMessage, PgError> {
	let code = read_i32(&mut payload)?;
	match code {
		SSL_REQUEST_CODE => Ok(StartupMessage::SslRequest),
		GSSENC_REQUEST_CODE => Ok(StartupMessage::GssEncRequest),
		CANCEL_REQUEST_CODE => {
			let pid = read_i32(&mut payload)?;
			let secret = read_i32(&mut payload)?;
			Ok(StartupMessage::CancelRequest {
				pid,
				secret,
			})
		}
		version => {
			let mut params = Vec::new();
			loop {
				let key = read_cstr(&mut payload)?;
				if key.is_empty() {
					break;
				}
				let value = read_cstr(&mut payload)?;
				params.push((key, value));
			}
			Ok(StartupMessage::Startup {
				version,
				params,
			})
		}
	}
}

/// Parse a tagged frontend message body.
pub(super) fn parse_frontend(tag: u8, mut payload: &[u8]) -> Result<Frontend, PgError> {
	let payload = &mut payload;
	match tag {
		b'Q' => Ok(Frontend::Query(read_cstr(payload)?)),
		b'p' => Ok(Frontend::Password(read_cstr(payload)?)),
		b'S' => Ok(Frontend::Sync),
		b'H' => Ok(Frontend::Flush),
		b'X' => Ok(Frontend::Terminate),
		b'P' => {
			let name = read_cstr(payload)?;
			let query = read_cstr(payload)?;
			let count = read_count(payload)?;
			let mut param_types = Vec::with_capacity(count.min(64));
			for _ in 0..count {
				param_types.push(read_i32(payload)?);
			}
			Ok(Frontend::Parse {
				name,
				query,
				param_types,
			})
		}
		b'B' => {
			let portal = read_cstr(payload)?;
			let statement = read_cstr(payload)?;
			let fmt_count = read_count(payload)?;
			let mut param_formats = Vec::with_capacity(fmt_count.min(64));
			for _ in 0..fmt_count {
				param_formats.push(read_i16(payload)?);
			}
			let param_count = read_count(payload)?;
			let mut params = Vec::with_capacity(param_count.min(64));
			for _ in 0..param_count {
				let len = read_i32(payload)?;
				if len < 0 {
					params.push(None);
				} else {
					let len = len as usize;
					if payload.len() < len {
						return Err(PgError::protocol("bind parameter truncated"));
					}
					let (raw, rest) = payload.split_at(len);
					params.push(Some(raw.to_vec()));
					*payload = rest;
				}
			}
			let result_count = read_count(payload)?;
			let mut result_formats = Vec::with_capacity(result_count.min(64));
			for _ in 0..result_count {
				result_formats.push(read_i16(payload)?);
			}
			Ok(Frontend::Bind {
				portal,
				statement,
				param_formats,
				params,
				result_formats,
			})
		}
		b'D' => {
			let target = describe_target(read_u8(payload)?)?;
			Ok(Frontend::Describe {
				target,
				name: read_cstr(payload)?,
			})
		}
		b'E' => {
			let portal = read_cstr(payload)?;
			let max_rows = read_i32(payload)?;
			Ok(Frontend::Execute {
				portal,
				max_rows,
			})
		}
		b'C' => {
			let target = describe_target(read_u8(payload)?)?;
			Ok(Frontend::Close {
				target,
				name: read_cstr(payload)?,
			})
		}
		other => Ok(Frontend::Unknown(other)),
	}
}

fn read_u8(buf: &mut &[u8]) -> Result<u8, PgError> {
	let Some((first, rest)) = buf.split_first() else {
		return Err(PgError::protocol("message truncated"));
	};
	*buf = rest;
	Ok(*first)
}

/// Append a tagged, length-prefixed message built by `body`.
fn frame(buf: &mut BytesMut, tag: u8, body: impl FnOnce(&mut BytesMut)) {
	buf.put_u8(tag);
	let len_at = buf.len();
	buf.put_i32(0);
	body(buf);
	let len = (buf.len() - len_at) as i32;
	buf[len_at..len_at + 4].copy_from_slice(&len.to_be_bytes());
}

fn put_cstr(buf: &mut BytesMut, s: &str) {
	if s.as_bytes().contains(&0) {
		// Wire cstrings cannot carry NUL bytes.
		buf.extend_from_slice(s.replace('\0', "\u{fffd}").as_bytes());
	} else {
		buf.extend_from_slice(s.as_bytes());
	}
	buf.put_u8(0);
}

pub(super) fn write_ssl_response(buf: &mut BytesMut, accept: bool) {
	buf.put_u8(if accept {
		b'S'
	} else {
		b'N'
	});
}

pub(super) fn write_authentication_cleartext_password(buf: &mut BytesMut) {
	frame(buf, b'R', |buf| buf.put_i32(3));
}

/// AuthenticationSASL (R, 10): offer the given SASL mechanisms, terminated by
/// an empty string.
pub(super) fn write_authentication_sasl(buf: &mut BytesMut, mechanisms: &[&str]) {
	frame(buf, b'R', |buf| {
		buf.put_i32(10);
		for mechanism in mechanisms {
			put_cstr(buf, mechanism);
		}
		buf.put_u8(0);
	});
}

/// AuthenticationSASLContinue (R, 11): the server-first message.
pub(super) fn write_authentication_sasl_continue(buf: &mut BytesMut, data: &[u8]) {
	frame(buf, b'R', |buf| {
		buf.put_i32(11);
		buf.extend_from_slice(data);
	});
}

/// AuthenticationSASLFinal (R, 12): the server-final message.
pub(super) fn write_authentication_sasl_final(buf: &mut BytesMut, data: &[u8]) {
	frame(buf, b'R', |buf| {
		buf.put_i32(12);
		buf.extend_from_slice(data);
	});
}

/// Parse a SASLInitialResponse body (frontend tag `p`): a mechanism cstring,
/// an i32 initial-response length (`-1` = absent), then that many bytes.
pub(super) fn parse_sasl_initial(payload: &[u8]) -> Result<(String, Vec<u8>), PgError> {
	let mut buf = payload;
	let mechanism = read_cstr(&mut buf)?;
	let len = read_i32(&mut buf)?;
	let initial = if len < 0 {
		Vec::new()
	} else {
		let len = len as usize;
		if buf.len() < len {
			return Err(PgError::protocol("SASL initial response truncated"));
		}
		buf[..len].to_vec()
	};
	Ok((mechanism, initial))
}

pub(super) fn write_authentication_ok(buf: &mut BytesMut) {
	frame(buf, b'R', |buf| buf.put_i32(0));
}

pub(super) fn write_parameter_status(buf: &mut BytesMut, key: &str, value: &str) {
	frame(buf, b'S', |buf| {
		put_cstr(buf, key);
		put_cstr(buf, value);
	});
}

pub(super) fn write_backend_key_data(buf: &mut BytesMut, pid: i32, secret: i32) {
	frame(buf, b'K', |buf| {
		buf.put_i32(pid);
		buf.put_i32(secret);
	});
}

pub(super) fn write_ready_for_query(buf: &mut BytesMut, status: u8) {
	frame(buf, b'Z', |buf| buf.put_u8(status));
}

pub(super) fn write_parse_complete(buf: &mut BytesMut) {
	frame(buf, b'1', |_| {});
}

pub(super) fn write_bind_complete(buf: &mut BytesMut) {
	frame(buf, b'2', |_| {});
}

pub(super) fn write_close_complete(buf: &mut BytesMut) {
	frame(buf, b'3', |_| {});
}

pub(super) fn write_portal_suspended(buf: &mut BytesMut) {
	frame(buf, b's', |_| {});
}

/// NoData: the reply to Describe when the statement/portal returns no rows
/// (used here for an empty query string).
pub(super) fn write_no_data(buf: &mut BytesMut) {
	frame(buf, b'n', |_| {});
}

/// ParameterDescription: the type OID of each `$n` parameter, in order.
pub(super) fn write_parameter_description(buf: &mut BytesMut, oids: &[i32]) {
	// The parameter count is an i16 on the wire. Callers cap it (see MAX_PARAMS),
	// but clamp here too so an oversized slice can never wrap the count negative
	// and desync framing; write exactly as many OIDs as the count claims.
	let count = oids.len().min(i16::MAX as usize);
	frame(buf, b't', |buf| {
		buf.put_i16(count as i16);
		for oid in &oids[..count] {
			buf.put_i32(*oid);
		}
	});
}

/// The wire caps a result at 65535 columns (the field count is an i16). No
/// SurrealDB result realistically approaches this, but clamp defensively so a
/// pathological object can never wrap the count negative and desync framing.
pub(super) const MAX_COLUMNS: usize = i16::MAX as usize;

/// RowDescription with an explicit wire format per column. `format_of(i)`
/// returns 0 (text) or 1 (binary) for column `i`, matching how its DataRow
/// cells are encoded.
pub(super) fn write_row_description(
	buf: &mut BytesMut,
	columns: &[PgColumn],
	format_of: impl Fn(usize) -> i16,
) {
	frame(buf, b'T', |buf| {
		buf.put_i16(columns.len() as i16);
		for (i, column) in columns.iter().enumerate() {
			put_cstr(buf, &column.name);
			// No backing catalog: table oid and attribute number are zero.
			buf.put_i32(0);
			buf.put_i16(0);
			buf.put_i32(column.ty.oid());
			buf.put_i16(column.ty.typlen());
			// Type modifier: none.
			buf.put_i32(-1);
			buf.put_i16(format_of(i));
		}
	});
}

pub(super) fn write_data_row(buf: &mut BytesMut, cells: &[Option<Vec<u8>>]) {
	frame(buf, b'D', |buf| {
		buf.put_i16(cells.len() as i16);
		for cell in cells {
			match cell {
				Some(bytes) => {
					buf.put_i32(bytes.len() as i32);
					buf.extend_from_slice(bytes);
				}
				None => buf.put_i32(-1),
			}
		}
	});
}

pub(super) fn write_command_complete(buf: &mut BytesMut, tag: &str) {
	frame(buf, b'C', |buf| put_cstr(buf, tag));
}

pub(super) fn write_empty_query_response(buf: &mut BytesMut) {
	frame(buf, b'I', |_| {});
}

pub(super) fn write_error_response(buf: &mut BytesMut, err: &PgError) {
	frame(buf, b'E', |buf| {
		buf.put_u8(b'S');
		put_cstr(buf, err.severity());
		buf.put_u8(b'V');
		put_cstr(buf, err.severity());
		buf.put_u8(b'C');
		put_cstr(buf, err.code);
		buf.put_u8(b'M');
		put_cstr(buf, &err.message);
		buf.put_u8(0);
	});
}

/// NoticeResponse: a non-fatal warning (e.g. `BEGIN` inside a transaction).
/// Same field layout as ErrorResponse but does not interrupt the command.
pub(super) fn write_notice(buf: &mut BytesMut, code: &str, message: &str) {
	frame(buf, b'N', |buf| {
		buf.put_u8(b'S');
		put_cstr(buf, "WARNING");
		buf.put_u8(b'V');
		put_cstr(buf, "WARNING");
		buf.put_u8(b'C');
		put_cstr(buf, code);
		buf.put_u8(b'M');
		put_cstr(buf, message);
		buf.put_u8(0);
	});
}

/// Answer a 3.x (minor > 0) startup request: the newest minor protocol
/// version this server speaks, plus any `_pq_.*` protocol options it does
/// not recognise.
pub(super) fn write_negotiate_protocol_version(buf: &mut BytesMut, unsupported: &[String]) {
	frame(buf, b'v', |buf| {
		buf.put_i32(PROTOCOL_VERSION_3_0);
		buf.put_i32(unsupported.len() as i32);
		for option in unsupported {
			put_cstr(buf, option);
		}
	});
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::pg::typing::PgType;

	fn startup_payload(params: &[(&str, &str)]) -> Vec<u8> {
		let mut payload = PROTOCOL_VERSION_3_0.to_be_bytes().to_vec();
		for (key, value) in params {
			payload.extend_from_slice(key.as_bytes());
			payload.push(0);
			payload.extend_from_slice(value.as_bytes());
			payload.push(0);
		}
		payload.push(0);
		payload
	}

	#[test]
	fn parses_startup_params() {
		let payload = startup_payload(&[("user", "root"), ("database", "ns/db")]);
		match parse_startup(&payload).unwrap() {
			StartupMessage::Startup {
				version,
				params,
			} => {
				assert_eq!(version, PROTOCOL_VERSION_3_0);
				assert_eq!(
					params,
					vec![
						("user".to_string(), "root".to_string()),
						("database".to_string(), "ns/db".to_string()),
					]
				);
			}
			other => panic!("unexpected message: {other:?}"),
		}
	}

	#[test]
	fn parses_ssl_and_cancel_requests() {
		assert!(matches!(
			parse_startup(&SSL_REQUEST_CODE.to_be_bytes()).unwrap(),
			StartupMessage::SslRequest
		));
		let mut payload = CANCEL_REQUEST_CODE.to_be_bytes().to_vec();
		payload.extend_from_slice(&7i32.to_be_bytes());
		payload.extend_from_slice(&42i32.to_be_bytes());
		assert!(matches!(
			parse_startup(&payload).unwrap(),
			StartupMessage::CancelRequest {
				pid: 7,
				secret: 42
			}
		));
	}

	#[test]
	fn rejects_unterminated_startup() {
		let mut payload = PROTOCOL_VERSION_3_0.to_be_bytes().to_vec();
		payload.extend_from_slice(b"user");
		assert!(parse_startup(&payload).is_err());
	}

	#[test]
	fn parses_query_message() {
		let mut payload = b"SELECT 1".to_vec();
		payload.push(0);
		match parse_frontend(b'Q', &payload).unwrap() {
			Frontend::Query(sql) => assert_eq!(sql, "SELECT 1"),
			other => panic!("unexpected message: {other:?}"),
		}
	}

	#[test]
	fn frames_have_correct_lengths() {
		let mut buf = BytesMut::new();
		write_ready_for_query(&mut buf, b'I');
		assert_eq!(&buf[..], &[b'Z', 0, 0, 0, 5, b'I']);

		let mut buf = BytesMut::new();
		write_command_complete(&mut buf, "SELECT 1");
		assert_eq!(buf[0], b'C');
		let len = i32::from_be_bytes(buf[1..5].try_into().unwrap());
		assert_eq!(len as usize, buf.len() - 1);
	}

	#[test]
	fn row_description_encodes_columns() {
		let mut buf = BytesMut::new();
		write_row_description(
			&mut buf,
			&[PgColumn {
				name: "id".to_string(),
				ty: PgType::Int8,
			}],
			|_| 0,
		);
		assert_eq!(buf[0], b'T');
		// Field count.
		assert_eq!(i16::from_be_bytes(buf[5..7].try_into().unwrap()), 1);
		// Name is a cstring directly after the count.
		assert_eq!(&buf[7..10], b"id\0");
		// Type oid follows the table oid (i32) and attribute number (i16).
		assert_eq!(i32::from_be_bytes(buf[16..20].try_into().unwrap()), 20);
	}

	#[test]
	fn data_row_encodes_null_as_negative_length() {
		let mut buf = BytesMut::new();
		write_data_row(&mut buf, &[Some(b"1".to_vec()), None]);
		assert_eq!(buf[0], b'D');
		assert_eq!(i16::from_be_bytes(buf[5..7].try_into().unwrap()), 2);
		assert_eq!(i32::from_be_bytes(buf[7..11].try_into().unwrap()), 1);
		assert_eq!(buf[11], b'1');
		assert_eq!(i32::from_be_bytes(buf[12..16].try_into().unwrap()), -1);
	}

	#[test]
	fn error_response_carries_code_and_message() {
		let mut buf = BytesMut::new();
		write_error_response(&mut buf, &PgError::syntax("boom"));
		let bytes = &buf[..];
		assert_eq!(bytes[0], b'E');
		let body = &bytes[5..];
		assert!(body.windows(6).any(|w| w == b"C42601"));
		assert!(body.windows(5).any(|w| w == b"Mboom"));
	}

	#[test]
	fn cstr_strips_nul_bytes() {
		let mut buf = BytesMut::new();
		put_cstr(&mut buf, "a\0b");
		assert_eq!(buf.iter().filter(|b| **b == 0).count(), 1);
	}
}
