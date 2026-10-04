//! Protocol packet serialization and deserialization.

use anyhow::{Result, ensure};

use crate::codec::{
	read_lenenc_bytes, read_null_terminated_bytes, read_null_terminated_str, write_lenenc_int,
	write_lenenc_str, write_null_terminated_str,
};
use crate::constants::*;

/// Protocol::HandshakeV10 initial server handshake packet.
#[derive(Debug, Clone)]
pub struct HandshakeV10 {
	pub server_version: String,
	pub connection_id: u32,
	pub auth_plugin_data: [u8; 20],
	pub capabilities: u32,
	pub character_set: u8,
	pub status_flags: u16,
	pub auth_plugin_name: String,
}

impl HandshakeV10 {
	pub fn serialize(&self) -> Vec<u8> {
		let mut buf = Vec::with_capacity(128);
		// Protocol version (10)
		buf.push(10);
		// Server version (null-terminated)
		write_null_terminated_str(&mut buf, &self.server_version);
		// Connection id (4 bytes little-endian)
		buf.extend_from_slice(&self.connection_id.to_le_bytes());
		// Auth plugin data part 1 (first 8 bytes)
		buf.extend_from_slice(&self.auth_plugin_data[..8]);
		// Filler (1 byte)
		buf.push(0x00);
		// Capability flags lower 2 bytes
		buf.extend_from_slice(&(self.capabilities as u16).to_le_bytes());
		// Character set (1 byte)
		buf.push(self.character_set);
		// Status flags (2 bytes)
		buf.extend_from_slice(&self.status_flags.to_le_bytes());
		// Capability flags upper 2 bytes
		buf.extend_from_slice(&((self.capabilities >> 16) as u16).to_le_bytes());

		// Length of auth-plugin-data (21 for 20 bytes + null)
		if (self.capabilities & CLIENT_PLUGIN_AUTH) != 0 {
			buf.push(21);
		} else {
			buf.push(0);
		}

		// Reserved 10 bytes
		buf.extend_from_slice(&[0u8; 10]);

		// Auth plugin data part 2 (remaining 12 bytes + null byte)
		if (self.capabilities & CLIENT_SECURE_CONNECTION) != 0 {
			buf.extend_from_slice(&self.auth_plugin_data[8..20]);
			buf.push(0x00);
		}

		// Auth plugin name (null-terminated)
		if (self.capabilities & CLIENT_PLUGIN_AUTH) != 0 {
			write_null_terminated_str(&mut buf, &self.auth_plugin_name);
		}

		buf
	}
}

/// Protocol::HandshakeResponse41 client handshake response.
#[derive(Debug, Clone)]
pub struct HandshakeResponse41 {
	pub capabilities: u32,
	pub max_packet_size: u32,
	pub character_set: u8,
	pub username: String,
	pub auth_response: Vec<u8>,
	pub database: Option<String>,
	pub auth_plugin_name: Option<String>,
}

impl HandshakeResponse41 {
	pub fn deserialize(payload: &[u8]) -> Result<Self> {
		let mut cursor = payload;
		ensure!(cursor.len() >= 32, "handshake response payload too short");

		// Capability flags (4 bytes)
		let capabilities = u32::from_le_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
		cursor = &cursor[4..];

		ensure!((capabilities & CLIENT_PROTOCOL_41) != 0, "client must support Protocol 41");

		// Max packet size (4 bytes)
		let max_packet_size = u32::from_le_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
		cursor = &cursor[4..];

		// Character set (1 byte)
		let character_set = cursor[0];
		cursor = &cursor[1..];

		// Reserved 23 bytes
		ensure!(cursor.len() >= 23, "malformed handshake response reserved area");
		cursor = &cursor[23..];

		// Username (null-terminated string)
		let username =
			read_null_terminated_str(&mut cursor).map(|s| s.to_string()).unwrap_or_default();

		// Auth response
		let auth_response = if (capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0 {
			read_lenenc_bytes(&mut cursor).unwrap_or(&[]).to_vec()
		} else if (capabilities & CLIENT_SECURE_CONNECTION) != 0 {
			if !cursor.is_empty() {
				let len = cursor[0] as usize;
				cursor = &cursor[1..];
				if cursor.len() >= len {
					let data = cursor[..len].to_vec();
					cursor = &cursor[len..];
					data
				} else {
					Vec::new()
				}
			} else {
				Vec::new()
			}
		} else {
			read_null_terminated_bytes(&mut cursor).unwrap_or(&[]).to_vec()
		};

		// Database name (if CLIENT_CONNECT_WITH_DB)
		let database = if (capabilities & CLIENT_CONNECT_WITH_DB) != 0 {
			read_null_terminated_str(&mut cursor).map(|s| s.to_string())
		} else {
			None
		};

		// Auth plugin name (if CLIENT_PLUGIN_AUTH)
		let auth_plugin_name = if (capabilities & CLIENT_PLUGIN_AUTH) != 0 {
			read_null_terminated_str(&mut cursor).map(|s| s.to_string())
		} else {
			None
		};

		Ok(Self {
			capabilities,
			max_packet_size,
			character_set,
			username,
			auth_response,
			database,
			auth_plugin_name,
		})
	}
}

/// Protocol::SSLRequest packet sent by client to initiate TLS.
#[derive(Debug, Clone)]
pub struct SslRequest {
	pub capabilities: u32,
	pub max_packet_size: u32,
	pub character_set: u8,
}

impl SslRequest {
	pub fn is_ssl_request(payload: &[u8]) -> bool {
		if payload.len() < 32 {
			return false;
		}
		let caps = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
		(caps & CLIENT_SSL) != 0
	}

	pub fn deserialize(payload: &[u8]) -> Result<Self> {
		ensure!(payload.len() >= 32, "SSL request payload too short");
		let capabilities = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
		ensure!((capabilities & CLIENT_SSL) != 0, "CLIENT_SSL must be set in SSLRequest");
		let max_packet_size = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
		let character_set = payload[8];
		Ok(Self {
			capabilities,
			max_packet_size,
			character_set,
		})
	}
}

/// Protocol::AuthSwitchRequest packet sent by server to change auth method.
#[derive(Debug, Clone)]
pub struct AuthSwitchRequest {
	pub plugin_name: String,
	pub auth_plugin_data: Vec<u8>,
}

impl AuthSwitchRequest {
	pub fn clear_password() -> Self {
		Self {
			plugin_name: "mysql_clear_password".to_string(),
			auth_plugin_data: Vec::new(),
		}
	}

	pub fn serialize(&self) -> Vec<u8> {
		let mut buf = Vec::with_capacity(32 + self.auth_plugin_data.len());
		buf.push(0xfe); // AuthSwitchRequest header
		write_null_terminated_str(&mut buf, &self.plugin_name);
		buf.extend_from_slice(&self.auth_plugin_data);
		buf
	}
}

/// OK_Packet sent on command success.
#[derive(Debug, Clone)]
pub struct OkPacket {
	pub affected_rows: u64,
	pub last_insert_id: u64,
	pub status_flags: u16,
	pub warnings: u16,
	pub info: String,
}

impl OkPacket {
	pub fn new(affected_rows: u64, last_insert_id: u64) -> Self {
		Self {
			affected_rows,
			last_insert_id,
			status_flags: SERVER_STATUS_AUTOCOMMIT,
			warnings: 0,
			info: String::new(),
		}
	}

	pub fn serialize(&self) -> Vec<u8> {
		let mut buf = Vec::with_capacity(32);
		buf.push(OK_HEADER);
		write_lenenc_int(&mut buf, self.affected_rows);
		write_lenenc_int(&mut buf, self.last_insert_id);
		buf.extend_from_slice(&self.status_flags.to_le_bytes());
		buf.extend_from_slice(&self.warnings.to_le_bytes());
		if !self.info.is_empty() {
			write_lenenc_str(&mut buf, &self.info);
		}
		buf
	}
}

/// ERR_Packet sent on command or authentication failure.
#[derive(Debug, Clone)]
pub struct ErrPacket {
	pub error_code: u16,
	pub sql_state: [u8; 5],
	pub error_message: String,
}

impl ErrPacket {
	pub fn syntax(msg: impl Into<String>) -> Self {
		Self {
			error_code: 1064, // ER_PARSE_ERROR
			sql_state: *b"42000",
			error_message: msg.into(),
		}
	}

	pub fn access_denied(msg: impl Into<String>) -> Self {
		Self {
			error_code: 1045, // ER_ACCESS_DENIED_ERROR
			sql_state: *b"28000",
			error_message: msg.into(),
		}
	}

	pub fn bad_db(msg: impl Into<String>) -> Self {
		Self {
			error_code: 1049, // ER_BAD_DB_ERROR
			sql_state: *b"42000",
			error_message: msg.into(),
		}
	}

	pub fn general(code: u16, sql_state: &[u8; 5], msg: impl Into<String>) -> Self {
		Self {
			error_code: code,
			sql_state: *sql_state,
			error_message: msg.into(),
		}
	}

	pub fn serialize(&self) -> Vec<u8> {
		let mut buf = Vec::with_capacity(64 + self.error_message.len());
		buf.push(ERR_HEADER);
		buf.extend_from_slice(&self.error_code.to_le_bytes());
		buf.push(b'#'); // SQL State marker
		buf.extend_from_slice(&self.sql_state);
		buf.extend_from_slice(self.error_message.as_bytes());
		buf
	}
}

/// EOF_Packet terminating column definitions or rows.
#[derive(Debug, Clone)]
pub struct EofPacket {
	pub warnings: u16,
	pub status_flags: u16,
}

impl Default for EofPacket {
	fn default() -> Self {
		Self {
			warnings: 0,
			status_flags: SERVER_STATUS_AUTOCOMMIT,
		}
	}
}

impl EofPacket {
	pub fn serialize(&self) -> Vec<u8> {
		let mut buf = Vec::with_capacity(5);
		buf.push(EOF_HEADER);
		buf.extend_from_slice(&self.warnings.to_le_bytes());
		buf.extend_from_slice(&self.status_flags.to_le_bytes());
		buf
	}
}

/// Protocol::ColumnDefinition41 describing a result column.
#[derive(Debug, Clone)]
pub struct ColumnDefinition41 {
	pub catalog: String,
	pub schema: String,
	pub table: String,
	pub org_table: String,
	pub name: String,
	pub org_name: String,
	pub character_set: u16,
	pub column_length: u32,
	pub column_type: u8,
	pub flags: u16,
	pub decimals: u8,
}

impl ColumnDefinition41 {
	pub fn new(schema: &str, table: &str, name: &str, column_type: u8) -> Self {
		Self {
			catalog: "def".to_string(),
			schema: schema.to_string(),
			table: table.to_string(),
			org_table: table.to_string(),
			name: name.to_string(),
			org_name: name.to_string(),
			character_set: CHARSET_UTF8MB4_GENERAL_CI,
			column_length: 255,
			column_type,
			flags: 0,
			decimals: 0,
		}
	}

	pub fn serialize(&self) -> Vec<u8> {
		let mut buf = Vec::with_capacity(128);
		write_lenenc_str(&mut buf, &self.catalog);
		write_lenenc_str(&mut buf, &self.schema);
		write_lenenc_str(&mut buf, &self.table);
		write_lenenc_str(&mut buf, &self.org_table);
		write_lenenc_str(&mut buf, &self.name);
		write_lenenc_str(&mut buf, &self.org_name);

		// Fixed length fields length (always 0x0c)
		buf.push(0x0c);
		// Character set (2 bytes)
		buf.extend_from_slice(&self.character_set.to_le_bytes());
		// Column length (4 bytes)
		buf.extend_from_slice(&self.column_length.to_le_bytes());
		// Column type (1 byte)
		buf.push(self.column_type);
		// Flags (2 bytes)
		buf.extend_from_slice(&self.flags.to_le_bytes());
		// Decimals (1 byte)
		buf.push(self.decimals);
		// Reserved (2 bytes)
		buf.extend_from_slice(&[0u8, 0u8]);

		buf
	}
}
