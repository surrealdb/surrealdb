//! MySQL client/server protocol constants and flags.

// Capability Flags (32-bit bitmask)
pub const CLIENT_LONG_PASSWORD: u32 = 1;
pub const CLIENT_FOUND_ROWS: u32 = 2;
pub const CLIENT_LONG_FLAG: u32 = 4;
pub const CLIENT_CONNECT_WITH_DB: u32 = 8;
pub const CLIENT_NO_SCHEMA: u32 = 16;
pub const CLIENT_COMPRESS: u32 = 32;
pub const CLIENT_ODBC: u32 = 64;
pub const CLIENT_LOCAL_FILES: u32 = 128;
pub const CLIENT_IGNORE_SPACE: u32 = 256;
pub const CLIENT_PROTOCOL_41: u32 = 512;
pub const CLIENT_INTERACTIVE: u32 = 1024;
pub const CLIENT_SSL: u32 = 2048;
pub const CLIENT_IGNORE_SIGPIPE: u32 = 4096;
pub const CLIENT_TRANSACTIONS: u32 = 8192;
pub const CLIENT_RESERVED: u32 = 16384;
pub const CLIENT_SECURE_CONNECTION: u32 = 32768;
pub const CLIENT_MULTI_STATEMENTS: u32 = 1 << 16;
pub const CLIENT_MULTI_RESULTS: u32 = 1 << 17;
pub const CLIENT_PS_MULTI_RESULTS: u32 = 1 << 18;
pub const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
pub const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
pub const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;

// Default server capabilities advertised in HandshakeV10
pub const DEFAULT_SERVER_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
	| CLIENT_FOUND_ROWS
	| CLIENT_LONG_FLAG
	| CLIENT_CONNECT_WITH_DB
	| CLIENT_PROTOCOL_41
	| CLIENT_TRANSACTIONS
	| CLIENT_SECURE_CONNECTION
	| CLIENT_MULTI_STATEMENTS
	| CLIENT_MULTI_RESULTS
	| CLIENT_PS_MULTI_RESULTS
	| CLIENT_PLUGIN_AUTH
	| CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	| CLIENT_DEPRECATE_EOF;

// Character sets
pub const CHARSET_UTF8MB4_GENERAL_CI: u16 = 45;
pub const CHARSET_UTF8MB4_0900_AI_CI: u16 = 255;

// Status Flags
pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
pub const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;

// Command bytes
pub const COM_SLEEP: u8 = 0x00;
pub const COM_QUIT: u8 = 0x01;
pub const COM_INIT_DB: u8 = 0x02;
pub const COM_QUERY: u8 = 0x03;
pub const COM_FIELD_LIST: u8 = 0x04;
pub const COM_PING: u8 = 0x0e;
pub const COM_RESET_CONNECTION: u8 = 0x1f;

// MySQL Column / Field Types
pub const MYSQL_TYPE_DECIMAL: u8 = 0x00;
pub const MYSQL_TYPE_TINY: u8 = 0x01;
pub const MYSQL_TYPE_SHORT: u8 = 0x02;
pub const MYSQL_TYPE_LONG: u8 = 0x03;
pub const MYSQL_TYPE_FLOAT: u8 = 0x04;
pub const MYSQL_TYPE_DOUBLE: u8 = 0x05;
pub const MYSQL_TYPE_NULL: u8 = 0x06;
pub const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;
pub const MYSQL_TYPE_INT24: u8 = 0x09;
pub const MYSQL_TYPE_DATE: u8 = 0x0a;
pub const MYSQL_TYPE_TIME: u8 = 0x0b;
pub const MYSQL_TYPE_DATETIME: u8 = 0x0c;
pub const MYSQL_TYPE_YEAR: u8 = 0x0d;
pub const MYSQL_TYPE_VARCHAR: u8 = 0x0f;
pub const MYSQL_TYPE_BIT: u8 = 0x10;
pub const MYSQL_TYPE_JSON: u8 = 0xf5;
pub const MYSQL_TYPE_NEWDECIMAL: u8 = 0xf6;
pub const MYSQL_TYPE_ENUM: u8 = 0xf7;
pub const MYSQL_TYPE_SET: u8 = 0xf8;
pub const MYSQL_TYPE_TINY_BLOB: u8 = 0xf9;
pub const MYSQL_TYPE_MEDIUM_BLOB: u8 = 0xfa;
pub const MYSQL_TYPE_LONG_BLOB: u8 = 0xfb;
pub const MYSQL_TYPE_BLOB: u8 = 0xfc;
pub const MYSQL_TYPE_VAR_STRING: u8 = 0xfd;
pub const MYSQL_TYPE_STRING: u8 = 0xfe;
pub const MYSQL_TYPE_GEOMETRY: u8 = 0xff;

// Column flags
pub const NOT_NULL_FLAG: u16 = 0x0001;
pub const PRI_KEY_FLAG: u16 = 0x0002;
pub const UNIQUE_KEY_FLAG: u16 = 0x0004;
pub const MULTIPLE_KEY_FLAG: u16 = 0x0008;
pub const BLOB_FLAG: u16 = 0x0010;
pub const UNSIGNED_FLAG: u16 = 0x0020;
pub const ZEROFILL_FLAG: u16 = 0x0040;
pub const BINARY_FLAG: u16 = 0x0080;
pub const ENUM_FLAG: u16 = 0x0100;
pub const AUTO_INCREMENT_FLAG: u16 = 0x0200;
pub const TIMESTAMP_FLAG: u16 = 0x0400;
pub const SET_FLAG: u16 = 0x0800;

// Header identifiers
pub const OK_HEADER: u8 = 0x00;
pub const EOF_HEADER: u8 = 0xfe;
pub const ERR_HEADER: u8 = 0xff;
pub const NULL_VALUE: u8 = 0xfb;
