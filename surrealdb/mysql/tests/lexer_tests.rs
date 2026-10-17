//! Lexer tests for surrealdb-mysql.

use surrealdb_mysql::lexer::keyword::Keyword;
use surrealdb_mysql::lexer::token::TokenKind;
use surrealdb_mysql::lexer::tokenize;

#[test]
fn test_keywords_and_case_insensitivity() {
	let tokens = tokenize("SELECT select SeLeCt FROM from WHERE where").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Select));
	assert_eq!(tokens[1].kind, TokenKind::Keyword(Keyword::Select));
	assert_eq!(tokens[2].kind, TokenKind::Keyword(Keyword::Select));
	assert_eq!(tokens[3].kind, TokenKind::Keyword(Keyword::From));
	assert_eq!(tokens[4].kind, TokenKind::Keyword(Keyword::From));
	assert_eq!(tokens[5].kind, TokenKind::Keyword(Keyword::Where));
	assert_eq!(tokens[6].kind, TokenKind::Keyword(Keyword::Where));
}

#[test]
fn test_backtick_identifiers() {
	let tokens = tokenize("`users` `user``name` `table.with.dots`").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::Ident("users".to_string()));
	assert_eq!(tokens[1].kind, TokenKind::Ident("user`name".to_string()));
	assert_eq!(tokens[2].kind, TokenKind::Ident("table.with.dots".to_string()));
}

#[test]
fn test_string_literals() {
	let tokens =
		tokenize("'hello world' \"double quoted\" 'escaped \\' quote' 'doubled '' quote'").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::StringLiteral("hello world".to_string()));
	assert_eq!(tokens[1].kind, TokenKind::StringLiteral("double quoted".to_string()));
	assert_eq!(tokens[2].kind, TokenKind::StringLiteral("escaped ' quote".to_string()));
	assert_eq!(tokens[3].kind, TokenKind::StringLiteral("doubled ' quote".to_string()));
}

#[test]
fn test_numbers() {
	let tokens = tokenize("42 3.25 0.5 .75 1e5 2.5e-3").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::IntLiteral(42));
	assert_eq!(tokens[1].kind, TokenKind::FloatLiteral(3.25));
	assert_eq!(tokens[2].kind, TokenKind::FloatLiteral(0.5));
	assert_eq!(tokens[3].kind, TokenKind::FloatLiteral(0.75));
	assert_eq!(tokens[4].kind, TokenKind::FloatLiteral(100000.0));
	assert_eq!(tokens[5].kind, TokenKind::FloatLiteral(0.0025));
}

#[test]
fn test_hex_and_bit() {
	let tokens = tokenize("0x12AF x'12af' 0b101 b'101'").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::HexLiteral(vec![0x12, 0xAF]));
	assert_eq!(tokens[1].kind, TokenKind::HexLiteral(vec![0x12, 0xAF]));
	assert_eq!(tokens[2].kind, TokenKind::BitLiteral(vec![1, 0, 1]));
	assert_eq!(tokens[3].kind, TokenKind::BitLiteral(vec![1, 0, 1]));
}

#[test]
fn test_variables() {
	let tokens = tokenize("@user_var @@system_var @@global.system_var @@session.timeout").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::UserVar("user_var".to_string()));
	assert_eq!(
		tokens[1].kind,
		TokenKind::SystemVar {
			name: "system_var".to_string(),
			is_global: false
		}
	);
	assert_eq!(
		tokens[2].kind,
		TokenKind::SystemVar {
			name: "system_var".to_string(),
			is_global: true
		}
	);
	assert_eq!(
		tokens[3].kind,
		TokenKind::SystemVar {
			name: "timeout".to_string(),
			is_global: false
		}
	);
}

#[test]
fn test_operators() {
	let tokens = tokenize(":= <=> != <> <= >= << >> && ||").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::ColonEqual);
	assert_eq!(tokens[1].kind, TokenKind::NullSafeEqual);
	assert_eq!(tokens[2].kind, TokenKind::NotEqual);
	assert_eq!(tokens[3].kind, TokenKind::NotEqual);
	assert_eq!(tokens[4].kind, TokenKind::LessEqual);
	assert_eq!(tokens[5].kind, TokenKind::GreaterEqual);
	assert_eq!(tokens[6].kind, TokenKind::ShiftLeft);
	assert_eq!(tokens[7].kind, TokenKind::ShiftRight);
	assert_eq!(tokens[8].kind, TokenKind::DoubleAmpersand);
	assert_eq!(tokens[9].kind, TokenKind::DoublePipe);
}

#[test]
fn test_comments() {
	let tokens =
		tokenize("SELECT # this is a comment\n1 -- another comment\n/* block comment */ + 2")
			.unwrap();
	assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Select));
	assert_eq!(tokens[1].kind, TokenKind::IntLiteral(1));
	assert_eq!(tokens[2].kind, TokenKind::Plus);
	assert_eq!(tokens[3].kind, TokenKind::IntLiteral(2));
}

#[test]
fn test_sql_mode_ansi_quotes() {
	use surrealdb_mysql::lexer::tokenize_with_settings;
	use surrealdb_mysql::{ParserSettings, SqlMode};

	// Default mode: double quotes are string literals
	let tokens_default = tokenize(r#""hello" "world""#).unwrap();
	assert_eq!(tokens_default[0].kind, TokenKind::StringLiteral("hello".to_string()));
	assert_eq!(tokens_default[1].kind, TokenKind::StringLiteral("world".to_string()));

	// ANSI_QUOTES mode: double quotes are identifiers
	let settings = ParserSettings::default().with_sql_mode(SqlMode::empty().with_ansi_quotes(true));
	let tokens_ansi = tokenize_with_settings(r#""col_name" "table""name""#, settings).unwrap();
	assert_eq!(tokens_ansi[0].kind, TokenKind::Ident("col_name".to_string()));
	assert_eq!(tokens_ansi[1].kind, TokenKind::Ident("table\"name".to_string()));
}

#[test]
fn test_sql_mode_no_backslash_escapes() {
	use surrealdb_mysql::lexer::tokenize_with_settings;
	use surrealdb_mysql::{ParserSettings, SqlMode};

	// Default mode: \n is newline
	let tokens_default = tokenize(r"'hello\nworld'").unwrap();
	assert_eq!(tokens_default[0].kind, TokenKind::StringLiteral("hello\nworld".to_string()));

	// NO_BACKSLASH_ESCAPES mode: backslash is literal
	let settings =
		ParserSettings::default().with_sql_mode(SqlMode::empty().with_no_backslash_escapes(true));
	let tokens_nobackslash = tokenize_with_settings(r"'hello\nworld'", settings).unwrap();
	assert_eq!(tokens_nobackslash[0].kind, TokenKind::StringLiteral(r"hello\nworld".to_string()));
}

#[test]
fn test_executable_comments() {
	use surrealdb_mysql::ParserSettings;
	use surrealdb_mysql::lexer::tokenize_with_settings;

	// Version comment <= server_version (default 80400) executes
	let tokens = tokenize("SELECT /*!50000 1 */ + 1").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Select));
	assert_eq!(tokens[1].kind, TokenKind::IntLiteral(1));
	assert_eq!(tokens[2].kind, TokenKind::Plus);
	assert_eq!(tokens[3].kind, TokenKind::IntLiteral(1));

	// Version comment without version `/*! ... */` executes
	let tokens_no_ver = tokenize("/*!40101 SET NAMES utf8mb4 */;").unwrap();
	assert_eq!(tokens_no_ver[0].kind, TokenKind::Keyword(Keyword::Set));
	assert_eq!(tokens_no_ver[1].kind, TokenKind::Ident("NAMES".to_string()));
	assert_eq!(tokens_no_ver[2].kind, TokenKind::Ident("utf8mb4".to_string()));

	// Version comment > server_version is skipped as comment
	let settings = ParserSettings::default().with_server_version(50700);
	let tokens_skipped = tokenize_with_settings("SELECT /*!80000 1 + */ 2", settings).unwrap();
	assert_eq!(tokens_skipped[0].kind, TokenKind::Keyword(Keyword::Select));
	assert_eq!(tokens_skipped[1].kind, TokenKind::IntLiteral(2));
}

#[test]
fn test_charset_introducers() {
	let tokens = tokenize("_utf8mb4'hello' _binary 0x01AF _latin1'test' _utf8mb4").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::CharsetIntro("utf8mb4".to_string()));
	assert_eq!(tokens[1].kind, TokenKind::StringLiteral("hello".to_string()));
	assert_eq!(tokens[2].kind, TokenKind::CharsetIntro("binary".to_string()));
	assert_eq!(tokens[3].kind, TokenKind::HexLiteral(vec![0x01, 0xAF]));
	assert_eq!(tokens[4].kind, TokenKind::CharsetIntro("latin1".to_string()));
	assert_eq!(tokens[5].kind, TokenKind::StringLiteral("test".to_string()));
	// Non-introduced identifier
	assert_eq!(tokens[6].kind, TokenKind::Ident("_utf8mb4".to_string()));
}

#[test]
fn test_unicode_identifiers() {
	let tokens = tokenize("SELECT ユーザー, café FROM table_ñ").unwrap();
	assert_eq!(tokens[0].kind, TokenKind::Keyword(Keyword::Select));
	assert_eq!(tokens[1].kind, TokenKind::Ident("ユーザー".to_string()));
	assert_eq!(tokens[2].kind, TokenKind::Comma);
	assert_eq!(tokens[3].kind, TokenKind::Ident("café".to_string()));
	assert_eq!(tokens[4].kind, TokenKind::Keyword(Keyword::From));
	assert_eq!(tokens[5].kind, TokenKind::Ident("table_ñ".to_string()));
}
