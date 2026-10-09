//! Statement-at-a-time parsing of input that arrives in pieces.

use bytes::BytesMut;
use surrealdb_parser_next_lower::{StatementStream, parse};
use surrealdb_types::ToSql;

/// A statement is returned only once more input could not change it, and the
/// statements streamed are those a whole parse gives.
#[test]
fn streamed_statements_match_a_whole_parse() {
	let mut stream = StatementStream::new();
	let mut buf = BytesMut::from("RETURN 1; RETURN 'a;b'; RETURN [");
	let mut streamed = Vec::new();
	while let Ok(Some(s)) = stream.parse_partial(&mut buf) {
		streamed.push(s.to_sql());
	}
	assert_eq!(streamed.len(), 2, "{streamed:?}");
	buf.extend_from_slice(b"2];");
	while let Ok(Some(s)) = stream.parse_complete(&mut buf) {
		streamed.push(s.to_sql());
	}
	let whole: Vec<String> = parse("RETURN 1; RETURN 'a;b'; RETURN [2];")
		.expect("valid")
		.expressions
		.iter()
		.map(ToSql::to_sql)
		.collect();
	assert_eq!(streamed, whole);
}

/// A character split at the end of a partial buffer waits for the rest of
/// its bytes; bytes that are not UTF-8 are an error.
#[test]
fn utf8_is_read_across_chunks_and_checked() {
	let text = "RETURN 'é';".as_bytes();
	let split = text.iter().position(|&b| b >= 0x80).expect("a multi-byte character") + 1;
	let mut stream = StatementStream::new();
	let mut buf = BytesMut::from(&text[..split]);
	assert!(stream.parse_partial(&mut buf).expect("no error yet").is_none());
	buf.extend_from_slice(&text[split..]);
	let s = stream.parse_complete(&mut buf).expect("valid").expect("a statement");
	assert_eq!(s.to_sql(), "RETURN 'é'");

	let mut stream = StatementStream::new();
	let mut buf = BytesMut::from(&b"RETURN 1; \xff"[..]);
	assert!(stream.parse_complete(&mut buf).expect("the first statement").is_some());
	let err = stream.parse_complete(&mut buf).expect_err("invalid UTF-8");
	assert!(err.to_string().contains("non valid UTF-8"), "{err}");
}

/// A rejection is located in the whole stream, not in the current buffer.
#[test]
fn errors_are_located_in_the_whole_stream() {
	let mut stream = StatementStream::new();
	let mut buf = BytesMut::from("RETURN 1;\nRETURN 2;\nSELECT * FROM;");
	while let Ok(Some(_)) = stream.parse_complete(&mut buf) {}
	let err = stream.parse_complete(&mut buf).expect_err("the third statement is invalid");
	assert!(err.to_string().contains("[3:"), "{err}");
}
