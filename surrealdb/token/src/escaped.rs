use logos::Logos;

use crate::Joined;

/// Token type used to unescape all stringly formatted surrealql structures.
#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
pub enum EscapeTokenKind {
	#[token("\\n")]
	EscNewline,
	#[token("\\r")]
	EscCarriageReturn,
	#[token("\\t")]
	EscTab,
	#[token("\\0")]
	EscZeroByte,
	#[token("\\\\")]
	EscBackSlash,
	#[token("\\b")]
	EscBackSpace,
	#[token("\\f")]
	EscFormFeed,
	#[token("\\'")]
	EscQuote,
	#[token("\\\"")]
	EscDoubleQuote,
	#[token("\\`")]
	EscBackTick,
	#[token("\\⟩")]
	EscBracketClose,
	#[regex(r#"\\u\{[0-9a-fA-F]{1,6}}"#)]
	EscUnicode,
	#[regex(r#"[^\\]+"#, priority = 0)]
	Chars,
}
