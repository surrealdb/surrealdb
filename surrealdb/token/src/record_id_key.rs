use logos::Logos;

use crate::Joined;

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(extras = Joined)]
#[logos(subpattern backtick_ident = r"`([^`\\]|\\.)*`")]
#[logos(subpattern bracket_ident = r"⟨([^⟩\\]|\\.)*⟩")]
pub enum RecordIdKeyToken {
	#[token("..")]
	Range,
	#[regex("(?i)RAND")]
	Rand,
	#[regex("(?i)UUID")]
	Uuid,
	#[regex("(?i)ULID")]
	Ulid,
	#[token("{")]
	OpenBrace,
	#[token("[")]
	OpenBracket,
	#[regex(r#"u"([^"\\]|\\.)*""#)]
	#[regex(r#"u'([^'\\]|\\.)*'"#)]
	UuidString,
	#[regex(r#"(s)?"([^"\\]|\\.)*""#)]
	#[regex(r#"(s)?'([^'\\]|\\.)*'"#)]
	String,
	#[regex(r"[+\-]?([0-9]+)", priority = 3)]
	Number,
	#[regex(r"(?&backtick_ident)")]
	#[regex(r"(?&bracket_ident)")]
	#[regex(r"[_0-9a-zA-Z]+")]
	Identifier,
}
