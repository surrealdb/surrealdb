//! Rendering parser-next diagnostics in the established error wording.
//!
//! A [`Diag`] holds static templates and spans; this module substitutes the
//! source-dependent parts. Spans stay byte offsets: placing them in the
//! source is the renderer's job.

use std::borrow::Cow;

use surrealdb_parser_next::kw::Kw;
use surrealdb_parser_next::lex::{
	Lexer, PREFIX_DATE, PREFIX_INTERVAL, PREFIX_TIME, PREFIX_TIMESTAMP, T,
};
use surrealdb_parser_next::{Diag, DiagKind};

/// Words errors name as language, algorithm, distance and vector-type tokens
/// rather than identifiers, with the name each is given.
const WORD_TOKENS: &[(&str, &str)] = &[
	("AR", "ARABIC"),
	("ARA", "ARABIC"),
	("ARABIC", "ARABIC"),
	("DA", "DANISH"),
	("DAN", "DANISH"),
	("DANISH", "DANISH"),
	("DE", "GERMAN"),
	("DEU", "GERMAN"),
	("DUTCH", "DUTCH"),
	("EL", "GREEK"),
	("ELL", "GREEK"),
	("EN", "ENGLISH"),
	("ENG", "ENGLISH"),
	("ENGLISH", "ENGLISH"),
	("ES", "SPANISH"),
	("FI", "FINNISH"),
	("FIN", "FINNISH"),
	("FINNISH", "FINNISH"),
	("FR", "FRENCH"),
	("FRA", "FRENCH"),
	("FRENCH", "FRENCH"),
	("GERMAN", "GERMAN"),
	("GREEK", "GREEK"),
	("HU", "HUNGARIAN"),
	("HUN", "HUNGARIAN"),
	("HUNGARIAN", "HUNGARIAN"),
	("IT", "ITALIAN"),
	("ITA", "ITALIAN"),
	("ITALIAN", "ITALIAN"),
	("NL", "DUTCH"),
	("NLD", "DUTCH"),
	("NOR", "NORWEGIAN"),
	("NORWEGIAN", "NORWEGIAN"),
	("POR", "PORTUGUESE"),
	("PORTUGUESE", "PORTUGUESE"),
	("PT", "PORTUGUESE"),
	("RO", "ROMANIAN"),
	("ROMANIAN", "ROMANIAN"),
	("RON", "ROMANIAN"),
	("RU", "RUSSIAN"),
	("RUS", "RUSSIAN"),
	("RUSSIAN", "RUSSIAN"),
	("SPA", "SPANISH"),
	("SPANISH", "SPANISH"),
	("SV", "SWEDISH"),
	("SWE", "SWEDISH"),
	("SWEDISH", "SWEDISH"),
	("TA", "TAMIL"),
	("TAM", "TAMIL"),
	("TAMIL", "TAMIL"),
	("TR", "TURKISH"),
	("TUR", "TURKISH"),
	("TURKISH", "TURKISH"),
	("EDDSA", "EDDSA"),
	("ES256", "ES256"),
	("ES384", "ES384"),
	("ES512", "ES512"),
	("HS256", "HS256"),
	("HS384", "HS384"),
	("HS512", "HS512"),
	("PS256", "PS256"),
	("PS384", "PS384"),
	("PS512", "PS512"),
	("RS256", "RS256"),
	("RS384", "RS384"),
	("RS512", "RS512"),
	("CHEBYSHEV", "CHEBYSHEV"),
	("COSINE", "COSINE"),
	("COSINE_NORMALIZED", "COSINE_NORMALIZED"),
	("EUCLIDEAN", "EUCLIDEAN"),
	("HAMMING", "HAMMING"),
	("INNER_PRODUCT", "INNER_PRODUCT"),
	("JACCARD", "JACCARD"),
	("MANHATTAN", "MANHATTAN"),
	("MINKOWSKI", "MINKOWSKI"),
	("PEARSON", "PEARSON"),
	("F16", "F16"),
	("F32", "F32"),
	("F64", "F64"),
	("I16", "I16"),
	("I32", "I32"),
	("I64", "I64"),
	("I8", "I8"),
	("U8", "U8"),
];

/// The token at `at` as an error names it: `an identifier`, a
/// keyword in upper case, `a strand`, `;`.
pub fn token_name(src: &str, at: u32) -> Cow<'static, str> {
	let mut l = Lexer::new(src.as_bytes());
	l.pos = at;
	let t = l.next_token();
	let text = src.get(t.start as usize..t.end as usize).unwrap_or("");
	Cow::Borrowed(match t.kind {
		T::Eof => "Eof",
		T::Invalid => "Invalid",
		T::Ident => {
			if t.kw != Kw::None {
				// The keywords errors spell in lower case, and `SC`, which
				// errors name after the keyword it abbreviates.
				if t.kw == Kw::True {
					"true"
				} else if t.kw == Kw::False {
					"false"
				} else if t.kw == Kw::Fn {
					"fn"
				} else if t.kw == Kw::Silo {
					"silo"
				} else if t.kw == Kw::Mod {
					"mod"
				} else if t.kw == Kw::Ext {
					"ext"
				} else if t.kw == Kw::Ml {
					"ml"
				} else if t.kw == Kw::Scope {
					"SCOPE"
				} else {
					t.kw.text()
				}
			} else if let Some(&(_, name)) =
				WORD_TOKENS.iter().find(|(w, _)| w.eq_ignore_ascii_case(text))
			{
				name
			} else {
				"an identifier"
			}
		}
		T::EscIdent => "an identifier",
		T::NaN => "NaN",
		T::Infinity => "Infinity",
		T::Param => "a parameter",
		T::Digits => "a number",
		T::Str => match t.aux() {
			b'r' => "a record-id strand",
			b'u' => "a uuid",
			b'd' => "a datetime",
			b'b' => "a bytestring",
			b'f' => "a file",
			PREFIX_DATE => "a date",
			PREFIX_TIME => "a time",
			PREFIX_TIMESTAMP => "a timestamp",
			PREFIX_INTERVAL => "an interval",
			_ => "a strand",
		},
		T::Regex | T::Slash => "/",
		T::LParen => "(",
		T::RParen => ")",
		T::LBracket => "[",
		T::RBracket => "]",
		T::LBrace => "{",
		T::RBrace => "}",
		T::Comma => ",",
		T::Semi => ";",
		T::Colon => ":",
		T::PathSep => "::",
		T::Dot => ".",
		T::DotDot => "..",
		T::DotDotDot => "...",
		T::Arrow => "->",
		T::Lt => "<",
		T::Gt => ">",
		T::Le => "<=",
		T::Ge => ">=",
		T::Eq => "=",
		T::EqEq => "==",
		T::Ne => "!=",
		T::Plus => "+",
		T::Minus => "-",
		T::Star => "*",
		T::StarStar => "**",
		T::Percent => "%",
		T::Bang => "!",
		// Errors name only the first `?` of `??`.
		T::Question | T::QQ => "?",
		T::QColon => "?:",
		T::AnyEq => "?=",
		T::AllEq => "*=",
		T::Tilde => "~",
		T::NotLike => "!~",
		T::AnyLike => "?~",
		T::AllLike => "*~",
		T::Pipe => "|",
		T::OrOr => "||",
		T::AndAnd => "&&",
		T::At => "@",
		T::Dollar => "$",
		T::KnnOpen => "<|",
		T::PlusEq => "+=",
		T::MinusEq => "-=",
		T::PlusQEq => "+?=",
		T::Times => "×",
		T::Divide => "÷",
		T::Contains => "∋",
		T::NotContains => "∌",
		T::ContainsAll => "⊇",
		T::ContainsAny => "⊃",
		T::ContainsNone => "⊅",
		T::Inside => "∈",
		T::NotInside => "∉",
		T::AllInside => "⊆",
		T::AnyInside => "⊂",
		T::NoneInside => "⊄",
	})
}

/// Substitutes `{found}`, `{text}`, `{char}`, `{last}` (the last character of
/// the text), `{next}` (the character after it), `{last_byte}` (its last
/// byte, as a number) and `{suggest}` (the builtin
/// path suggested for the text) in `template` from the source at
/// `start..end`.
pub fn fill(template: &str, src: &str, start: u32, end: u32) -> String {
	if !template.contains('{') {
		return template.to_owned();
	}
	let text = src.get(start as usize..end as usize).unwrap_or("");
	let first: String = text.chars().next().map(String::from).unwrap_or_default();
	let last: String = text.chars().next_back().map(String::from).unwrap_or_default();
	let next: String = src
		.get(end as usize..)
		.and_then(|r| r.chars().next())
		.map(String::from)
		.unwrap_or_default();
	let mut out = template
		.replace("{found}", &token_name(src, start))
		.replace("{text}", text)
		.replace("{char}", &first)
		.replace("{last}", &last)
		.replace("{next}", &next);
	if out.contains("{last_byte}") {
		// The last byte of the span as a number, which the established
		// wording prints where a character is meant.
		let byte = end.checked_sub(1).and_then(|i| src.as_bytes().get(i as usize));
		out = out.replace("{last_byte}", &byte.map_or(String::new(), u8::to_string));
	}
	if out.contains("{first}") {
		let mut l = Lexer::new(src.as_bytes());
		l.pos = start;
		let t = l.next_token();
		out = out.replace("{first}", src.get(t.start as usize..t.end as usize).unwrap_or(""));
	}
	if out.contains("{regex_error}") {
		out = out.replace("{regex_error}", &super::lit::regex_error(text));
	}
	if out.contains("{table}") {
		// The record-id table name before `table:` at `start`.
		let before = src.get(..start as usize).unwrap_or("").trim_end_matches(':');
		let name_start = before
			.char_indices()
			.rev()
			.find(|(_, c)| !(c.is_alphanumeric() || *c == '_'))
			.map_or(0, |(i, c)| i + c.len_utf8());
		out = out.replace("{table}", &before[name_start..]);
	}
	if out.contains("{suggest}") {
		out = out.replace("{suggest}", &super::lit::suggestion(text));
	}
	out
}

/// The message of `d` in the established wording.
pub fn message(src: &str, d: &Diag) -> String {
	match d.kind {
		DiagKind::Message => fill(d.msg, src, d.start, d.end),
		DiagKind::Unexpected => {
			let expected = fill(d.msg, src, d.start, d.end);
			if d.start as usize >= src.len() {
				format!("Unexpected end of file, expected {expected}")
			} else {
				format!("Unexpected token `{}`, expected {expected}", token_name(src, d.start))
			}
		}
	}
}

/// `d`, a diagnostic against `src`, as a lowering failure in the established wording.
pub fn reject(src: &str, d: &Diag) -> super::Reject {
	let mut r = super::Reject::new(message(src, d)).at(d.start, d.end);
	if !d.label.is_empty() {
		r = r.with_label(fill(d.label, src, d.start, d.end));
	}
	r.extra = d.extra.map(|x| (x.start, x.end, fill(x.label, src, x.start, x.end).into()));
	r
}
