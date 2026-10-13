use logos::{Lexer, Logos};

/// Consume the remainder of a string token up to and including the unescaped `TERMINATOR`
/// quote, skipping over backslash escape sequences.
///
/// Errors if the source ends before the terminator, consuming the rest of the source.
fn eat_string_impl<'s, const TERMINATOR: u8, T: Logos<'s, Source = str>>(
	lexer: &mut Lexer<'s, T>,
) -> Result<(), ()> {
	let mut iter = lexer.remainder().as_bytes().iter().copied().enumerate();

	while let Some((idx, b)) = iter.next() {
		if b == TERMINATOR {
			lexer.bump(idx + 1);
			return Ok(());
		}
		if b == b'\\' {
			iter.next();
		}
	}

	lexer.bump(lexer.remainder().len());
	Err(())
}

fn eat_string<const TERMINATOR: u8>(lexer: &mut Lexer<Token>) -> Result<(), ()> {
	eat_string_impl::<TERMINATOR, Token>(lexer)
}

fn eat_string_key<const TERMINATOR: u8>(lexer: &mut Lexer<RecordIdKeyToken>) -> Result<(), ()> {
	eat_string_impl::<TERMINATOR, RecordIdKeyToken>(lexer)
}

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(subpattern duration_part = r"[0-9]+(y|w|d|h|m(s)?|s|us|µs|ns)")]
#[logos(subpattern backtick_ident = r"`([^`\\]|\\.)*`")]
#[logos(subpattern bracket_ident = r"⟨([^⟩\\]|\\.)*⟩")]
#[logos(subpattern whitespace = r"[ \t\n\r\u{000B}\u{000C}\u{0085}\u{00A0}\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}\u{2007}\u{2008}\u{2009}\u{200A}\u{2028}\u{2029}\u{202F}\u{205F}\u{3000}]+")]
#[logos(subpattern multi_line_comment = r"/\*([^*]|\*+[^*/])*\*+/")]
#[logos(subpattern line_comment = r"(//|#|--)[^\n\r\u{2028}\u{2029}]*")]
#[logos(skip(r"((?&whitespace)|(?&line_comment)|(?&multi_line_comment))+"))]
pub enum Token {
	#[token("{")]
	/// `{`
	OpenBrace,
	#[token("}")]
	/// `}`
	CloseBrace,
	#[token("[")]
	/// `[`
	OpenBracket,
	#[token("]")]
	/// `]`
	CloseBracket,
	#[token(",")]
	Comma,
	#[token(":")]
	Colon,

	#[token("..")]
	DotDot,
	#[token("..=")]
	DotDotEqual,
	#[token(">..")]
	ShevronDotDot,
	#[token(">..=")]
	ShevronDotDotEqual,

	#[regex(r"(?&backtick_ident)")]
	#[regex(r"(?&bracket_ident)")]
	#[regex(r"[_\p{XID_Start}]\p{XID_Continue}*")]
	Ident,

	#[token("s\"", callback = eat_string::<b'\"'>)]
	#[token("s'", callback = eat_string::<b'\''>)]
	#[token("\"", callback = eat_string::<b'\"'>)]
	#[token("'", callback = eat_string::<b'\''>)]
	String,
	#[token("r\"", callback = eat_string::<b'\"'>)]
	#[token("r'", callback = eat_string::<b'\''>)]
	RecordIdString,
	#[token("u\"", callback = eat_string::<b'\"'>)]
	#[token("u'", callback = eat_string::<b'\''>)]
	UuidString,
	#[token("d\"", callback = eat_string::<b'\"'>)]
	#[token("d'", callback = eat_string::<b'\''>)]
	DateTimeString,
	#[token("f\"", callback = eat_string::<b'\"'>)]
	#[token("f'", callback = eat_string::<b'\''>)]
	FileString,
	#[token("b\"", callback = eat_string::<b'\"'>)]
	#[token("b'", callback = eat_string::<b'\''>)]
	ByteString,

	#[token("NaN")]
	NaN,
	#[token(r"Infinity")]
	Infinity,
	#[token(r"+Infinity")]
	PosInfinity,
	#[token(r"-Infinity")]
	NegInfinity,
	#[regex(r"[\-+]?[0-9][0-9_]*f")]
	#[regex(r"[\-+]?[0-9][0-9_]*(\.[0-9][0-9_]*)?([eE][-+]?[0-9][0-9_]*)?(f)?")]
	// Don't allow a float postfix to be immediatly followed by an identifier
	#[regex(r"[\-+]?[0-9][0-9_]*(\.[0-9][0-9_]*)?([eE][-+]?[0-9][0-9_]*)?f[_\p{XID_START}]",callback = |_| None)]
	Float,
	#[regex(r"[\-+]?[0-9][0-9_]*(\.[0-9][0-9_]*)?([eE][-+]?[0-9][0-9_]*)?dec")]
	// Don't allow a decimal postfix to be immediatly followed by an identifier
	#[regex(r"[\-+]?[0-9][0-9_]*(\.[0-9][0-9_]*)?([eE][-+]?[0-9][0-9_]*)?dec[_\p{XID_START}]",callback = |_| None)]
	Decimal,
	#[regex(r"[\-+]?[0-9][0-9_]*", priority = 3)]
	Int,
	#[regex(r"(?&duration_part)+", priority = 5)]
	// Don't allow a duration postfix to be immediatly followed by an identifier
	#[regex(r"(?&duration_part)+[_\p{XID_START}]", callback = |_| None,priority = 4)]
	Duration,

	#[regex("(?i)NULL")]
	KwNull,
	#[regex("(?i)NONE")]
	KwNone,
	#[regex("(?i)TRUE")]
	KwTrue,
	#[regex("(?i)FALSE")]
	KwFalse,
}

#[derive(Logos, Clone, Copy, PartialEq, Eq, Debug)]
#[logos(subpattern backtick_ident = r"`([^`\\]|\\.)*`")]
#[logos(subpattern bracket_ident = r"⟨([^⟩\\]|\\.)*⟩")]
pub enum RecordIdKeyToken {
	#[regex(r"(?&backtick_ident)")]
	#[regex(r"(?&bracket_ident)")]
	#[regex(r"[_\p{XID_Start}0-9]\p{XID_Continue}*")]
	Ident,
	#[token("{")]
	Brace,
	#[token("[")]
	Bracket,
	#[regex(r"[\-+]?[0-9][0-9_]*", priority = 3)]
	Integer,
	#[token("u\"", callback = eat_string_key::<b'\"'>)]
	#[token("u'", callback = eat_string_key::<b'\''>)]
	UuidString,
	#[token("s\"", callback = eat_string_key::<b'\"'>)]
	#[token("s'", callback = eat_string_key::<b'\''>)]
	#[token("\"", callback = eat_string_key::<b'\"'>)]
	#[token("'", callback = eat_string_key::<b'\''>)]
	String,
	#[token("..")]
	DotDot,
	#[token("..=")]
	DotDotEqual,
}
