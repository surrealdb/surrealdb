use unicase::UniCase;

use crate::gql::lexer::{Lexer, keywords};
use crate::gql::token::{Keyword, NumberKind, NumberSuffix, Span, Token, TokenKind, t};
use crate::syn::error::SyntaxError;

/// Lexes the source into token kinds, asserting that no token is invalid.
fn tokens(source: &str) -> Vec<TokenKind> {
	let mut lexer = Lexer::new(source);
	let mut result = Vec::new();
	loop {
		let token = lexer.next_token();
		if token.is_eof() {
			break;
		}
		if token.kind == TokenKind::Invalid {
			panic!("unexpected invalid token in {source:?}: {:?}", lexer.error);
		}
		result.push(token.kind);
	}
	result
}

/// Lexes the source until the first invalid token, returning it together
/// with its error.
fn lex_error(source: &str) -> (Token, SyntaxError) {
	let mut lexer = Lexer::new(source);
	loop {
		let token = lexer.next_token();
		match token.kind {
			TokenKind::Invalid => {
				let error = lexer.error.take().expect("invalid token without an error");
				return (token, error);
			}
			TokenKind::Eof => panic!("no invalid token in {source:?}"),
			_ => {}
		}
	}
}

/// Lexes a single token and returns its kind together with its source text.
fn single(source: &str) -> (TokenKind, &str) {
	let mut lexer = Lexer::new(source);
	let token = lexer.next_token();
	assert_ne!(token.kind, TokenKind::Invalid, "{:?}", lexer.error);
	(token.kind, lexer.span_str(token.span))
}

/// Lexes a single quoted token and decodes its value.
fn quoted_value(source: &str) -> String {
	let mut lexer = Lexer::new(source);
	let token = lexer.next_token();
	assert!(
		matches!(
			token.kind,
			TokenKind::SingleQuoted { .. }
				| TokenKind::DoubleQuoted { .. }
				| TokenKind::AccentQuoted { .. }
		),
		"{:?}: {:?}",
		token.kind,
		lexer.error
	);
	Lexer::unescape_quoted_span(lexer.span_str(token.span), token.span).expect("decoding failed")
}

/// Lexes a single quoted token and returns its decode error.
fn quoted_error(source: &str) -> SyntaxError {
	let mut lexer = Lexer::new(source);
	let token = lexer.next_token();
	Lexer::unescape_quoted_span(lexer.span_str(token.span), token.span)
		.expect_err("decoding should have failed")
}

fn render(error: &SyntaxError, source: &str) -> String {
	format!("{:?}", error.render_on(source))
}

#[test]
fn simple_tokens() {
	assert_eq!(
		tokens("( ) [ ] { } , & % ! ? * + : = > < - . / ~ | @"),
		vec![
			t!("("),
			t!(")"),
			t!("["),
			t!("]"),
			t!("{"),
			t!("}"),
			t!(","),
			t!("&"),
			t!("%"),
			t!("!"),
			t!("?"),
			t!("*"),
			t!("+"),
			t!(":"),
			t!("="),
			t!(">"),
			t!("<"),
			t!("-"),
			t!("."),
			t!("/"),
			t!("~"),
			t!("|"),
			t!("@"),
		]
	);
}

#[test]
fn compound_tokens() {
	// Every compound token from GQL.g4:3629-3658, space separated.
	assert_eq!(
		tokens(
			"|+| ]-> ]~> || :: .. >= <- <~ <-[ <~[ <-> <-/ <~/ <= -[ -/ <> -> ]- ]~ => /- /-> /~ /~> ~[ ~> ~/"
		),
		vec![
			t!("|+|"),
			t!("]->"),
			t!("]~>"),
			t!("||"),
			t!("::"),
			t!(".."),
			t!(">="),
			t!("<-"),
			t!("<~"),
			t!("<-["),
			t!("<~["),
			t!("<->"),
			t!("<-/"),
			t!("<~/"),
			t!("<="),
			t!("-["),
			t!("-/"),
			t!("<>"),
			t!("->"),
			t!("]-"),
			t!("]~"),
			t!("=>"),
			t!("/-"),
			t!("/->"),
			t!("/~"),
			t!("/~>"),
			t!("~["),
			t!("~>"),
			t!("~/"),
		]
	);
}

#[test]
fn compound_longest_match() {
	// Compounds are single tokens; spaced-out constituents are not.
	assert_eq!(tokens("<-["), vec![t!("<-[")]);
	assert_eq!(tokens("<- ["), vec![t!("<-"), t!("[")]);
	assert_eq!(tokens("< -["), vec![t!("<"), t!("-[")]);
	assert_eq!(tokens("]->"), vec![t!("]->")]);
	assert_eq!(tokens("]- >"), vec![t!("]-"), t!(">")]);
	assert_eq!(tokens("<=>"), vec![t!("<="), t!(">")]);
	assert_eq!(tokens("<>="), vec![t!("<>"), t!("=")]);
	assert_eq!(tokens("|+"), vec![t!("|"), t!("+")]);
	assert_eq!(tokens("||+|"), vec![t!("||"), t!("+"), t!("|")]);
	assert_eq!(tokens("..."), vec![t!(".."), t!(".")]);
	assert_eq!(tokens(":::"), vec![t!("::"), t!(":")]);
	assert_eq!(tokens("~/>"), vec![t!("~/"), t!(">")]);
	assert_eq!(tokens("/~>>"), vec![t!("/~>"), t!(">")]);
}

#[test]
fn minus_comment_trap() {
	// The openCypher trap: `--` is a line comment, never an edge.
	assert_eq!(tokens("(a)--(b)"), vec![t!("("), TokenKind::Identifier, t!(")")]);
	// The GQL any-direction abbreviation is a single `-`.
	assert_eq!(
		tokens("(a)-(b)"),
		vec![
			t!("("),
			TokenKind::Identifier,
			t!(")"),
			t!("-"),
			t!("("),
			TokenKind::Identifier,
			t!(")"),
		]
	);
	// `-[` wins over `-` and is never a comment.
	assert_eq!(tokens("-[k]->"), vec![t!("-["), TokenKind::Identifier, t!("]->")]);
	// `-->` is a comment introducer followed by commented-out text.
	assert_eq!(tokens("a -->(b)"), vec![TokenKind::Identifier]);
	// A `--` comment ends at the line end.
	assert_eq!(tokens("a --comment\nb"), vec![TokenKind::Identifier, TokenKind::Identifier]);
	assert_eq!(tokens("a --comment\r\nb"), vec![TokenKind::Identifier, TokenKind::Identifier]);
}

#[test]
fn comments() {
	assert_eq!(tokens("// only a comment"), vec![]);
	assert_eq!(tokens("-- only a comment"), vec![]);
	assert_eq!(tokens("// a\n// b\nc"), vec![TokenKind::Identifier]);
	assert_eq!(tokens("/* multi\nline */ a"), vec![TokenKind::Identifier]);
	assert_eq!(tokens("/**/a"), vec![TokenKind::Identifier]);
	// Bracketed comments end at the FIRST `*/`: they do not nest.
	assert_eq!(tokens("/* /* */ a"), vec![TokenKind::Identifier]);
	// The `/** … */` language-test header form lexes as a comment.
	assert_eq!(tokens("/** header\n*/ MATCH"), vec![t!("MATCH")]);
	// Comment introducers inside line comments are inert.
	assert_eq!(tokens("// /* \na"), vec![TokenKind::Identifier]);
}

#[test]
fn unterminated_comment() {
	let (token, error) = lex_error("a /* unterminated");
	// The invalid token spans the whole unterminated comment.
	assert_eq!(
		token.span,
		Span {
			offset: 2,
			len: 15,
		}
	);
	let rendered = render(&error, "a /* unterminated");
	assert!(rendered.contains("comment"), "{rendered}");
}

#[test]
fn keywords_case_insensitive() {
	assert_eq!(tokens("match"), vec![t!("MATCH")]);
	assert_eq!(tokens("MATCH"), vec![t!("MATCH")]);
	assert_eq!(tokens("MaTcH"), vec![t!("MATCH")]);
	assert_eq!(
		tokens("MATCH (a) WHERE true RETURN a"),
		vec![
			t!("MATCH"),
			t!("("),
			TokenKind::Identifier,
			t!(")"),
			t!("WHERE"),
			t!("TRUE"),
			t!("RETURN"),
			TokenKind::Identifier,
		]
	);
}

#[test]
fn keyword_classes_lex_as_keywords() {
	// All three keyword classes lex to a Keyword token; the parser decides
	// whether a non-reserved keyword may act as an identifier.
	assert_eq!(tokens("RETURN"), vec![t!("RETURN")]); // reserved
	assert_eq!(tokens("abstract"), vec![t!("ABSTRACT")]); // prereserved
	assert_eq!(tokens("node"), vec![t!("NODE")]); // non-reserved
	assert_eq!(tokens("type"), vec![t!("TYPE")]); // non-reserved
	// The token text is recoverable from the span for identifier use.
	assert_eq!(single("Node"), (t!("NODE"), "Node"));
}

#[test]
fn keyword_map_is_complete() {
	// 218 reserved (incl. TRUE/FALSE/UNKNOWN) + 39 prereserved + 47
	// non-reserved keywords, see token/keyword.rs.
	assert_eq!(keywords::KEYWORDS.len(), 307);
	// Together with the count this proves the map is a bijection over all
	// Keyword variants.
	for (key, keyword) in keywords::KEYWORDS.entries() {
		assert_eq!(*key, UniCase::ascii(keyword.as_str()));
	}
}

#[test]
fn identifiers() {
	assert_eq!(tokens("foo"), vec![TokenKind::Identifier]);
	assert_eq!(tokens("_foo"), vec![TokenKind::Identifier]);
	assert_eq!(tokens("foo_bar9"), vec![TokenKind::Identifier]);
	assert_eq!(single("matches"), (TokenKind::Identifier, "matches"));
	// Unicode identifiers: ID_Start letters and Pc connector punctuation.
	assert_eq!(single("caf\u{e9}"), (TokenKind::Identifier, "caf\u{e9}"));
	assert_eq!(single("a\u{203F}b"), (TokenKind::Identifier, "a\u{203F}b"));
	assert_eq!(single("\u{0394}\u{03B5}"), (TokenKind::Identifier, "\u{0394}\u{03B5}"));
	// Identifiers cannot start with a digit.
	assert_eq!(
		tokens("1abc"),
		vec![
			TokenKind::Number {
				kind: NumberKind::Integer,
				suffix: None,
			},
			TokenKind::Identifier,
		]
	);
}

#[test]
fn quoted_token_kinds() {
	assert_eq!(tokens("'a'"), vec![t!("'")]);
	assert_eq!(tokens("@'a'"), vec![t!("@'")]);
	assert_eq!(tokens("\"a\""), vec![t!("\"")]);
	assert_eq!(tokens("@\"a\""), vec![t!("@\"")]);
	assert_eq!(tokens("`a`"), vec![t!("`")]);
	assert_eq!(tokens("@`a`"), vec![t!("@`")]);
	// The span covers the whole token, including prefix and quotes.
	assert_eq!(single("@'a b'"), (t!("@'"), "@'a b'"));
	// An `@` not followed by a quote is the commercial-at token.
	assert_eq!(tokens("@a"), vec![t!("@"), TokenKind::Identifier]);
}

#[test]
fn string_escapes() {
	assert_eq!(quoted_value(r"'a\nb'"), "a\nb");
	assert_eq!(quoted_value(r"'a\tb'"), "a\tb");
	assert_eq!(quoted_value(r"'a\rb'"), "a\rb");
	assert_eq!(quoted_value(r"'a\bb'"), "a\u{0008}b");
	assert_eq!(quoted_value(r"'a\fb'"), "a\u{000C}b");
	assert_eq!(quoted_value(r"'a\\b'"), "a\\b");
	assert_eq!(quoted_value(r"'a\'b'"), "a'b");
	assert_eq!(quoted_value(r#"'a\"b'"#), "a\"b");
	assert_eq!(quoted_value(r"'a\`b'"), "a`b");
	assert_eq!(quoted_value(r"'\u0041'"), "A");
	assert_eq!(quoted_value(r"'\u00e9'"), "\u{e9}");
	// Unicode escape hex digits are case-insensitive.
	assert_eq!(quoted_value(r"'\u00E9'"), "\u{e9}");
	assert_eq!(quoted_value(r"'\U01F600'"), "\u{1F600}");
	// Escapes work the same in double- and accent-quoted sequences.
	assert_eq!(quoted_value(r#""a\nb""#), "a\nb");
	assert_eq!(quoted_value(r"`a\nb`"), "a\nb");
}

#[test]
fn string_escape_errors() {
	// Escape letters are case-sensitive: `\N` is not a newline escape.
	let error = quoted_error(r"'a\Nb'");
	assert!(render(&error, r"'a\Nb'").contains("case-sensitive"));
	quoted_error(r"'a\qb'");
	// `\U` must be followed by exactly 6 hex digits, `\u` by 4.
	quoted_error(r"'\u12'");
	quoted_error(r"'\u12G4'");
	quoted_error(r"'\U0041'");
	// A lone surrogate is not a valid unicode code point.
	let error = quoted_error(r"'\uD800'");
	assert!(render(&error, r"'\uD800'").contains("code point"));
}

#[test]
fn string_quote_doubling() {
	assert_eq!(quoted_value("'it''s'"), "it's");
	assert_eq!(quoted_value(r#""a""b""#), "a\"b");
	assert_eq!(quoted_value("`a``b`"), "a`b");
	assert_eq!(quoted_value("''''"), "'");
	// Doubling also works in no-escape mode.
	assert_eq!(quoted_value("@'it''s'"), "it's");
	// An empty string.
	assert_eq!(quoted_value("''"), "");
}

#[test]
fn string_no_escape_mode() {
	// `@` disables escape processing: the backslash is an ordinary
	// character.
	assert_eq!(quoted_value(r"@'a\nb'"), "a\\nb");
	assert_eq!(quoted_value(r"@'C:\dir'"), "C:\\dir");
	// In no-escape mode `\'` does not escape the quote: the string ends at
	// the quote.
	assert_eq!(single(r"@'a\'"), (t!("@'"), r"@'a\'"));
	assert_eq!(quoted_value(r"@'a\'"), "a\\");
}

#[test]
fn string_raw_newline() {
	let (_, error) = lex_error("'a\nb'");
	assert!(render(&error, "'a\nb'").contains("newline"));
	lex_error("'a\rb'");
	lex_error("\"a\nb\"");
	lex_error("`a\nb`");
	// Raw newlines are equally forbidden in no-escape mode.
	lex_error("@'a\nb'");
	// And directly after a backslash.
	lex_error("'a\\\nb'");
}

#[test]
fn unterminated_string() {
	let (token, error) = lex_error("'abc");
	assert_eq!(
		token.span,
		Span {
			offset: 0,
			len: 4,
		}
	);
	assert!(render(&error, "'abc").contains("end of file"));
	// An escaped quote does not terminate the string.
	lex_error(r"'abc\'");
	lex_error("`abc");
}

#[test]
fn numbers_integer() {
	let int = |suffix| TokenKind::Number {
		kind: NumberKind::Integer,
		suffix,
	};
	assert_eq!(single("123"), (int(None), "123"));
	assert_eq!(single("0"), (int(None), "0"));
	// No special leading-zero rule: `010` is plain decimal.
	assert_eq!(single("010"), (int(None), "010"));
	assert_eq!(single("1_000_000"), (int(None), "1_000_000"));
	assert_eq!(single("123M"), (int(Some(NumberSuffix::Exact)), "123M"));
	assert_eq!(single("123m"), (int(Some(NumberSuffix::Exact)), "123m"));
	assert_eq!(single("123F"), (int(Some(NumberSuffix::Float)), "123F"));
	assert_eq!(single("3D"), (int(Some(NumberSuffix::Double)), "3D"));
	assert_eq!(single("3d"), (int(Some(NumberSuffix::Double)), "3d"));
}

#[test]
fn numbers_prefixed() {
	let num = |kind| TokenKind::Number {
		kind,
		suffix: None,
	};
	assert_eq!(single("0xdead_beef"), (num(NumberKind::Hex), "0xdead_beef"));
	// Hex digits are case-insensitive, only the prefix letter is not.
	assert_eq!(single("0xDEAD"), (num(NumberKind::Hex), "0xDEAD"));
	// An underscore may directly follow the prefix.
	assert_eq!(single("0x_1"), (num(NumberKind::Hex), "0x_1"));
	assert_eq!(single("0o777"), (num(NumberKind::Octal), "0o777"));
	assert_eq!(single("0b1010"), (num(NumberKind::Binary), "0b1010"));
	// The prefix letters are lowercase only: `0X1` is `0` then `X1`.
	assert_eq!(tokens("0X1"), vec![num(NumberKind::Integer), TokenKind::Identifier]);
	// Without a digit the prefix letter starts an identifier instead.
	assert_eq!(tokens("0x"), vec![num(NumberKind::Integer), TokenKind::Identifier]);
	assert_eq!(tokens("0xg"), vec![num(NumberKind::Integer), TokenKind::Identifier]);
	// Digits beyond the base end the token, like the grammar splits it.
	assert_eq!(tokens("0b12"), vec![num(NumberKind::Binary), num(NumberKind::Integer)]);
}

#[test]
fn numbers_float() {
	let float = |suffix| TokenKind::Number {
		kind: NumberKind::Float,
		suffix,
	};
	assert_eq!(single("123.456"), (float(None), "123.456"));
	assert_eq!(single("123."), (float(None), "123."));
	assert_eq!(single(".456"), (float(None), ".456"));
	assert_eq!(single("1_0.2_5"), (float(None), "1_0.2_5"));
	assert_eq!(single("1.5M"), (float(Some(NumberSuffix::Exact)), "1.5M"));
	assert_eq!(single("2.0F"), (float(Some(NumberSuffix::Float)), "2.0F"));
	assert_eq!(single("2.0d"), (float(Some(NumberSuffix::Double)), "2.0d"));
	// Maximal munch: the period belongs to the number.
	assert_eq!(tokens("123..456"), vec![float(None), float(None)]);
	assert_eq!(tokens("123.bar"), vec![float(None), TokenKind::Identifier]);
	// ... and so does a suffix letter: `123.fish` is `123.f` then `ish`.
	assert_eq!(tokens("123.fish"), vec![float(Some(NumberSuffix::Float)), TokenKind::Identifier]);
}

#[test]
fn numbers_scientific() {
	let sci = |suffix| TokenKind::Number {
		kind: NumberKind::Scientific,
		suffix,
	};
	assert_eq!(single("1.5e10"), (sci(None), "1.5e10"));
	assert_eq!(single("2E-3"), (sci(None), "2E-3"));
	assert_eq!(single("1e+5"), (sci(None), "1e+5"));
	assert_eq!(single("1.e5"), (sci(None), "1.e5"));
	assert_eq!(single(".5e2"), (sci(None), ".5e2"));
	assert_eq!(single("1e2f"), (sci(Some(NumberSuffix::Float)), "1e2f"));
	assert_eq!(single("1e1_0"), (sci(None), "1e1_0"));
	// `e` without an exponent is not consumed: `1e` is `1` then `e`.
	assert_eq!(
		tokens("1e"),
		vec![
			TokenKind::Number {
				kind: NumberKind::Integer,
				suffix: None,
			},
			TokenKind::Identifier,
		]
	);
	assert_eq!(
		tokens("1e+"),
		vec![
			TokenKind::Number {
				kind: NumberKind::Integer,
				suffix: None,
			},
			TokenKind::Identifier,
			t!("+"),
		]
	);
}

#[test]
fn numbers_suffix_split() {
	// The suffix is consumed even when identifier characters follow, like
	// the grammar splits it.
	assert_eq!(
		tokens("123fish"),
		vec![
			TokenKind::Number {
				kind: NumberKind::Integer,
				suffix: Some(NumberSuffix::Float),
			},
			TokenKind::Identifier,
		]
	);
}

#[test]
fn numbers_underscore_errors() {
	// Misplaced underscore digit separators are lexical errors.
	let (_, error) = lex_error("1__2");
	assert!(render(&error, "1__2").contains("underscore"));
	lex_error("1_");
	lex_error("0x1__2");
	lex_error("0x1_");
	lex_error("0x_");
	lex_error("1.2_");
	lex_error("1e2_");
	// A leading underscore is an identifier, not a number.
	assert_eq!(tokens("_1"), vec![TokenKind::Identifier]);
}

#[test]
fn parameters() {
	assert_eq!(single("$x"), (t!("$param"), "$x"));
	assert_eq!(single("$where"), (t!("$param"), "$where"));
	// Parameter names are extended identifiers: they may start with a digit.
	assert_eq!(single("$1abc"), (t!("$param"), "$1abc"));
	assert_eq!(single("$0"), (t!("$param"), "$0"));
	assert_eq!(single("$_x"), (t!("$param"), "$_x"));
	// Delimited parameter names.
	assert_eq!(single("$\"weird name\""), (t!("$param"), "$\"weird name\""));
	assert_eq!(single("$`x`"), (t!("$param"), "$`x`"));
	assert_eq!(single("$@\"raw\""), (t!("$param"), "$@\"raw\""));
	// Substituted parameters.
	assert_eq!(single("$$sub"), (t!("$$param"), "$$sub"));
	assert_eq!(single("$$\"a b\""), (t!("$$param"), "$$\"a b\""));
	// The parameter ends where the name ends.
	assert_eq!(tokens("$a.b"), vec![t!("$param"), t!("."), TokenKind::Identifier]);
}

#[test]
fn parameter_names() {
	let name = |source| {
		let mut lexer = Lexer::new(source);
		let token = lexer.next_token();
		Lexer::parameter_name_span(lexer.span_str(token.span), token.span).expect("decoding failed")
	};
	assert_eq!(name("$x"), "x");
	assert_eq!(name("$1abc"), "1abc");
	assert_eq!(name("$$sub"), "sub");
	assert_eq!(name("$\"weird name\""), "weird name");
	assert_eq!(name("$`it``s`"), "it`s");
	assert_eq!(name(r#"$"a\nb""#), "a\nb");
	assert_eq!(name(r#"$@"a\nb""#), "a\\nb");
}

#[test]
fn parameter_errors() {
	let (_, error) = lex_error("$");
	assert!(render(&error, "$").contains("parameter name"));
	lex_error("$$");
	lex_error("$ x");
	lex_error("$+");
	lex_error("$@x");
	// Single-quoted strings are not valid delimited parameter names.
	lex_error("$'x'");
	// An unterminated delimited parameter name.
	lex_error("$\"abc");
}

#[test]
fn whitespace() {
	// The GQL whitespace set includes the no-break spaces and the C0
	// separators.
	for ws in [
		' ', '\t', '\n', '\u{000B}', '\u{000C}', '\r', '\u{001C}', '\u{001F}', '\u{00A0}',
		'\u{1680}', '\u{180E}', '\u{2000}', '\u{2007}', '\u{200A}', '\u{2028}', '\u{2029}',
		'\u{202F}', '\u{205F}', '\u{3000}',
	] {
		let source = format!("a{ws}b");
		assert_eq!(
			tokens(&source),
			vec![TokenKind::Identifier, TokenKind::Identifier],
			"U+{:04X}",
			ws as u32
		);
	}
	assert_eq!(tokens(""), vec![]);
	assert_eq!(tokens("   \t\n  "), vec![]);
}

#[test]
fn spans() {
	let source = "MATCH (a)-[k]->(b)";
	let mut lexer = Lexer::new(source);
	let expected = [
		(t!("MATCH"), 0, 5),
		(t!("("), 6, 1),
		(TokenKind::Identifier, 7, 1),
		(t!(")"), 8, 1),
		(t!("-["), 9, 2),
		(TokenKind::Identifier, 11, 1),
		(t!("]->"), 12, 3),
		(t!("("), 15, 1),
		(TokenKind::Identifier, 16, 1),
		(t!(")"), 17, 1),
	];
	for (kind, offset, len) in expected {
		let token = lexer.next_token();
		assert_eq!(token.kind, kind);
		assert_eq!(
			token.span,
			Span {
				offset,
				len,
			}
		);
	}
	let eof = lexer.next_token();
	assert!(eof.is_eof());
	assert_eq!(
		eof.span,
		Span {
			offset: 18,
			len: 0,
		}
	);
}

#[test]
fn unexpected_characters() {
	let (token, _) = lex_error(";");
	assert_eq!(
		token.span,
		Span {
			offset: 0,
			len: 1,
		}
	);
	lex_error("#");
	lex_error("^");
	lex_error("\\");
	// Unexpected non-ascii characters error rather than panic.
	let (token, _) = lex_error("\u{2603}");
	assert_eq!(
		token.span,
		Span {
			offset: 0,
			len: 3,
		}
	);
}

#[test]
fn lexing_resumes_after_error() {
	// The lexer keeps lexing after an invalid token.
	let mut lexer = Lexer::new("; MATCH");
	assert_eq!(lexer.next_token().kind, TokenKind::Invalid);
	assert!(lexer.error.take().is_some());
	assert_eq!(lexer.next_token().kind, t!("MATCH"));
}

#[test]
fn match_query_smoke() {
	// A representative v1 query lexes to the expected token sequence.
	let source = "MATCH (a:person)-[k:knows]->{1,3}(b) WHERE a.age >= $min RETURN DISTINCT a.name AS name ORDER BY name SKIP 1 LIMIT 2";
	assert_eq!(
		tokens(source),
		vec![
			t!("MATCH"),
			t!("("),
			TokenKind::Identifier,
			t!(":"),
			TokenKind::Identifier,
			t!(")"),
			t!("-["),
			TokenKind::Identifier,
			t!(":"),
			TokenKind::Identifier,
			t!("]->"),
			t!("{"),
			TokenKind::Number {
				kind: NumberKind::Integer,
				suffix: None,
			},
			t!(","),
			TokenKind::Number {
				kind: NumberKind::Integer,
				suffix: None,
			},
			t!("}"),
			t!("("),
			TokenKind::Identifier,
			t!(")"),
			t!("WHERE"),
			TokenKind::Identifier,
			t!("."),
			TokenKind::Identifier,
			t!(">="),
			t!("$param"),
			t!("RETURN"),
			t!("DISTINCT"),
			TokenKind::Identifier,
			t!("."),
			TokenKind::Identifier,
			t!("AS"),
			TokenKind::Identifier,
			t!("ORDER"),
			t!("BY"),
			TokenKind::Identifier,
			t!("SKIP"),
			TokenKind::Number {
				kind: NumberKind::Integer,
				suffix: None,
			},
			t!("LIMIT"),
			TokenKind::Number {
				kind: NumberKind::Integer,
				suffix: None,
			},
		]
	);
}

#[test]
fn keyword_variants_used_in_smoke() {
	// Direct Keyword variant checks for tokens the parser will dispatch on.
	assert_eq!(tokens("OFFSET"), vec![TokenKind::Keyword(Keyword::Offset)]);
	assert_eq!(tokens("SKIP"), vec![TokenKind::Keyword(Keyword::Skip)]);
	assert_eq!(tokens("IS"), vec![TokenKind::Keyword(Keyword::Is)]);
	assert_eq!(tokens("UNKNOWN"), vec![TokenKind::Keyword(Keyword::Unknown)]);
	assert_eq!(tokens("null"), vec![TokenKind::Keyword(Keyword::Null)]);
}
