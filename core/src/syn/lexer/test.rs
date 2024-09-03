use crate::syn::token::{t, DurationSuffix, TokenKind};

macro_rules! test_case(
	($source:expr => [$($token:expr),*$(,)?]) => {
		let mut lexer = crate::syn::lexer::Lexer::new($source.as_bytes());
		let mut i = 0;
		$(
			let next = lexer.next();
			if let Some(next) = next {
				let span = std::str::from_utf8(lexer.reader.span(next.span)).unwrap_or("invalid utf8");
				if let TokenKind::Invalid = next.kind{
					let error = lexer.error.take().unwrap();
					assert_eq!(next.kind, $token, "{} = {}:{} => {:?}",span, i, stringify!($token), error);
				}else{
					assert_eq!(next.kind, $token, "{} = {}:{}", span, i, stringify!($token));
				}
			}else{
				assert_eq!(next,None);
			}
			i += 1;
		)*
		let _ = i;
		assert_eq!(lexer.next(),None)
	};
);

#[test]
fn operators() {
	test_case! {
		r#"- + / * ! **
           < > <= >= <- <-> ->
           = == -= += != +?=
           ? ?? ?: ?~ ?=
           { } [ ] ( )
           ; , | || & &&
		   $
           . .. ...

           ^
    "# => [
			t!("-"), t!(" "), t!("+"),t!(" "),  t!("/"),t!(" "),  t!("*"),t!(" "),  t!("!"),t!(" "),  t!("**"), t!(" "),

			t!("<"), t!(" "), t!(">"), t!(" "), t!("<="), t!(" "), t!(">="), t!(" "), t!("<-"), t!(" "), t!("<->"), t!(" "), t!("->"), t!(" "),

			t!("="), t!(" "), t!("=="), t!(" "), t!("-="), t!(" "), t!("+="), t!(" "), t!("!="), t!(" "), t!("+?="), t!(" "),

			t!("?"), t!(" "), t!("??"), t!(" "), t!("?:"), t!(" "), t!("?~"), t!(" "), t!("?="), t!(" "),

			t!("{"), t!(" "), t!("}"), t!(" "), t!("["), t!(" "), t!("]"), t!(" "), t!("("), t!(" "), t!(")"), t!(" "),

			t!(";"), t!(" "), t!(","), t!(" "), t!("|"), t!(" "), t!("||"), t!(" "), TokenKind::Invalid, t!(" "), t!("&&"), t!(" "),

			t!("$"), t!(" "),

			t!("."), t!(" "), t!(".."), t!(" "), t!("..."), t!(" "),

			TokenKind::Invalid, t!(" ")
		]
	}
}

#[test]
fn comments() {
	test_case! {
		r"
			+ /* some comment */
			- // another comment
			+ -- a third comment
			-
		" => [
			t!(" "), t!("+"), t!(" "), t!(" "), t!(" "),
			t!("-"), t!(" "), t!(" "), t!(" "),
			t!("+"), t!(" "), t!(" "), t!(" "),
			t!("-"), t!(" ")
		]
	}
}

#[test]
fn whitespace() {
	test_case! {
		"+= \t\n\r -=" => [
			t!("+="), t!(" "),
			t!("-="),
		]
	}
}

#[test]
fn identifiers() {
	test_case! {
		r#"
			123123adwad+akdwkj+akdwkj1231312313123+_a_k_d_wkj1231312313123+____wdw____+
		"#
			=> [
			t!(" "),
			TokenKind::Digits, // 123123
			TokenKind::Identifier, // adwad
			t!("+"),
			TokenKind::Identifier, // akdwkj
			t!("+"),
			TokenKind::Identifier, // akdwkj1231312313123
			t!("+"),
			TokenKind::Identifier, // _a_k_d_wkj1231312313123
			t!("+"),
			TokenKind::Identifier, // ____wdw____
			t!("+"),
			t!(" "),
		]
	}
}

#[test]
fn numbers() {
	test_case! {
		r#"123123+32010230.123012031+33043030dec+33043030f+303e10dec+"#
			=> [
			TokenKind::Digits, // 123123
			t!("+"),
			TokenKind::Digits, // 32010230
			t!("."),
			TokenKind::Digits, // 123012031
			t!("+"),
			TokenKind::Digits, // 33043030
			t!("dec"),
			t!("+"),
			TokenKind::Digits, // 33043030
			t!("f"),
			t!("+"),
			TokenKind::Digits, // 303
			TokenKind::Exponent , // e
			TokenKind::Digits, // 10
			t!("dec"),
			t!("+"),
		]
	}

	test_case! {
		"+123129decs+"
			=> [
				t!("+"),
				TokenKind::Digits, // 123129
				TokenKind::Identifier, // decs
				t!("+"),
			]
	}

	test_case! {
		"+39349fs+"
			=> [
				t!("+"),
				TokenKind::Digits, // 39349
				TokenKind::Identifier, // fs
				t!("+"),
			]
	}

	test_case! {
		"+394393df+"
			=> [
				t!("+"),
				TokenKind::Digits, // 39349
				TokenKind::Identifier, // df
				t!("+"),
			]
	}

	test_case! {
		"+32932932def+"
			=> [
				t!("+"),
				TokenKind::Digits, // 32932932
				TokenKind::Identifier, // def
				t!("+"),
			]
	}

	test_case! {
		"+329239329z+"
			=> [
				t!("+"),
				TokenKind::Digits, // 329239329
				TokenKind::Identifier, // z
				t!("+"),
			]
	}
}

#[test]
fn duration() {
	test_case! {
		r#"
			1ns+1µs+1us+1ms+1s+1m+1h+1w+1y

			1nsa+1ans+1aus+1usa+1ams+1msa+1am+1ma+1ah+1ha+1aw+1wa+1ay+1ya+1µsa
		"#
			=> [
			t!(" "),
			TokenKind::Digits,
			TokenKind::DurationSuffix(DurationSuffix::Nano),
			t!("+"),
			TokenKind::Digits,
			TokenKind::DurationSuffix(DurationSuffix::MicroUnicode),
			t!("+"),
			TokenKind::Digits,
			TokenKind::DurationSuffix(DurationSuffix::Micro),
			t!("+"),
			TokenKind::Digits,
			TokenKind::DurationSuffix(DurationSuffix::Milli),
			t!("+"),
			TokenKind::Digits,
			TokenKind::DurationSuffix(DurationSuffix::Second),
			t!("+"),
			TokenKind::Digits,
			TokenKind::DurationSuffix(DurationSuffix::Minute),
			t!("+"),
			TokenKind::Digits,
			TokenKind::DurationSuffix(DurationSuffix::Hour),
			t!("+"),
			TokenKind::Digits,
			TokenKind::DurationSuffix(DurationSuffix::Week),
			t!("+"),
			TokenKind::Digits,
			TokenKind::DurationSuffix(DurationSuffix::Year),

			t!(" "),

			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Digits,
			TokenKind::Invalid,
			TokenKind::Identifier,
			t!(" "),
		]
	}
}

#[test]
fn keyword() {
	test_case! {
		r#"select SELECT sElEcT"# => [
			t!("SELECT"),t!(" "),
			t!("SELECT"),t!(" "),
			t!("SELECT"),
		]
	}
}
