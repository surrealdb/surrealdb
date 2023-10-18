use crate::syn::token::{t, TokenKind};

macro_rules! test_case(
	($source:expr => [$($token:expr),*$(,)?]) => {
		let mut lexer = crate::syn::lexer::Lexer::new($source);
		$(
			assert_eq!(lexer.next().map(|x| x.kind), Some($token));
		)*
		assert!(lexer.next().is_none())
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
           . .. ...

           ^
    "# => [
			t!("-"), t!("+"), t!("/"), t!("*"), t!("!"), t!("**"),

			t!("<"), t!(">"), t!("<="), t!(">="), t!("<-"), t!("<->"), t!("->"),

			t!("="), t!("=="), t!("-="), t!("+="), t!("!="), t!("+?="),

			t!("?"), t!("??"), t!("?:"), t!("?~"), t!("?="),

			t!("{"), t!("}"), t!("["), t!("]"), t!("("), t!(")"),

			t!(";"), t!(","), t!("|"), t!("||"), TokenKind::Invalid, t!("&&"),

			t!("."), t!(".."), t!("..."),

			TokenKind::Invalid
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
			t!("+"),
			t!("-"),
			t!("+"),
			t!("-"),
		]
	}
}

#[test]
fn whitespace() {
	test_case! {
		"+= \t\n\r -=" => [
			t!("+="),
			t!("-="),
		]
	}
}

#[test]
fn identifiers() {
	test_case! {
		r#"
			123123adwad +
			akdwkj +
			akdwkj1231312313123 +
			_a_k_d_wkj1231312313123 +
			____wdw____ +
		"#
			=> [
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Identifier,
			t!("+"),
			TokenKind::Identifier,
			t!("+"),
		]
	}
}

#[test]
fn numbers() {
	test_case! {
		r#"
			123123+3201023012300123.123012031230123123+33043030dec+33043030f+

			123129decs+39349fs+394393df+32932932def+329239329z
		"#
			=> [
			TokenKind::Number,
			t!("+"),
			TokenKind::Number,
			t!("+"),
			TokenKind::Number,
			t!("+"),
			TokenKind::Number,
			t!("+"),

			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
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
			TokenKind::Duration,
			t!("+"),
			TokenKind::Duration,
			t!("+"),
			TokenKind::Duration,
			t!("+"),
			TokenKind::Duration,
			t!("+"),
			TokenKind::Duration,
			t!("+"),
			TokenKind::Duration,
			t!("+"),
			TokenKind::Duration,
			t!("+"),
			TokenKind::Duration,
			t!("+"),
			TokenKind::Duration,

			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
			t!("+"),
			TokenKind::Invalid,
		]
	}
}

#[test]
fn keyword() {
	test_case! {
		r#"select SELECT sElEcT"# => [
			t!("SELECT"),
			t!("SELECT"),
			t!("SELECT"),
		]
	}
}
