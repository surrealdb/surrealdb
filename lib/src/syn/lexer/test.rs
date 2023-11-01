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

#[test]
fn uuid() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" u"e72bee20-f49b-11ec-b939-0242ac120002" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::Uuid);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"e72bee20-f49b-11ec-b939-0242ac120002"
	);

	let mut lexer = crate::syn::lexer::Lexer::new(r#" u"b19bc00b-aa98-486c-ae37-c8e1c54295b1" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::Uuid);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"b19bc00b-aa98-486c-ae37-c8e1c54295b1"
	);
}

#[test]
fn date_zone() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" t"2020-01-01T00:00:00Z" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::DateTime);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"2020-01-01T00:00:00Z"
	);
}

#[test]
fn date_time() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" t"2012-04-23T18:25:43Z" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::DateTime);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"2012-04-23T18:25:43Z"
	);
}

#[test]
fn date_time_nanos() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" t"2012-04-23T18:25:43.5631Z" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::DateTime);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"2012-04-23T18:25:43.5631Z"
	);
}

#[test]
fn date_time_timezone_utc() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" t"2012-04-23T18:25:43.0000511Z" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::DateTime);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"2012-04-23T18:25:43.0000511Z"
	);
}

#[test]
fn date_time_timezone_pacific() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" t"2012-04-23T18:25:43.511-08:00" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::DateTime);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"2012-04-24T02:25:43.511-08:00"
	);
}

#[test]
fn date_time_timezone_pacific_partial() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" t"2012-04-23T18:25:43.511-08:30" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::DateTime);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"2012-04-24T02:55:43.511-08:30"
	);
}

#[test]
fn date_time_timezone_utc_nanoseconds() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" t"2012-04-23T18:25:43.5110000Z" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::DateTime);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"2012-04-23T18:25:43.5110000Z"
	);
}

#[test]
fn date_time_timezone_utc_sub_nanoseconds() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" t"2012-04-23T18:25:43.0000511Z" "#);
	let token = lexer.next_token();
	assert_eq!(token.kind, TokenKind::DateTime);
	assert_eq!(
		lexer.strings[u32::from(token.data_index.unwrap()) as usize],
		"2012-04-23T18:25:43.000051100Z"
	);
}
