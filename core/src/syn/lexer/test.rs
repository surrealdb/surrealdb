use chrono::{FixedOffset, NaiveDate, Offset, TimeZone, Utc};

use crate::syn::token::{t, NumberKind, TokenKind};

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
					assert_eq!(next.kind, $token, "{} = {}:{} => {}",span, i, stringify!($token), error);
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
			t!("-"), t!("+"), t!("/"), t!("*"), t!("!"), t!("**"),

			t!("<"), t!(">"), t!("<="), t!(">="), t!("<-"), t!("<->"), t!("->"),

			t!("="), t!("=="), t!("-="), t!("+="), t!("!="), t!("+?="),

			t!("?"), t!("??"), t!("?:"), t!("?~"), t!("?="),

			t!("{"), t!("}"), t!("["), t!("]"), t!("("), t!(")"),

			t!(";"), t!(","), t!("|"), t!("||"), TokenKind::Invalid, t!("&&"),

			t!("$"),

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
			123123+32010230.123012031+33043030dec+33043030f+303e10dec+

		"#
			=> [
			TokenKind::Number(NumberKind::Integer),
			t!("+"),
			TokenKind::Number(NumberKind::Mantissa),
			t!("+"),
			TokenKind::Number(NumberKind::Decimal),
			t!("+"),
			TokenKind::Number(NumberKind::Float),
			t!("+"),
			TokenKind::Number(NumberKind::DecimalExponent),
			t!("+"),
		]
	}

	test_case! {
		"+123129decs+"
			=> [
				t!("+"),
				TokenKind::Invalid,
				t!("+"),
			]
	}

	test_case! {
		"+39349fs+"
			=> [
				t!("+"),
				TokenKind::Invalid,
				t!("+"),
			]
	}

	test_case! {
		"+394393df+"
			=> [
				t!("+"),
				TokenKind::Invalid,
				t!("+"),
			]
	}

	test_case! {
		"+32932932def+"
			=> [
				t!("+"),
				TokenKind::Invalid,
				t!("+"),
			]
	}

	test_case! {
		"+329239329z+"
			=> [
				t!("+"),
				TokenKind::Invalid,
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
	let mut lexer =
		crate::syn::lexer::Lexer::new(r#" u"e72bee20-f49b-11ec-b939-0242ac120002" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {} @ ", error);
	}
	assert_eq!(token.kind, TokenKind::Uuid);
	let uuid = lexer.uuid.take().unwrap();
	assert_eq!(uuid.0.to_string(), "e72bee20-f49b-11ec-b939-0242ac120002");

	let mut lexer =
		crate::syn::lexer::Lexer::new(r#" u"b19bc00b-aa98-486c-ae37-c8e1c54295b1" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {} @ ", error);
	}
	assert_eq!(token.kind, TokenKind::Uuid);
	let uuid = lexer.uuid.take().unwrap();
	assert_eq!(uuid.0.to_string(), "b19bc00b-aa98-486c-ae37-c8e1c54295b1");
}

#[test]
fn date_time_just_date() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" d"2012-04-23" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {} @ ", error);
	}
	assert_eq!(token.kind, TokenKind::DateTime);
	let datetime = lexer.datetime.take().unwrap();
	let expected_datetime = Utc
		.fix()
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2012, 4, 23).unwrap().and_hms_nano_opt(0, 0, 0, 0).unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);

	assert_eq!(datetime.0, expected_datetime);
}

#[test]
fn date_zone_time() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" d"2020-01-01T00:00:00Z" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {} @ ", error);
	}
	assert_eq!(token.kind, TokenKind::DateTime);
	let datetime = lexer.datetime.take().unwrap();
	let expected_datetime = Utc
		.fix()
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap().and_hms_nano_opt(0, 0, 0, 0).unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);

	assert_eq!(datetime.0, expected_datetime);
}

#[test]
fn date_time_with_time() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" d"2012-04-23T18:25:43Z" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {} @ ", error);
	}
	assert_eq!(token.kind, TokenKind::DateTime);
	let datetime = lexer.datetime.take().unwrap();
	let expected_datetime = Utc
		.fix()
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2012, 4, 23).unwrap().and_hms_nano_opt(18, 25, 43, 0).unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);

	assert_eq!(datetime.0, expected_datetime);
}

#[test]
fn date_time_nanos() {
	let mut lexer = crate::syn::lexer::Lexer::new(r#" d"2012-04-23T18:25:43.5631Z" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {} @ ", error);
	}
	assert_eq!(token.kind, TokenKind::DateTime);
	let datetime = lexer.datetime.take().unwrap();
	let expected_datetime = Utc
		.fix()
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2012, 4, 23)
				.unwrap()
				.and_hms_nano_opt(18, 25, 43, 563_100_000)
				.unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);
	assert_eq!(datetime.0, expected_datetime);
}

#[test]
fn date_time_timezone_utc() {
	let mut lexer =
		crate::syn::lexer::Lexer::new(r#" d"2012-04-23T18:25:43.0000511Z" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {}", error);
	}
	assert_eq!(token.kind, TokenKind::DateTime);
	let datetime = lexer.datetime.take().unwrap();
	let expected_datetime = Utc
		.fix()
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2012, 4, 23)
				.unwrap()
				.and_hms_nano_opt(18, 25, 43, 51_100)
				.unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);
	assert_eq!(datetime.0, expected_datetime);
}

#[test]
fn date_time_timezone_pacific() {
	let mut lexer =
		crate::syn::lexer::Lexer::new(r#" d"2012-04-23T18:25:43.511-08:00" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {}", error);
	}
	assert_eq!(token.kind, TokenKind::DateTime);
	let datetime = lexer.datetime.take().unwrap();
	let offset = FixedOffset::west_opt(8 * 3600).unwrap();
	let expected_datetime = offset
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2012, 4, 23)
				.unwrap()
				.and_hms_nano_opt(18, 25, 43, 511_000_000)
				.unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);
	assert_eq!(datetime.0, expected_datetime);
}

#[test]
fn date_time_timezone_pacific_partial() {
	let mut lexer =
		crate::syn::lexer::Lexer::new(r#" d"2012-04-23T18:25:43.511+08:30" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {}", error);
	}
	assert_eq!(token.kind, TokenKind::DateTime);
	let datetime = lexer.datetime.take().unwrap();
	let offset = FixedOffset::east_opt(8 * 3600 + 30 * 60).unwrap();
	let expected_datetime = offset
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2012, 4, 23)
				.unwrap()
				.and_hms_nano_opt(18, 25, 43, 511_000_000)
				.unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);
	assert_eq!(datetime.0, expected_datetime);
}

#[test]
fn date_time_timezone_utc_nanoseconds() {
	let mut lexer =
		crate::syn::lexer::Lexer::new(r#" d"2012-04-23T18:25:43.5110000Z" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {}", error);
	}
	assert_eq!(token.kind, TokenKind::DateTime);
	let datetime = lexer.datetime.take().unwrap();
	let offset = Utc.fix();
	let expected_datetime = offset
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2012, 4, 23)
				.unwrap()
				.and_hms_nano_opt(18, 25, 43, 511_000_000)
				.unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);
	assert_eq!(datetime.0, expected_datetime);
}

#[test]
fn date_time_timezone_utc_sub_nanoseconds() {
	let mut lexer =
		crate::syn::lexer::Lexer::new(r#" d"2012-04-23T18:25:43.0000511Z" "#.as_bytes());
	let token = lexer.next_token();
	if let Some(error) = lexer.error {
		println!("ERROR: {}", error);
	}
	assert_eq!(token.kind, TokenKind::DateTime);
	let datetime = lexer.datetime.take().unwrap();
	let offset = Utc.fix();
	let expected_datetime = offset
		.from_local_datetime(
			&NaiveDate::from_ymd_opt(2012, 4, 23)
				.unwrap()
				.and_hms_nano_opt(18, 25, 43, 51_100)
				.unwrap(),
		)
		.earliest()
		.unwrap()
		.with_timezone(&Utc);
	assert_eq!(datetime.0, expected_datetime);
}
