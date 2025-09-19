use std::borrow::Cow;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;
use std::time::Duration;

use rust_decimal::Decimal;

use crate::syn::error::{SyntaxError, bail, syntax_error};
use crate::syn::lexer::Lexer;
use crate::syn::token::{Span, Token, TokenKind, t};
use crate::val::DecimalExt;
use crate::val::duration::{
	SECONDS_PER_DAY, SECONDS_PER_HOUR, SECONDS_PER_MINUTE, SECONDS_PER_WEEK, SECONDS_PER_YEAR,
};

pub enum Numeric {
	Float(f64),
	Integer(i64),
	Decimal(Decimal),
	Duration(Duration),
}

/// Like numeric but holds of parsing the a number into a specific value.
#[derive(Debug)]
pub enum NumericKind {
	Float,
	Int,
	Decimal,
	Duration(Duration),
}

#[derive(Debug)]
pub enum NumberKind {
	Integer,
	Float,
	Decimal,
}

enum DurationSuffix {
	Nano,
	Micro,
	Milli,
	Second,
	Minute,
	Hour,
	Day,
	Week,
	Year,
}

fn prepare_number_str(str: &str) -> Cow<'_, str> {
	if str.contains('_') {
		Cow::Owned(str.chars().filter(|x| *x != '_').collect())
	} else {
		Cow::Borrowed(str)
	}
}

/// Tokens which can start with digits: Number or Duration.
/// Like numeric but holds off on parsing the a number into a specific value.
pub fn numeric_kind(lexer: &mut Lexer, start: Token) -> Result<NumericKind, SyntaxError> {
	match start.kind {
		t!("-") | t!("+") => match number_kind(lexer, start)? {
			NumberKind::Integer => Ok(NumericKind::Int),
			NumberKind::Float => Ok(NumericKind::Float),
			NumberKind::Decimal => Ok(NumericKind::Decimal),
		},
		TokenKind::Digits => match lexer.reader.peek() {
			Some(b'n' | b's' | b'm' | b'h' | b'y' | b'w' | b'u') => {
				duration(lexer, start).map(NumericKind::Duration)
			}
			Some(b'd') => {
				if let Some(b'e') = lexer.reader.peek1() {
					match number_kind(lexer, start)? {
						NumberKind::Integer => Ok(NumericKind::Int),
						NumberKind::Float => Ok(NumericKind::Float),
						NumberKind::Decimal => Ok(NumericKind::Decimal),
					}
				} else {
					duration(lexer, start).map(NumericKind::Duration)
				}
			}
			Some(x) if !x.is_ascii() => duration(lexer, start).map(NumericKind::Duration),
			_ => match number_kind(lexer, start)? {
				NumberKind::Integer => Ok(NumericKind::Int),
				NumberKind::Float => Ok(NumericKind::Float),
				NumberKind::Decimal => Ok(NumericKind::Decimal),
			},
		},
		x => {
			bail!("Unexpected token `{x}`, expected a numeric value, either a duration or number",@start.span)
		}
	}
}

/// Tokens which can start with digits: Number or Duration.
pub fn numeric(lexer: &mut Lexer, start: Token) -> Result<Numeric, SyntaxError> {
	match start.kind {
		t!("-") | t!("+") => number(lexer, start),
		TokenKind::Digits => match lexer.reader.peek() {
			Some(b'n' | b's' | b'm' | b'h' | b'y' | b'w' | b'u') => {
				duration(lexer, start).map(Numeric::Duration)
			}
			Some(b'd') => {
				if lexer.reader.peek1() == Some(b'e') {
					number(lexer, start)
				} else {
					duration(lexer, start).map(Numeric::Duration)
				}
			}
			Some(x) if !x.is_ascii() => duration(lexer, start).map(Numeric::Duration),
			_ => number(lexer, start),
		},
		x => {
			bail!("Unexpected token `{x}`, expected a numeric value, either a duration or number",@start.span)
		}
	}
}

pub fn number_kind(lexer: &mut Lexer, start: Token) -> Result<NumberKind, SyntaxError> {
	let offset = start.span.offset as usize;
	match start.kind {
		t!("-") | t!("+") => {
			eat_digits1(lexer, offset)?;
		}
		TokenKind::Digits => {}
		x => bail!("Unexpected start token for integer: {x}",@start.span),
	}

	let mut kind = NumberKind::Integer;

	let before_mantissa = lexer.reader.offset();
	// need to test for digit.. or digit.foo
	if lexer.reader.peek1().map(|x| x.is_ascii_digit()).unwrap_or(false) && lexer.eat(b'.') {
		eat_digits1(lexer, before_mantissa)?;
		kind = NumberKind::Float;
	}

	let before_exponent = lexer.reader.offset();
	if lexer.eat(b'e') || lexer.eat(b'E') {
		if !lexer.eat(b'-') {
			lexer.eat(b'+');
		}

		eat_digits1(lexer, before_exponent)?;
		kind = NumberKind::Float;
	}

	if !lexer.eat(b'f') {
		if lexer.eat(b'd') {
			lexer.expect('e')?;
			lexer.expect('c')?;
			kind = NumberKind::Decimal;
		}
	} else {
		kind = NumberKind::Float;
	}

	if has_ident_after(lexer) {
		let char = lexer.reader.next().unwrap();
		let char = lexer.reader.convert_to_char(char)?;
		bail!("Invalid token, found unexpected character `{char}` after number token", @lexer.current_span())
	}
	Ok(kind)
}

pub fn number(lexer: &mut Lexer, start: Token) -> Result<Numeric, SyntaxError> {
	let kind = number_kind(lexer, start)?;
	let span = lexer.current_span();
	let number_str = prepare_number_str(lexer.span_str(span));
	match kind {
		NumberKind::Integer => number_str
			.parse()
			.map(Numeric::Integer)
			.map_err(|e| syntax_error!("Failed to parse number: {e}", @lexer.current_span())),
		NumberKind::Float => {
			let number_str = number_str.trim_end_matches('f');
			number_str
				.parse()
				.map(Numeric::Float)
				.map_err(|e| syntax_error!("Failed to parse number: {e}", @lexer.current_span()))
		}
		NumberKind::Decimal => {
			let number_str = number_str.trim_end_matches("dec");
			let decimal = if number_str.contains(['e', 'E']) {
				Decimal::from_scientific(number_str).map_err(
					|e| syntax_error!("Failed to parser decimal: {e}", @lexer.current_span()),
				)?
			} else {
				Decimal::from_str_normalized(number_str).map_err(
					|e| syntax_error!("Failed to parser decimal: {e}", @lexer.current_span()),
				)?
			};
			Ok(Numeric::Decimal(decimal))
		}
	}
}

/// Generic integer parsing method,
/// works for all unsigned integers.
pub fn integer<I>(lexer: &mut Lexer, start: Token) -> Result<I, SyntaxError>
where
	I: FromStr<Err = ParseIntError>,
{
	let offset = start.span.offset as usize;
	match start.kind {
		t!("-") | t!("+") => {
			eat_digits1(lexer, offset)?;
		}
		TokenKind::Digits => {}
		x => bail!("Unexpected token {x}, expected integer",@start.span),
	};

	if has_ident_after(lexer) {
		let char = lexer.reader.next().unwrap();
		let char = lexer.reader.convert_to_char(char)?;
		bail!("Invalid token, found unexpected character `{char} after integer token", @lexer.current_span())
	}

	let last_offset = lexer.reader.offset();
	let peek = lexer.reader.peek();
	if peek == Some(b'.') {
		let is_mantissa = lexer.reader.peek1().map(|x| x.is_ascii_digit()).unwrap_or(false);
		if is_mantissa {
			let span = Span {
				offset: last_offset as u32,
				len: 1,
			};
			bail!("Unexpected character `.` starting float, only integers are allowed here", @span)
		}
	}

	if peek == Some(b'e') || peek == Some(b'E') {
		bail!("Unexpected character `{}` only integers are allowed here",peek.unwrap() as char, @lexer.current_span())
	}

	let span = lexer.current_span();
	let str = prepare_number_str(lexer.span_str(span));
	str.parse().map_err(|e| syntax_error!("Invalid integer: {e}", @span))
}

/// Generic integer parsing method,
/// works for all unsigned integers.
pub fn float<I>(lexer: &mut Lexer, start: Token) -> Result<I, SyntaxError>
where
	I: FromStr<Err = ParseFloatError>,
{
	let offset = start.span.offset as usize;
	match start.kind {
		t!("-") | t!("+") => {
			eat_digits1(lexer, offset)?;
		}
		TokenKind::Digits => {}
		x => bail!("Unexpected token {x}, expected floating point number",@start.span),
	};

	let before_mantissa = lexer.reader.offset();
	if lexer.eat(b'.') {
		eat_digits1(lexer, before_mantissa)?;
	}

	let before_exponent = lexer.reader.offset();
	if lexer.eat(b'e') || lexer.eat(b'E') {
		if !lexer.eat(b'-') {
			lexer.eat(b'+');
		}

		eat_digits1(lexer, before_exponent)?;
	}

	let number_span = lexer.current_span();

	lexer.eat(b'f');

	if has_ident_after(lexer) {
		let char = lexer.reader.next().unwrap();
		let char = lexer.reader.convert_to_char(char)?;
		bail!("Invalid token, found invalid character `{char}` after number token", @lexer.current_span())
	}

	let str = prepare_number_str(lexer.span_str(number_span));
	str.parse()
		.map_err(|e| syntax_error!("Invalid floating point number: {e}", @lexer.current_span()))
}

pub fn duration(lexer: &mut Lexer, start: Token) -> Result<Duration, SyntaxError> {
	match start.kind {
		TokenKind::Digits => {}
		x => bail!("Unexpected token {x}, expected duration", @start.span),
	}

	let mut duration = Duration::ZERO;

	let mut number_span = start.span;
	loop {
		let suffix = lex_duration_suffix(lexer)?;

		let numeric_string = prepare_number_str(lexer.span_str(number_span));
		let numeric_value: u64 = numeric_string.parse().map_err(
			|e| syntax_error!("Invalid token, failed to parse duration digits: {e}",@lexer.current_span()),
		)?;

		let addition = match suffix {
			DurationSuffix::Nano => Duration::from_nanos(numeric_value),
			DurationSuffix::Micro => Duration::from_micros(numeric_value),
			DurationSuffix::Milli => Duration::from_millis(numeric_value),
			DurationSuffix::Second => Duration::from_secs(numeric_value),
			DurationSuffix::Minute => {
				let minutes = numeric_value.checked_mul(SECONDS_PER_MINUTE).ok_or_else(
					|| syntax_error!("Invalid duration, value overflowed maximum allowed value", @lexer.current_span()),
				)?;
				Duration::from_secs(minutes)
			}
			DurationSuffix::Hour => {
				let hours = numeric_value.checked_mul(SECONDS_PER_HOUR).ok_or_else(
					|| syntax_error!("Invalid duration, value overflowed maximum allowed value", @lexer.current_span()),
				)?;
				Duration::from_secs(hours)
			}
			DurationSuffix::Day => {
				let day = numeric_value.checked_mul(SECONDS_PER_DAY).ok_or_else(
					|| syntax_error!("Invalid duration, value overflowed maximum allowed value", @lexer.current_span()),
				)?;
				Duration::from_secs(day)
			}
			DurationSuffix::Week => {
				let week = numeric_value.checked_mul(SECONDS_PER_WEEK).ok_or_else(
					|| syntax_error!("Invalid duration, value overflowed maximum allowed value", @lexer.current_span()),
				)?;
				Duration::from_secs(week)
			}
			DurationSuffix::Year => {
				let year = numeric_value.checked_mul(SECONDS_PER_YEAR).ok_or_else(
					|| syntax_error!("Invalid duration, value overflowed maximum allowed value", @lexer.current_span()),
				)?;
				Duration::from_secs(year)
			}
		};

		duration = duration.checked_add(addition).ok_or_else(
			|| syntax_error!("Invalid duration, value overflowed maximum allowed value", @lexer.current_span()),
		)?;

		match lexer.reader.peek() {
			Some(x) if x.is_ascii_digit() => {
				let before = lexer.reader.offset();
				eat_digits(lexer);
				number_span = lexer.span_since(before);
			}
			_ => break,
		}
	}

	Ok(duration)
}

fn lex_duration_suffix(lexer: &mut Lexer) -> Result<DurationSuffix, SyntaxError> {
	let suffix = match lexer.reader.next() {
		Some(b'n') => {
			lexer.expect('s')?;
			DurationSuffix::Nano
		}
		Some(b'u') => {
			lexer.expect('s')?;
			DurationSuffix::Micro
		}
		Some(b'm') => {
			if lexer.eat(b's') {
				DurationSuffix::Milli
			} else {
				DurationSuffix::Minute
			}
		}
		Some(b's') => DurationSuffix::Second,
		Some(b'h') => DurationSuffix::Hour,
		Some(b'd') => DurationSuffix::Day,
		Some(b'w') => DurationSuffix::Week,
		Some(b'y') => DurationSuffix::Year,
		// Start byte of 'µ'
		Some(0xC2) => {
			if !lexer.eat(0xB5) {
				let char = lexer.reader.complete_char(0xC2)?;
				bail!("Invalid duration token, expected a duration suffix found `{char}`",@lexer.current_span())
			}
			lexer.expect('s')?;
			DurationSuffix::Micro
		}
		Some(x) => {
			let char = lexer.reader.convert_to_char(x)?;
			bail!("Invalid duration token, expected a duration suffix found `{char}`",@lexer.current_span())
		}
		None => {
			bail!("Unexpected end of file, expected a duration suffix",@lexer.current_span())
		}
	};

	if has_ident_after(lexer) {
		let char = lexer.reader.next().unwrap();
		let char = lexer.reader.convert_to_char(char)?;
		bail!("Invalid token, found invalid character `{char}` after duration suffix", @lexer.current_span())
	}

	Ok(suffix)
}

fn has_ident_after(lexer: &mut Lexer) -> bool {
	match lexer.reader.peek() {
		Some(x) => !x.is_ascii() || x.is_ascii_alphabetic(),
		None => false,
	}
}

fn eat_digits1(lexer: &mut Lexer, start: usize) -> Result<(), SyntaxError> {
	match lexer.reader.peek() {
		Some(x) if x.is_ascii_digit() => {}
		Some(x) => {
			let char = lexer.reader.convert_to_char(x)?;
			bail!("Invalid number token, expected a digit, found: {char}", @lexer.span_since(start));
		}
		None => {
			bail!("Unexpected end of file, expected a number token digit", @lexer.span_since(start));
		}
	}

	eat_digits(lexer);
	Ok(())
}

fn eat_digits(lexer: &mut Lexer) {
	while lexer.eat_when(|x| x.is_ascii_digit() || x == b'_') {}
}
