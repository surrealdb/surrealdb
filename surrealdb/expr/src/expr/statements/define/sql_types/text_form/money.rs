//! The text form of the PostgreSQL `money` type: PostgreSQL's default `en_US` rendering, a
//! currency symbol, thousands separators, and two fractional digits, with the minus sign before
//! the symbol for a negative amount (`$1,234.56`, `-$1,234.56`). An amount is held in cents.

/// The text form of an amount given in cents, as PostgreSQL's `en_US` locale prints it.
pub fn money_text(cents: i64) -> String {
	let magnitude = cents.unsigned_abs();
	let dollars = (magnitude / 100).to_string();
	let fraction = magnitude % 100;
	let mut grouped = String::with_capacity(dollars.len() + dollars.len() / 3);
	for (index, ch) in dollars.chars().enumerate() {
		if index > 0 && (dollars.len() - index).is_multiple_of(3) {
			grouped.push(',');
		}
		grouped.push(ch);
	}
	let sign = if cents < 0 {
		"-"
	} else {
		""
	};
	format!("{sign}${grouped}.{fraction:02}")
}

/// Read the text form of a `money` value into cents. The accepted forms are an optional minus
/// sign, an optional `$` before or after that sign, digits with `,` thousands separators in
/// groups of three, and an optional fraction. A fraction beyond two digits is rounded half away
/// from zero, as the type rounds on write. `None` when the text has another shape or the amount
/// does not fit an `int8` of cents.
pub fn parse_money_text(text: &str) -> Option<i64> {
	let mut body = text.trim();
	let mut negative = false;
	if let Some(rest) = body.strip_prefix('-') {
		negative = true;
		body = rest;
	}
	body = body.strip_prefix('$').unwrap_or(body);
	if let Some(rest) = body.strip_prefix('-') {
		if negative {
			return None;
		}
		negative = true;
		body = rest;
	}

	let (whole, fraction) = match body.split_once('.') {
		Some((whole, fraction)) => (whole, fraction),
		None => (body, ""),
	};
	if whole.is_empty() && fraction.is_empty() {
		return None;
	}
	let dollars = if whole.is_empty() {
		0
	} else {
		parse_grouped_dollars(whole)?
	};
	if !fraction.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	let mut digits = fraction.bytes().map(|b| i128::from(b - b'0'));
	let tens = digits.next().unwrap_or(0);
	let units = digits.next().unwrap_or(0);
	let round_up = i128::from(digits.next().unwrap_or(0) >= 5);
	let magnitude = dollars.checked_mul(100)? + tens * 10 + units + round_up;
	let total = if negative {
		-magnitude
	} else {
		magnitude
	};
	i64::try_from(total).ok()
}

/// The whole-dollar digits of a money text, with `,` separators in groups of three after the
/// first group. `None` for an empty or misgrouped digit string.
fn parse_grouped_dollars(whole: &str) -> Option<i128> {
	let grouped = whole.contains(',');
	let mut digits = String::with_capacity(whole.len());
	for (index, group) in whole.split(',').enumerate() {
		let valid_length = if !grouped {
			!group.is_empty()
		} else if index == 0 {
			(1..=3).contains(&group.len())
		} else {
			group.len() == 3
		};
		if !valid_length || !group.bytes().all(|b| b.is_ascii_digit()) {
			return None;
		}
		digits.push_str(group);
	}
	digits.parse::<i128>().ok()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn text_uses_currency_symbol_and_grouping() {
		assert_eq!(money_text(123_456), "$1,234.56");
		assert_eq!(money_text(-123_456), "-$1,234.56");
		assert_eq!(money_text(0), "$0.00");
		assert_eq!(money_text(5), "$0.05");
		assert_eq!(money_text(100_000_000), "$1,000,000.00");
		assert_eq!(money_text(i64::MIN), "-$92,233,720,368,547,758.08");
		assert_eq!(money_text(i64::MAX), "$92,233,720,368,547,758.07");
	}

	#[test]
	fn text_reads_the_forms_it_writes() {
		for cents in [0, 1, -1, 123_456, -123_456, i64::MIN, i64::MAX] {
			assert_eq!(parse_money_text(&money_text(cents)), Some(cents), "{cents}");
		}
	}

	#[test]
	fn text_reads_the_accepted_shapes() {
		assert_eq!(parse_money_text("$1,234.56"), Some(123_456));
		assert_eq!(parse_money_text("-$1,234.56"), Some(-123_456));
		assert_eq!(parse_money_text("$-1,234.56"), Some(-123_456));
		assert_eq!(parse_money_text("1234.5"), Some(123_450));
		assert_eq!(parse_money_text("  $7  "), Some(700));
		assert_eq!(parse_money_text("$.5"), Some(50));
		assert_eq!(parse_money_text("$1,00"), None);
		assert_eq!(parse_money_text(",100"), None);
		assert_eq!(parse_money_text("1,2345"), None);
		assert_eq!(parse_money_text("--$1"), None);
		assert_eq!(parse_money_text("$-$1"), None);
		assert_eq!(parse_money_text("$"), None);
		assert_eq!(parse_money_text("$1.2x"), None);
		assert_eq!(parse_money_text("(1.00)"), None);
	}

	#[test]
	fn text_rounds_a_third_fractional_digit_half_away_from_zero() {
		assert_eq!(parse_money_text("$0.005"), Some(1));
		assert_eq!(parse_money_text("$0.0049999"), Some(0));
		assert_eq!(parse_money_text("-$0.005"), Some(-1));
		assert_eq!(parse_money_text("$1.009"), Some(101));
	}

	#[test]
	fn text_refuses_an_amount_beyond_int8_cents() {
		assert_eq!(parse_money_text("$92,233,720,368,547,758.07"), Some(i64::MAX));
		assert_eq!(parse_money_text("$92,233,720,368,547,758.08"), None);
		assert_eq!(parse_money_text("-$92,233,720,368,547,758.08"), Some(i64::MIN));
		assert_eq!(parse_money_text("$99999999999999999999999999999999999999999"), None);
	}
}
