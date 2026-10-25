//! Lowering of the pattern of `LIKE` and `ILIKE` when it is known only at run time: a bound
//! parameter, a column, or an expression over either.
//!
//! A pattern written as a literal is converted to a regular expression while the statement is
//! lowered, by [`sql_like_to_regex`](crate::semantics::sql_like_to_regex). [`like_pattern_regex`]
//! builds the same text for any other pattern from calls of `string::replace`, so that both forms
//! of a pattern match alike. The pattern is never read as a regular expression itself: every
//! character that means something to one is escaped, so a pattern can neither inject regular
//! expression syntax nor fail to compile for what it says. What can fail is the size of the
//! compiled expression, which the engine bounds (`SURREAL_REGEX_SIZE_LIMIT`), and the size of a
//! replacement, which `string::replace` bounds (`SURREAL_GENERATION_ALLOCATION_LIMIT`); both
//! raise an error.

use std::str::FromStr;
use std::sync::LazyLock;

use surrealdb_expr::expr::Function;
use surrealdb_expr::expr::logical::{LogicalCall, LogicalExpr, LogicalIf, LogicalLiteral};
use surrealdb_expr::val::Regex;
use surrealdb_strand::Strand;
use surrealdb_types::regex_engine::RegexError;

use crate::error::PlanError;

/// A noncharacter, which Unicode reserves for a program's own use. It opens a mark of two
/// characters that stands in for text a later step must not read as pattern syntax. A pattern
/// holding the noncharacter itself has it replaced by [`ESCAPED_MARK`] first, so no mark is
/// mistaken for text and no text for a mark.
const MARK: &str = "\u{fdd0}";
const ESCAPED_MARK: &str = "\u{fdd0}0";
const BACKSLASH: &str = "\u{fdd0}1";
const PERCENT: &str = "\u{fdd0}2";
const UNDERSCORE: &str = "\u{fdd0}3";

/// The characters a regular expression reads as syntax outside a bracketed class, other than the
/// backslash. Each is escaped with a backslash.
const REGEX_SYNTAX: [&str; 13] = [".", "^", "$", "*", "+", "?", "(", ")", "[", "]", "{", "}", "|"];

/// Matches a pattern that needs only its wildcards converted: one with no backslash, no
/// character a regular expression reads as syntax and no noncharacter of the chain.
static PLAIN_PATTERN: LazyLock<Result<Regex, RegexError>> =
	LazyLock::new(|| Regex::from_str(&format!(r"^[^\\.^$*+?()\[\]{{}}|{MARK}]*$")));

/// Matches a backslash that ends the text.
static TRAILING_BACKSLASH: LazyLock<Result<Regex, RegexError>> =
	LazyLock::new(|| Regex::from_str(r"\\$"));

/// A copy of the expression one of the statics above holds, for a literal of a plan. The statics
/// compile on first use and no input reaches them, so the error is the compiler's alone.
fn constant_regex(regex: &LazyLock<Result<Regex, RegexError>>) -> Result<Regex, PlanError> {
	regex
		.as_ref()
		.map(Regex::clone)
		.map_err(|e| PlanError::Custom(format!("a LIKE pattern could not be converted: {e}")))
}

/// The regular expression that stands for the `LIKE` pattern `pattern`, an expression that
/// evaluates to text for each row.
///
/// It is the text `sql_like_to_regex` builds for the same pattern: anchored at both ends, with
/// `.` matching a line break, ignoring case when `case_insensitive` is set, with `%` as `.*`, `_`
/// as `.`, a backslash making the character after it literal and a backslash that ends the
/// pattern standing for itself.
///
/// A pattern with no backslash, no regular expression syntax and no noncharacter of the chain has
/// only its wildcards converted. Any other pattern goes through [`escaping_chain`].
///
/// Fails only if an expression the chain uses cannot be built, which no input can cause.
pub(super) fn like_pattern_regex(
	pattern: LogicalExpr,
	case_insensitive: bool,
) -> Result<LogicalExpr, PlanError> {
	let plain_pattern = constant_regex(&PLAIN_PATTERN)?;
	let wildcards = replace(replace(pattern.clone(), "%", ".*"), "_", ".");
	let converted = LogicalExpr::If(Box::new(LogicalIf {
		branches: vec![(
			call(
				"string::matches",
				vec![pattern.clone(), LogicalExpr::Literal(LogicalLiteral::Regex(plain_pattern))],
			),
			wildcards,
		)],
		otherwise: Some(escaping_chain(pattern)?),
	}));
	let opening = if case_insensitive {
		"(?is)^"
	} else {
		"(?s)^"
	};
	Ok(call("string::concat", vec![string(opening), converted, string("$")]))
}

/// The body of the expression of any pattern, read from left to right the way a matcher reads it:
///
/// - The noncharacter of the marks, when the pattern holds it, is written as an escaped mark.
/// - The three escapes that mean something of their own, `\\`, `\%` and `\_`, become marks, in that
///   order. Once the `\\` pairs are gone every backslash left starts an escape, so `\%` and `\_`
///   find exactly the escapes of the pattern.
/// - A backslash left at the end of the text has nothing to escape and becomes the mark of a
///   backslash. A backslash left anywhere else is dropped, which leaves the character after it as
///   itself.
/// - The characters that are syntax to a regular expression are escaped, and the wildcards become
///   `.*` and `.`.
/// - The marks become the text they stand for.
fn escaping_chain(pattern: LogicalExpr) -> Result<LogicalExpr, PlanError> {
	let trailing_backslash = constant_regex(&TRAILING_BACKSLASH)?;

	let mut chain = replace(pattern, MARK, ESCAPED_MARK);
	chain = replace(chain, "\\\\", BACKSLASH);
	chain = replace(chain, "\\%", PERCENT);
	chain = replace(chain, "\\_", UNDERSCORE);
	chain = call(
		"string::replace",
		vec![
			chain,
			LogicalExpr::Literal(LogicalLiteral::Regex(trailing_backslash)),
			string(BACKSLASH),
		],
	);
	chain = replace(chain, "\\", "");
	for syntax in REGEX_SYNTAX {
		chain = replace(chain, syntax, &format!("\\{syntax}"));
	}
	chain = replace(chain, "%", ".*");
	chain = replace(chain, "_", ".");
	chain = replace(chain, PERCENT, "%");
	chain = replace(chain, UNDERSCORE, "_");
	chain = replace(chain, BACKSLASH, "\\\\");
	Ok(replace(chain, ESCAPED_MARK, MARK))
}

/// `string::replace(subject, search, replacement)`, which replaces every occurrence.
fn replace(subject: LogicalExpr, search: &str, replacement: &str) -> LogicalExpr {
	call("string::replace", vec![subject, string(search), string(replacement)])
}

fn string(text: &str) -> LogicalExpr {
	LogicalExpr::Literal(LogicalLiteral::String(Strand::from(text)))
}

fn call(name: &str, args: Vec<LogicalExpr>) -> LogicalExpr {
	LogicalExpr::Call(Box::new(LogicalCall {
		receiver: Function::Normal(name.to_string()),
		args,
	}))
}

#[cfg(test)]
mod tests;
