//! Syntax of the PostgreSQL `jsonpath` type.
//!
//! A path is an optional `lax` or `strict` mode, then an expression. The expression covers the
//! root `$`, the current item `@` (inside a filter), named variables, literals, arithmetic,
//! member and array accessors (`.key`, `."key"`, `.*`, `.**`, `[*]`, `[n, m to k, last]`),
//! the item methods, filters `?( … )` with comparisons, `&&`, `||`, `!`, `exists()`,
//! `starts with`, `like_regex … flag …`, and `is unknown`. The regular expression and the
//! datetime template are checked for their quoting only, not parsed.

/// A jsonpath text that is not a path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathError {
	pub reason: String,
}

/// The item methods of PostgreSQL 17.
const METHODS: &[&str] = &[
	"type",
	"size",
	"double",
	"ceiling",
	"floor",
	"abs",
	"datetime",
	"keyvalue",
	"bigint",
	"boolean",
	"date",
	"decimal",
	"integer",
	"number",
	"string",
	"time",
	"time_tz",
	"timestamp",
	"timestamp_tz",
];

/// Check the syntax of a jsonpath.
pub fn check(text: &str) -> Result<(), PathError> {
	let tokens = lex(text)?;
	let mut parser = Parser {
		tokens: &tokens,
		at: 0,
		filter_depth: 0,
		depth: 0,
		in_subscript: false,
	};
	// The mode is a keyword only when a path follows it.
	if let Some(Token::Ident(word)) = tokens.first()
		&& (word.eq_ignore_ascii_case("lax") || word.eq_ignore_ascii_case("strict"))
		&& tokens.len() > 1
	{
		parser.at = 1;
	}
	parser.logic_or()?;
	if parser.at != tokens.len() {
		return Err(syntax("unexpected token after the path"));
	}
	Ok(())
}

fn syntax(detail: &str) -> PathError {
	PathError {
		reason: format!("syntax error in jsonpath: {detail}"),
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
	Number,
	Str,
	Ident(String),
	Variable,
	Root,
	Current,
	Op(&'static str),
}

const TWO_CHAR_OPS: &[&str] = &["**", "==", "!=", "<>", "<=", ">=", "&&", "||"];
const ONE_CHAR_OPS: &[&str] =
	&["<", ">", "!", "+", "-", "*", "/", "%", "(", ")", "[", "]", "{", "}", ",", ".", "?"];

fn is_ident_start(c: char) -> bool {
	c.is_alphabetic() || c == '_' || !c.is_ascii()
}

fn is_ident_char(c: char) -> bool {
	c.is_alphanumeric() || c == '_' || !c.is_ascii()
}

fn lex(text: &str) -> Result<Vec<Token>, PathError> {
	let chars: Vec<char> = text.chars().collect();
	let mut tokens = Vec::new();
	let mut at = 0;
	while at < chars.len() {
		let c = chars[at];
		if c.is_whitespace() {
			at += 1;
		} else if c.is_ascii_digit() {
			at = lex_number(&chars, at)?;
			tokens.push(Token::Number);
		} else if c == '"' {
			at = lex_string(&chars, at)?;
			tokens.push(Token::Str);
		} else if c == '$' {
			if chars.get(at + 1).is_some_and(|d| is_ident_start(*d)) {
				at += 1;
				while chars.get(at).is_some_and(|d| is_ident_char(*d)) {
					at += 1;
				}
				tokens.push(Token::Variable);
			} else {
				at += 1;
				tokens.push(Token::Root);
			}
		} else if c == '@' {
			at += 1;
			tokens.push(Token::Current);
		} else if is_ident_start(c) {
			let start = at;
			while chars.get(at).is_some_and(|d| is_ident_char(*d)) {
				at += 1;
			}
			tokens.push(Token::Ident(chars[start..at].iter().collect()));
		} else {
			let pair: String = chars[at..(at + 2).min(chars.len())].iter().collect();
			if let Some(op) = TWO_CHAR_OPS.iter().find(|op| **op == pair) {
				tokens.push(Token::Op(op));
				at += 2;
			} else if let Some(op) = ONE_CHAR_OPS.iter().find(|op| op.starts_with(c)) {
				tokens.push(Token::Op(op));
				at += 1;
			} else {
				return Err(syntax(&format!("unexpected character \"{c}\"")));
			}
		}
	}
	Ok(tokens)
}

/// A number: digits, an optional fraction, and an optional exponent.
fn lex_number(chars: &[char], mut at: usize) -> Result<usize, PathError> {
	while chars.get(at).is_some_and(|c| c.is_ascii_digit()) {
		at += 1;
	}
	if chars.get(at) == Some(&'.') && chars.get(at + 1).is_some_and(|c| c.is_ascii_digit()) {
		at += 1;
		while chars.get(at).is_some_and(|c| c.is_ascii_digit()) {
			at += 1;
		}
	}
	if matches!(chars.get(at), Some('e' | 'E')) {
		let mut exponent = at + 1;
		if matches!(chars.get(exponent), Some('+' | '-')) {
			exponent += 1;
		}
		if chars.get(exponent).is_some_and(|c| c.is_ascii_digit()) {
			at = exponent;
			while chars.get(at).is_some_and(|c| c.is_ascii_digit()) {
				at += 1;
			}
		}
	}
	Ok(at)
}

/// A double-quoted string. A backslash escapes the next character.
fn lex_string(chars: &[char], mut at: usize) -> Result<usize, PathError> {
	at += 1;
	loop {
		match chars.get(at) {
			None => return Err(syntax("unterminated string")),
			Some('\\') => at += 2,
			Some('"') => return Ok(at + 1),
			Some(_) => at += 1,
		}
	}
}

/// The deepest a jsonpath may nest (parentheses, filters, `exists`, negations and signs), so a
/// check cannot exhaust the stack.
const MAX_PATH_DEPTH: usize = 256;

struct Parser<'a> {
	tokens: &'a [Token],
	at: usize,
	filter_depth: usize,
	/// The nested predicates and unary operands being parsed.
	depth: usize,
	in_subscript: bool,
}

impl Parser<'_> {
	fn peek(&self) -> Option<&Token> {
		self.tokens.get(self.at)
	}

	fn peek_at(&self, offset: usize) -> Option<&Token> {
		self.tokens.get(self.at + offset)
	}

	fn eat_op(&mut self, op: &str) -> bool {
		if matches!(self.peek(), Some(Token::Op(found)) if *found == op) {
			self.at += 1;
			true
		} else {
			false
		}
	}

	fn expect_op(&mut self, op: &str) -> Result<(), PathError> {
		if self.eat_op(op) {
			Ok(())
		} else {
			Err(syntax(&format!("\"{op}\" is expected")))
		}
	}

	fn eat_word(&mut self, word: &str) -> bool {
		if matches!(self.peek(), Some(Token::Ident(found)) if found.eq_ignore_ascii_case(word)) {
			self.at += 1;
			true
		} else {
			false
		}
	}

	fn expect_word(&mut self, word: &str) -> Result<(), PathError> {
		if self.eat_word(word) {
			Ok(())
		} else {
			Err(syntax(&format!("\"{word}\" is expected")))
		}
	}

	fn logic_or(&mut self) -> Result<(), PathError> {
		self.logic_and()?;
		while self.eat_op("||") {
			self.logic_and()?;
		}
		Ok(())
	}

	fn logic_and(&mut self) -> Result<(), PathError> {
		self.predicate()?;
		while self.eat_op("&&") {
			self.predicate()?;
		}
		Ok(())
	}

	/// Run `parse` one nesting level deeper, refusing a path nested beyond [`MAX_PATH_DEPTH`].
	fn nested(&mut self, parse: fn(&mut Self) -> Result<(), PathError>) -> Result<(), PathError> {
		if self.depth == MAX_PATH_DEPTH {
			return Err(syntax("the path is nested too deeply"));
		}
		self.depth += 1;
		let parsed = parse(self);
		self.depth -= 1;
		parsed
	}

	fn predicate(&mut self) -> Result<(), PathError> {
		self.nested(Self::predicate_at)
	}

	fn predicate_at(&mut self) -> Result<(), PathError> {
		while self.eat_op("!") {}
		if self.eat_word("exists") {
			self.expect_op("(")?;
			self.logic_or()?;
			return self.expect_op(")");
		}
		self.arith()?;
		let comparison =
			matches!(self.peek(), Some(Token::Op("==" | "!=" | "<>" | "<" | "<=" | ">" | ">=")));
		if comparison {
			self.at += 1;
			return self.arith();
		}
		if self.eat_word("starts") {
			self.expect_word("with")?;
			return self.arith();
		}
		if self.eat_word("like_regex") {
			if !matches!(self.peek(), Some(Token::Str)) {
				return Err(syntax("a pattern string is expected after like_regex"));
			}
			self.at += 1;
			if self.eat_word("flag") {
				if !matches!(self.peek(), Some(Token::Str)) {
					return Err(syntax("a flag string is expected after flag"));
				}
				self.at += 1;
			}
			return Ok(());
		}
		if self.eat_word("is") {
			return self.expect_word("unknown");
		}
		Ok(())
	}

	fn arith(&mut self) -> Result<(), PathError> {
		self.multiplicative()?;
		while self.eat_op("+") || self.eat_op("-") {
			self.multiplicative()?;
		}
		Ok(())
	}

	fn multiplicative(&mut self) -> Result<(), PathError> {
		self.unary()?;
		while self.eat_op("*") || self.eat_op("/") || self.eat_op("%") {
			self.unary()?;
		}
		Ok(())
	}

	fn unary(&mut self) -> Result<(), PathError> {
		self.nested(Self::unary_at)
	}

	fn unary_at(&mut self) -> Result<(), PathError> {
		while self.eat_op("+") || self.eat_op("-") {}
		self.primary()?;
		self.accessors()
	}

	fn primary(&mut self) -> Result<(), PathError> {
		match self.peek() {
			Some(Token::Root | Token::Variable | Token::Number | Token::Str) => {
				self.at += 1;
				Ok(())
			}
			Some(Token::Current) => {
				if self.filter_depth == 0 {
					return Err(syntax("\"@\" is only allowed in a filter"));
				}
				self.at += 1;
				Ok(())
			}
			Some(Token::Ident(word))
				if word.eq_ignore_ascii_case("true")
					|| word.eq_ignore_ascii_case("false")
					|| word.eq_ignore_ascii_case("null") =>
			{
				self.at += 1;
				Ok(())
			}
			Some(Token::Ident(word)) if word.eq_ignore_ascii_case("last") => {
				if !self.in_subscript {
					return Err(syntax("\"last\" is only allowed in an array subscript"));
				}
				self.at += 1;
				Ok(())
			}
			Some(Token::Op("(")) => {
				self.at += 1;
				self.logic_or()?;
				self.expect_op(")")
			}
			_ => Err(syntax("a path element is expected")),
		}
	}

	/// The accessors that follow a primary: members, subscripts, methods, and filters.
	fn accessors(&mut self) -> Result<(), PathError> {
		loop {
			if self.eat_op(".") {
				self.member()?;
			} else if self.eat_op("[") {
				self.subscripts()?;
			} else if self.eat_op("?") {
				self.expect_op("(")?;
				self.filter_depth += 1;
				self.logic_or()?;
				self.filter_depth -= 1;
				self.expect_op(")")?;
			} else {
				return Ok(());
			}
		}
	}

	/// What follows a `.`: `*`, `**` with an optional level, a method, or a key.
	fn member(&mut self) -> Result<(), PathError> {
		if self.eat_op("*") {
			return Ok(());
		}
		if self.eat_op("**") {
			if self.eat_op("{") {
				self.level()?;
				if self.eat_word("to") {
					self.level()?;
				}
				self.expect_op("}")?;
			}
			return Ok(());
		}
		match self.peek() {
			Some(Token::Str) => {
				self.at += 1;
				Ok(())
			}
			Some(Token::Ident(word)) => {
				let is_method = METHODS.iter().any(|m| word.eq_ignore_ascii_case(m))
					&& matches!(self.peek_at(1), Some(Token::Op("(")));
				self.at += 1;
				if is_method {
					self.method_arguments()
				} else {
					Ok(())
				}
			}
			_ => Err(syntax("a key or a method is expected after \".\"")),
		}
	}

	fn method_arguments(&mut self) -> Result<(), PathError> {
		self.expect_op("(")?;
		if self.eat_op(")") {
			return Ok(());
		}
		loop {
			self.arith()?;
			if self.eat_op(",") {
				continue;
			}
			return self.expect_op(")");
		}
	}

	/// A level of a `.**` accessor: a number or `last`.
	fn level(&mut self) -> Result<(), PathError> {
		match self.peek() {
			Some(Token::Number) => {
				self.at += 1;
				Ok(())
			}
			Some(Token::Ident(word)) if word.eq_ignore_ascii_case("last") => {
				self.at += 1;
				Ok(())
			}
			_ => Err(syntax("a level is a number or last")),
		}
	}

	/// The inside of `[ … ]` after the `[`: `*`, or a list of indices and ranges.
	fn subscripts(&mut self) -> Result<(), PathError> {
		if self.eat_op("*") {
			return self.expect_op("]");
		}
		let outer = self.in_subscript;
		self.in_subscript = true;
		loop {
			self.arith()?;
			if self.eat_word("to") {
				self.arith()?;
			}
			if self.eat_op(",") {
				continue;
			}
			break;
		}
		self.in_subscript = outer;
		self.expect_op("]")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ok(text: &str) {
		if let Err(err) = check(text) {
			panic!("{text:?} should be a jsonpath: {err:?}");
		}
	}

	fn refused(text: &str) {
		assert!(check(text).is_err(), "{text:?} should be refused");
	}

	#[test]
	fn paths_with_root_members_and_arrays_are_accepted() {
		ok("$");
		ok("$.a");
		ok("$.a.b.c");
		ok("$.\"a key\"");
		ok("$.*");
		ok("$.**");
		ok("$.**{2}");
		ok("$.**{1 to last}");
		ok("$.a[0]");
		ok("$.a[*]");
		ok("$.a[0, 2 to 4, last]");
		ok("$.a[last - 1]");
		ok("lax $.a");
		ok("STRICT $.a");
		ok("$.a.size()");
		ok("$.a.type()");
		ok("$.a.datetime(\"YYYY-MM-DD\")");
		ok("$.a.keyvalue()");
		ok("$.a.double()");
		ok("$.a.integer()");
		ok("$.a.time_tz()");
		ok("$.a.timestamp_tz()");
		ok("$.a.decimal(4, 2)");
		ok("$ ? (@ == $var)");
		ok("$.a * 2 + 1");
		ok("-$.a");
		ok("$.a.abs() % 3");
		ok("1 + 2");
		ok("1.5e3");
		ok("\"text\"");
		ok("null");
		ok("true");
	}

	#[test]
	fn filters_predicates_and_comparisons_are_accepted() {
		ok("$.a ? (@ > 1)");
		ok("$.a ? (@.b == \"x\" && @.c != 2)");
		ok("$.a ? (@ == 1 || @ == 2)");
		ok("$.a ? (!(@ == 1))");
		ok("$.a ? (exists (@.b))");
		ok("$.a ? (@ starts with \"pre\")");
		ok("$.a ? (@ like_regex \"^a\" flag \"i\")");
		ok("$.a ? (@ is unknown)");
		ok("$.a ? (@ <> 3)");
		ok("$.a ? (@ >= 1 && @ <= 9)");
		ok("$[*] ? (@.x > 0)[0]");
	}

	#[test]
	fn malformed_paths_are_refused() {
		refused("");
		refused("   ");
		refused("$.");
		refused("$..a");
		refused("$[");
		refused("$[0");
		refused("$.a ?");
		refused("$.a ? (@ >)");
		refused("$.a ? (@ = 1)");
		refused("@.a");
		refused("$.a ? (last > 1)");
		refused("$[last]x");
		refused("$ 1");
		refused("$.a == ");
		refused("$.a == 1 =");
		refused("\"unterminated");
		refused("$.a.size(");
		refused("$.a like_regex 1");
		refused("$.a ? (@ like_regex)");
		refused("$.a ? (@ is 1)");
		refused("$.**{x}");
		refused("$ # 1");
		refused("lax");
	}

	#[test]
	fn nesting_is_bounded() {
		let nested = |depth: usize| format!("{}1{}", "(".repeat(depth), ")".repeat(depth));
		assert!(check(&nested(100)).is_ok());
		assert!(check(&nested(100_000)).is_err());
		assert!(
			check(&format!("$ ? ({}@ == 1{})", "(".repeat(100_000), ")".repeat(100_000))).is_err()
		);
		assert!(check(&format!("{}1", "-".repeat(100_000))).is_ok());
	}

	#[test]
	fn mode_is_a_keyword_only_before_a_path() {
		ok("lax $");
		refused("strict");
	}
}
