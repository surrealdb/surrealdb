//! SQL-level prepared statements: `PREPARE name [(types)] AS statement`, `EXECUTE name(args)` and
//! `DEALLOCATE [PREPARE] name | ALL`.
//!
//! A prepared statement is kept as its source text. `EXECUTE` splices each argument, as it was
//! written, in place of the `$n` it stands for, so the planner only sees the statement the
//! arguments make. An argument that is more than one token is parenthesised to keep its binding,
//! and one for a declared parameter type is cast to it. Because an argument is spliced rather than
//! evaluated once, an argument that is more than one token is refused for a parameter the
//! statement reads more than once. The statements are the connection's own and are separate from
//! those of the extended protocol; their number and total size are bounded.

use std::collections::HashMap;

use surrealdb_pgsql::{Lexer, TokenKind};

/// The most statements one connection keeps.
pub(crate) const MAX_STATEMENTS: usize = 1024;
/// The most source text one connection keeps across its statements, in bytes.
pub(crate) const MAX_BYTES: usize = 32 * 1024 * 1024;
/// The most source text one `EXECUTE` may make of a statement and its arguments, in bytes.
pub(crate) const MAX_EXPANDED: usize = 16 * 1024 * 1024;
/// The bookkeeping every statement costs besides its own text, in bytes.
const ENTRY_OVERHEAD: usize = 128;
/// PostgreSQL's limit on the parameters of a statement.
const MAX_PARAMETERS: usize = 65535;

/// A prepared statement.
#[derive(Debug)]
pub(crate) struct Entry {
	sql: String,
	/// The arguments `EXECUTE` must give: the declared types, or the highest `$n` read.
	parameters: usize,
	/// The declared parameter types, as written.
	types: Vec<String>,
	/// How many times each `$n` is read, by `n - 1`.
	reads: Vec<usize>,
	/// What the statement holds in memory, counted against [`MAX_BYTES`]: its name, text, declared
	/// types and read counts.
	size: usize,
}

/// Why a statement was not kept.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PrepareError {
	/// The name is in use.
	Exists,
	/// The connection holds as many statements or as much text as it may.
	Full,
	/// The statement reads a parameter beyond the limit.
	TooManyParameters,
}

/// Why the arguments of an `EXECUTE` did not make a statement.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BindError {
	/// There are more or fewer arguments than parameters.
	WrongCount,
	/// The argument for parameter `n` is more than one token, and the statement reads it twice.
	Reused(usize),
	/// The statement the arguments make is larger than [`MAX_EXPANDED`].
	TooLarge,
	/// An argument names a column or other object, which an argument has no scope to read.
	Unbound(String),
}

/// An argument of an `EXECUTE`, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Argument {
	text: String,
	single_token: bool,
	/// The first name the argument reads that is not a function or a type.
	unbound: Option<String>,
}

impl Argument {
	pub(crate) fn new(source: &str) -> Self {
		let source = source.trim();
		let mut lexer = Lexer::new(source);
		let mut tokens: Vec<TokenKind<'_>> = Vec::new();
		while let Ok(token) = lexer.next_token() {
			if token.is_eof() {
				break;
			}
			tokens.push(token.kind);
		}
		let mut unbound = None;
		for (index, kind) in tokens.iter().enumerate() {
			let name = if let TokenKind::Ident(name) = kind {
				name.to_string()
			} else if let TokenKind::QuotedIdent(name) = kind {
				name.clone()
			} else {
				continue;
			};
			let calls = matches!(tokens.get(index + 1), Some(TokenKind::LParen));
			let casts = index > 0 && matches!(tokens.get(index - 1), Some(TokenKind::DoubleColon));
			if !calls && !casts {
				unbound = Some(name);
				break;
			}
		}
		Self {
			text: source.to_string(),
			single_token: tokens.len() <= 1,
			unbound,
		}
	}

	fn spliced(&self) -> String {
		if self.single_token {
			self.text.clone()
		} else {
			format!("({})", self.text)
		}
	}
}

/// The SQL-level prepared statements of one connection. Names are as the parser reads them, so an
/// unquoted name is folded and a quoted one is not.
#[derive(Debug, Default)]
pub(crate) struct SqlPrepared {
	statements: HashMap<String, Entry>,
	bytes: usize,
}

impl SqlPrepared {
	pub(crate) fn prepare(
		&mut self,
		name: &str,
		sql: String,
		types: Vec<String>,
	) -> Result<(), PrepareError> {
		if self.statements.contains_key(name) {
			return Err(PrepareError::Exists);
		}
		let reads = parameter_reads(&sql).ok_or(PrepareError::TooManyParameters)?;
		let size = ENTRY_OVERHEAD
			+ name.len()
			+ sql.len()
			+ types.iter().map(|ty| ty.len() + std::mem::size_of::<String>()).sum::<usize>()
			+ reads.len() * std::mem::size_of::<usize>();
		if self.statements.len() >= MAX_STATEMENTS || self.bytes.saturating_add(size) > MAX_BYTES {
			return Err(PrepareError::Full);
		}
		let parameters = reads.len().max(types.len());
		self.bytes += size;
		self.statements.insert(
			name.to_string(),
			Entry {
				sql,
				parameters,
				types,
				reads,
				size,
			},
		);
		Ok(())
	}

	/// The statement `name` makes of `arguments`; `None` when there is no such statement.
	pub(crate) fn bind(
		&self,
		name: &str,
		arguments: &[Argument],
	) -> Option<Result<String, BindError>> {
		self.statements.get(name).map(|entry| entry.bind(arguments))
	}

	pub(crate) fn deallocate(&mut self, name: &str) -> bool {
		match self.statements.remove(name) {
			Some(entry) => {
				self.bytes -= entry.size;
				true
			}
			None => false,
		}
	}

	pub(crate) fn deallocate_all(&mut self) {
		self.statements.clear();
		self.bytes = 0;
	}
}

impl Entry {
	fn bind(&self, arguments: &[Argument]) -> Result<String, BindError> {
		if arguments.len() != self.parameters {
			return Err(BindError::WrongCount);
		}
		let mut literals = Vec::with_capacity(arguments.len());
		for (index, argument) in arguments.iter().enumerate() {
			if let Some(name) = &argument.unbound {
				return Err(BindError::Unbound(name.clone()));
			}
			if !argument.single_token && self.reads.get(index).copied().unwrap_or(0) > 1 {
				return Err(BindError::Reused(index + 1));
			}
			literals.push(match self.types.get(index) {
				Some(ty) => format!("CAST({} AS {ty})", argument.text),
				None => argument.spliced(),
			});
		}
		let expanded =
			self.reads.iter().zip(&literals).fold(self.sql.len(), |total, (reads, literal)| {
				total.saturating_add(reads.saturating_mul(literal.len()))
			});
		if expanded > MAX_EXPANDED {
			return Err(BindError::TooLarge);
		}
		Ok(splice(&self.sql, &literals))
	}
}

/// How many times each `$n` of `sql` is read, by `n - 1`. `None` when one is past the limit.
fn parameter_reads(sql: &str) -> Option<Vec<usize>> {
	let mut reads: Vec<usize> = Vec::new();
	let mut lexer = Lexer::new(sql);
	while let Ok(token) = lexer.next_token() {
		if token.is_eof() {
			break;
		}
		if let TokenKind::Param(n) = token.kind {
			let n = n as usize;
			if n == 0 || n > MAX_PARAMETERS {
				return None;
			}
			if reads.len() < n {
				reads.resize(n, 0);
			}
			reads[n - 1] += 1;
		}
	}
	Some(reads)
}

/// `sql` with each `$n` replaced by the n-th literal.
fn splice(sql: &str, literals: &[String]) -> String {
	let mut lexer = Lexer::new(sql);
	let mut out = String::with_capacity(sql.len());
	let mut at = 0usize;
	while let Ok(token) = lexer.next_token() {
		if token.is_eof() {
			break;
		}
		if let TokenKind::Param(n) = token.kind
			&& let Some(literal) = literals.get((n as usize).wrapping_sub(1))
		{
			out.push_str(&sql[at..token.span.start as usize]);
			out.push_str(literal);
			at = token.span.end as usize;
		}
	}
	out.push_str(&sql[at..]);
	out
}

/// The first word of `sql` past whitespace and comments, which is its keyword.
pub(crate) fn first_keyword(sql: &str) -> &str {
	let mut rest = sql;
	loop {
		rest = rest.trim_start();
		if let Some(after) = rest.strip_prefix("/*") {
			let Some(end) = after.find("*/") else {
				return "";
			};
			rest = &after[end + 2..];
		} else if rest.starts_with("--") {
			let Some(end) = rest.find('\n') else {
				return "";
			};
			rest = &rest[end..];
		} else {
			break;
		}
	}
	rest.split(|c: char| c.is_whitespace() || c == ';' || c == '(').next().unwrap_or("")
}

#[cfg(test)]
mod tests {
	use super::*;

	fn args(sources: &[&str]) -> Vec<Argument> {
		sources.iter().map(|s| Argument::new(s)).collect()
	}

	fn prepare(sql: &str, types: &[&str]) -> SqlPrepared {
		let mut set = SqlPrepared::default();
		set.prepare("s", sql.to_string(), types.iter().map(|t| t.to_string()).collect()).unwrap();
		set
	}

	#[test]
	fn arguments_replace_their_parameters() {
		let set = prepare("select $2, '$1' from t where a > $1", &[]);
		assert_eq!(
			set.bind("s", &args(&["5", "'b'"])).unwrap().as_deref(),
			Ok("select 'b', '$1' from t where a > 5")
		);
		assert_eq!(set.bind("s", &args(&["5"])).unwrap(), Err(BindError::WrongCount));
		assert!(set.bind("t", &[]).is_none());
	}

	#[test]
	fn surplus_and_missing_arguments_are_refused() {
		let none = prepare("select 1", &[]);
		assert_eq!(none.bind("s", &args(&["42"])).unwrap(), Err(BindError::WrongCount));
		assert_eq!(none.bind("s", &[]).unwrap().as_deref(), Ok("select 1"));
		let declared = prepare("select 1", &["int"]);
		assert_eq!(declared.bind("s", &[]).unwrap(), Err(BindError::WrongCount));
	}

	#[test]
	fn declared_types_cast_their_arguments() {
		let set = prepare("select $1 / 2", &["integer"]);
		assert_eq!(
			set.bind("s", &args(&["5.0"])).unwrap().as_deref(),
			Ok("select CAST(5.0 AS integer) / 2")
		);
	}

	#[test]
	fn a_compound_argument_is_parenthesised_and_not_repeated() {
		let once = prepare("select $1 * 2", &[]);
		assert_eq!(once.bind("s", &args(&["1 + 2"])).unwrap().as_deref(), Ok("select (1 + 2) * 2"));
		let twice = prepare("select $1 = $1", &[]);
		assert_eq!(twice.bind("s", &args(&["random()"])).unwrap(), Err(BindError::Reused(1)));
		assert_eq!(twice.bind("s", &args(&["7"])).unwrap().as_deref(), Ok("select 7 = 7"));
	}

	#[test]
	fn names_are_exact_and_bounded() {
		let mut set = SqlPrepared::default();
		set.prepare("S", "select 1".into(), vec![]).unwrap();
		set.prepare("s", "select 2".into(), vec![]).unwrap();
		assert_eq!(set.prepare("s", "select 3".into(), vec![]), Err(PrepareError::Exists));
		assert!(set.deallocate("S"));
		assert!(!set.deallocate("S"));
		assert!(set.bind("s", &[]).is_some());
		let mut full = SqlPrepared::default();
		for i in 0..MAX_STATEMENTS {
			full.prepare(&format!("p{i}"), "select 1".into(), vec![]).unwrap();
		}
		assert_eq!(full.prepare("one-more", "select 1".into(), vec![]), Err(PrepareError::Full));
	}

	#[test]
	fn a_statements_name_and_types_count_toward_the_bound() {
		let mut set = SqlPrepared::default();
		let name = "n".repeat(MAX_BYTES / 2);
		set.prepare(&name, "select 1".into(), vec![]).unwrap();
		let other = "m".repeat(MAX_BYTES / 2 + 1);
		assert_eq!(set.prepare(&other, "select 1".into(), vec![]), Err(PrepareError::Full));
		assert!(set.deallocate(&name));
		assert!(set.prepare(&other, "select 1".into(), vec![]).is_ok());
	}

	#[test]
	fn an_expansion_past_the_bound_is_refused() {
		let set = prepare(&format!("select {}", "$1, ".repeat(99) + "$1"), &[]);
		let big = format!("'{}'", "x".repeat(MAX_EXPANDED / 50));
		assert_eq!(set.bind("s", &args(&[&big])).unwrap(), Err(BindError::TooLarge));
		assert!(set.bind("s", &args(&["'x'"])).unwrap().is_ok());
	}

	#[test]
	fn an_argument_cannot_read_a_name() {
		let set = prepare("select $1 from item", &[]);
		assert_eq!(
			set.bind("s", &args(&["qty"])).unwrap(),
			Err(BindError::Unbound("qty".to_string()))
		);
		assert_eq!(
			set.bind("s", &args(&["item.qty + 1"])).unwrap(),
			Err(BindError::Unbound("item".to_string()))
		);
		assert!(set.bind("s", &args(&["abs(-1)"])).unwrap().is_ok());
		assert!(set.bind("s", &args(&["5::int"])).unwrap().is_ok());
		assert!(set.bind("s", &args(&["'a'"])).unwrap().is_ok());
	}

	#[test]
	fn the_keyword_is_found_past_comments() {
		assert_eq!(first_keyword("-- c\n/* d */ PREPARE s AS select 1"), "PREPARE");
		assert_eq!(first_keyword("execute s(1)"), "execute");
		assert_eq!(first_keyword("/* open"), "");
	}
}
