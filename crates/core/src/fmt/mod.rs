//! SurrealQL formatting utilities.

#[cfg(test)]
mod test;

mod escape;
use std::cell::Cell;
use std::fmt::{self, Display, Formatter, Write};

pub use escape::{
	EscapeIdent, EscapeKey, EscapeKwFreeIdent, EscapeKwIdent, EscapeRidKey, QuoteStr,
};

use crate::{expr, sql};

/// Implements fmt::Display by calling formatter on contents.
pub(crate) struct Fmt<T, F> {
	contents: Cell<Option<T>>,
	formatter: F,
}

impl<T, F: Fn(T, &mut Formatter) -> fmt::Result> Fmt<T, F> {
	pub(crate) fn new(t: T, formatter: F) -> Self {
		Self {
			contents: Cell::new(Some(t)),
			formatter,
		}
	}
}

impl<T, F: Fn(T, &mut Formatter) -> fmt::Result> Display for Fmt<T, F> {
	/// fmt is single-use only.
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let contents = self.contents.replace(None).expect("only call Fmt::fmt once");
		(self.formatter)(contents, f)
	}
}

impl<I: IntoIterator<Item = T>, T: Display> Fmt<I, fn(I, &mut Formatter) -> fmt::Result> {
	/// Formats values with a comma and a space separating them.
	pub(crate) fn comma_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_comma_separated)
	}

	/// Formats values with a verbar and a space separating them.
	pub(crate) fn verbar_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_verbar_separated)
	}

	/// Formats values with a comma and a space separating them or, if pretty
	/// printing is in effect, a comma, a newline, and indentation.
	pub(crate) fn pretty_comma_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_pretty_comma_separated)
	}

	/// Formats values with a new line separating them.
	pub(crate) fn one_line_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_one_line_separated)
	}

	/// Formats values with a new line separating them.
	pub(crate) fn two_line_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_two_line_separated)
	}
}

fn fmt_comma_separated<T: Display, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut Formatter,
) -> fmt::Result {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			f.write_str(", ")?;
		}
		Display::fmt(&v, f)?;
	}
	Ok(())
}

fn fmt_verbar_separated<T: Display, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut Formatter,
) -> fmt::Result {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			f.write_str(" | ")?;
		}
		Display::fmt(&v, f)?;
	}
	Ok(())
}

fn fmt_pretty_comma_separated<T: Display, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut Formatter,
) -> fmt::Result {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			if is_pretty() {
				f.write_char(',')?;
				pretty_sequence_item();
			} else {
				f.write_str(", ")?;
			}
		}
		Display::fmt(&v, f)?;
	}
	Ok(())
}

fn fmt_one_line_separated<T: Display, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut Formatter,
) -> fmt::Result {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			if is_pretty() {
				pretty_sequence_item();
			} else {
				f.write_char('\n')?;
			}
		}
		Display::fmt(&v, f)?;
	}
	Ok(())
}

fn fmt_two_line_separated<T: Display, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut Formatter,
) -> fmt::Result {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			if is_pretty() {
				f.write_char('\n')?;
				pretty_sequence_item();
			} else {
				f.write_char('\n')?;
				f.write_char('\n')?;
			}
		}
		Display::fmt(&v, f)?;
	}
	Ok(())
}

/// Creates a formatting function that joins iterators with an arbitrary
/// separator.
pub fn fmt_separated_by<T: Display, I: IntoIterator<Item = T>>(
	separator: impl Display,
) -> impl Fn(I, &mut Formatter) -> fmt::Result {
	move |into_iter: I, f: &mut Formatter| {
		for (i, v) in into_iter.into_iter().enumerate() {
			if i > 0 {
				Display::fmt(&separator, f)?;
			}
			Display::fmt(&v, f)?;
		}
		Ok(())
	}
}

thread_local! {
	/// Whether pretty-printing.
	static PRETTY: Cell<bool> = const { Cell::new(false)};
	/// The current level of indentation, in units of tabs.
	static INDENT: Cell<u32> = const{ Cell::new(0)};
	/// Whether the next formatting action should be preceded by a newline and indentation.
	static NEW_LINE: Cell<bool> = const{ Cell::new(false)};
}

/// An adapter that, if enabled, adds pretty print formatting.
pub(crate) struct Pretty<W: std::fmt::Write> {
	inner: W,
	/// This is the active pretty printer, responsible for injecting formatting.
	active: bool,
}

impl<W: std::fmt::Write> Pretty<W> {
	#[expect(unused)]
	pub fn new(inner: W) -> Self {
		Self::conditional(inner, true)
	}

	pub fn conditional(inner: W, enable: bool) -> Self {
		let pretty_started_here = enable
			&& PRETTY.with(|pretty| {
				if pretty.get() {
					false
				} else {
					pretty.set(true);
					true
				}
			});
		if pretty_started_here {
			// Clean slate.
			NEW_LINE.with(|new_line| new_line.set(false));
			INDENT.with(|indent| indent.set(0));
		}
		Self {
			inner,
			// Don't want multiple active pretty printers, although they wouldn't necessarily
			// misbehave.
			active: pretty_started_here,
		}
	}
}

impl<'a, 'b> From<&'a mut Formatter<'b>> for Pretty<&'a mut Formatter<'b>> {
	fn from(f: &'a mut Formatter<'b>) -> Self {
		Self::conditional(f, f.alternate())
	}
}

impl<W: std::fmt::Write> Drop for Pretty<W> {
	fn drop(&mut self) {
		if self.active {
			PRETTY.with(|pretty| {
				debug_assert!(pretty.get(), "pretty status changed unexpectedly");
				pretty.set(false);
			});
		}
	}
}

pub struct CoverStmts<'a, E>(pub &'a E);

impl Display for CoverStmts<'_, expr::Expr> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self.0 {
			expr::Expr::Literal(_)
			| expr::Expr::Param(_)
			| expr::Expr::Idiom(_)
			| expr::Expr::Table(_)
			| expr::Expr::Mock(_)
			| expr::Expr::Block(_)
			| expr::Expr::Constant(_)
			| expr::Expr::Prefix {
				..
			}
			| expr::Expr::Postfix {
				..
			}
			| expr::Expr::Binary {
				..
			}
			| expr::Expr::FunctionCall(_)
			| expr::Expr::Closure(_)
			| expr::Expr::Break
			| expr::Expr::Continue
			| expr::Expr::Throw(_) => self.0.fmt(f),
			expr::Expr::Return(_)
			| expr::Expr::IfElse(_)
			| expr::Expr::Select(_)
			| expr::Expr::Create(_)
			| expr::Expr::Update(_)
			| expr::Expr::Upsert(_)
			| expr::Expr::Delete(_)
			| expr::Expr::Relate(_)
			| expr::Expr::Insert(_)
			| expr::Expr::Define(_)
			| expr::Expr::Remove(_)
			| expr::Expr::Rebuild(_)
			| expr::Expr::Alter(_)
			| expr::Expr::Info(_)
			| expr::Expr::Foreach(_)
			| expr::Expr::Let(_)
			| expr::Expr::Sleep(_) => {
				f.write_str("(")?;
				self.0.fmt(f)?;
				f.write_str(")")
			}
		}
	}
}

impl Display for CoverStmts<'_, sql::Expr> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self.0 {
			sql::Expr::Literal(_)
			| sql::Expr::Param(_)
			| sql::Expr::Idiom(_)
			| sql::Expr::Table(_)
			| sql::Expr::Mock(_)
			| sql::Expr::Block(_)
			| sql::Expr::Constant(_)
			| sql::Expr::Prefix {
				..
			}
			| sql::Expr::Postfix {
				..
			}
			| sql::Expr::Binary {
				..
			}
			| sql::Expr::FunctionCall(_)
			| sql::Expr::Closure(_)
			| sql::Expr::Break
			| sql::Expr::Continue
			| sql::Expr::Throw(_) => self.0.fmt(f),
			sql::Expr::Return(_)
			| sql::Expr::IfElse(_)
			| sql::Expr::Select(_)
			| sql::Expr::Create(_)
			| sql::Expr::Update(_)
			| sql::Expr::Upsert(_)
			| sql::Expr::Delete(_)
			| sql::Expr::Relate(_)
			| sql::Expr::Insert(_)
			| sql::Expr::Define(_)
			| sql::Expr::Remove(_)
			| sql::Expr::Rebuild(_)
			| sql::Expr::Alter(_)
			| sql::Expr::Info(_)
			| sql::Expr::Foreach(_)
			| sql::Expr::Let(_)
			| sql::Expr::Sleep(_) => {
				f.write_str("(")?;
				self.0.fmt(f)?;
				f.write_str(")")
			}
		}
	}
}

/// Returns whether pretty printing is in effect.
pub(crate) fn is_pretty() -> bool {
	PRETTY.with(|pretty| pretty.get())
}

/// If pretty printing is in effect, increments the indentation level (until the
/// return value is dropped).
#[must_use = "hold for the span of the indent, then drop"]
pub(crate) fn pretty_indent() -> PrettyGuard {
	PrettyGuard::new(1)
}

/// Marks the end of an item in the sequence, after which indentation will
/// follow if pretty printing is in effect.
pub(crate) fn pretty_sequence_item() {
	// List items need a new line, but no additional indentation.
	NEW_LINE.with(|new_line| new_line.set(true));
}

/// When dropped, applies the opposite increment to the current indentation
/// level.
pub(crate) struct PrettyGuard {
	increment: i8,
}

impl PrettyGuard {
	fn new(increment: i8) -> Self {
		Self::raw(increment);
		PrettyGuard {
			increment,
		}
	}

	fn raw(increment: i8) {
		INDENT.with(|indent| {
			// Equivalent to `indent += increment` if signed numbers could be added to
			// unsigned numbers in stable, atomic Rust.
			if increment >= 0 {
				indent.set(indent.get() + increment as u32);
			} else {
				indent.set(indent.get() - increment.unsigned_abs() as u32);
			}
		});
		NEW_LINE.with(|new_line| new_line.set(true));
	}
}

impl Drop for PrettyGuard {
	fn drop(&mut self) {
		Self::raw(-self.increment)
	}
}

impl<W: std::fmt::Write> std::fmt::Write for Pretty<W> {
	fn write_str(&mut self, s: &str) -> std::fmt::Result {
		if self.active && NEW_LINE.with(|new_line| new_line.replace(false)) {
			// Newline.
			self.inner.write_char('\n')?;
			for _ in 0..INDENT.with(|indent| indent.get()) {
				// One level of indentation.
				self.inner.write_char('\t')?;
			}
		}
		// What we were asked to write.
		self.inner.write_str(s)
	}
}

pub struct Float(pub f64);

impl Display for Float {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		if !self.0.is_finite() {
			if self.0.is_nan() {
				f.write_str("NaN")?;
			} else if self.0.is_sign_positive() {
				f.write_str("Infinity")?;
			} else {
				f.write_str("-Infinity")?;
			}
		} else {
			self.0.fmt(f)?;
			f.write_str("f")?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::syn::{expr, parse};

	#[test]
	fn pretty_query() {
		let query = parse("SELECT * FROM {foo: [1, 2, 3]};").unwrap();
		assert_eq!(format!("{query}"), "SELECT * FROM { foo: [1, 2, 3] };");
		assert_eq!(
			format!("{:#}", query),
			"SELECT * FROM {\n\tfoo: [\n\t\t1,\n\t\t2,\n\t\t3\n\t]\n};"
		);
	}

	#[test]
	fn pretty_define_query() {
		let query = parse("DEFINE TABLE test SCHEMAFULL PERMISSIONS FOR create, update, delete NONE FOR select WHERE public = true;").unwrap();
		assert_eq!(
			format!("{}", query),
			"DEFINE TABLE test TYPE NORMAL SCHEMAFULL PERMISSIONS FOR select WHERE public = true, FOR create, update, delete NONE;"
		);
		assert_eq!(
			format!("{:#}", query),
			"DEFINE TABLE test TYPE NORMAL SCHEMAFULL\n\tPERMISSIONS\n\t\tFOR select\n\t\t\tWHERE public = true\n\t\tFOR create, update, delete NONE\n;"
		);
	}

	#[test]
	fn pretty_value() {
		let value = expr("{foo: [1, 2, 3]}").unwrap();
		assert_eq!(format!("{}", value), "{ foo: [1, 2, 3] }");
		assert_eq!(format!("{:#}", value), "{\n\tfoo: [\n\t\t1,\n\t\t2,\n\t\t3\n\t]\n}");
	}

	#[test]
	fn pretty_array() {
		let array = expr("[1, 2, 3]").unwrap();
		assert_eq!(format!("{}", array), "[1, 2, 3]");
		assert_eq!(format!("{:#}", array), "[\n\t1,\n\t2,\n\t3\n]");
	}
}
