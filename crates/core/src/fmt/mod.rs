//! SurrealQL formatting utilities.

#[cfg(test)]
mod test;

mod escape;
use std::cell::Cell;
use std::fmt::{self, Display, Formatter, Write};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

pub use escape::{EscapeIdent, EscapeKey, EscapeKwFreeIdent, EscapeRid, QuoteStr};
use surrealdb_types::{write_sql, ToSql, PrettyMode};

/// Implements fmt::Display by calling formatter on contents.
pub(crate) struct Fmt<T, F> {
	contents: Cell<Option<T>>,
	formatter: F,
}

impl<T, F: Fn(T, &mut String, bool)> Fmt<T, F> {
	pub(crate) fn new(t: T, formatter: F) -> Self {
		Self {
			contents: Cell::new(Some(t)),
			formatter,
		}
	}
}

impl<T, F: Fn(T, &mut String, bool)> ToSql for Fmt<T, F> {
	/// fmt is single-use only.
	fn fmt_sql(&self, f: &mut String, pretty: PrettyMode) {
		let contents = self.contents.replace(None).expect("only call Fmt::fmt once");
		(self.formatter)(contents, f, pretty)
	}
}

impl<I: IntoIterator<Item = T>, T: ToSql> Fmt<I, fn(I, &mut String, bool)> {
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

fn fmt_comma_separated<T: ToSql, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut String,
	pretty: PrettyMode,
) {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			f.push_str(", ");
		}
		v.fmt_sql(f, pretty);
	}
}

fn fmt_verbar_separated<T: ToSql, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut String,
	pretty: PrettyMode,
) {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			f.push_str(" | ");
		}
		v.fmt_sql(f, pretty);
	}
}

fn fmt_pretty_comma_separated<T: ToSql, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut String,
	pretty: PrettyMode,
) {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			if pretty {
				f.push(',');
				pretty_sequence_item();
			} else {
				f.push_str(", ");
			}
		}
		v.fmt_sql(f, pretty);
	}
}

fn fmt_one_line_separated<T: ToSql, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut String,
	pretty: PrettyMode,
) {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			if pretty {
				pretty_sequence_item();
			} else {
				f.push('\n');
			}
		}
		v.fmt_sql(f, pretty);
	}
}

fn fmt_two_line_separated<T: ToSql, I: IntoIterator<Item = T>>(
	into_iter: I,
	f: &mut String,
	pretty: PrettyMode,
) {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			if pretty {
				f.push('\n');
				pretty_sequence_item();
			} else {
				f.push('\n');
				f.push('\n');
			}
		}
		v.fmt_sql(f, pretty);
	}
}

/// Creates a formatting function that joins iterators with an arbitrary
/// separator.
pub fn fmt_separated_by<T: ToSql, I: IntoIterator<Item = T>>(
	separator: impl Display,
) -> impl Fn(I, &mut String, bool)  {
	move |into_iter: I, f: &mut String, pretty: PrettyMode| {
		for (i, v) in into_iter.into_iter().enumerate() {
			if i > 0 {
				write_sql!(f, "{}", separator);
			}
			v.fmt_sql(f, pretty);
		}
	}
}

// thread_local! {
// 	// Avoid `RefCell`/`UnsafeCell` by using atomic types. Access is synchronized due to
// 	// `thread_local!` so all accesses can use `Ordering::Relaxed`.

// 	/// Whether pretty-printing.
// 	static PRETTY: AtomicBool = const {AtomicBool::new(false)};
// 	/// The current level of indentation, in units of tabs.
// 	static INDENT: AtomicU32 = const{AtomicU32::new(0)};
// 	/// Whether the next formatting action should be preceded by a newline and indentation.
// 	static NEW_LINE: AtomicBool = const{AtomicBool::new(false)};
// }


// /// Returns whether pretty printing is in effect.
// pub(crate) fn is_pretty() -> bool {
// 	PRETTY.with(|pretty| pretty.load(Ordering::Relaxed))
// }

// /// If pretty printing is in effect, increments the indentation level (until the
// /// return value is dropped).
// #[must_use = "hold for the span of the indent, then drop"]
// pub(crate) fn pretty_indent() -> PrettyGuard {
// 	PrettyGuard::new(1)
// }

// /// Marks the end of an item in the sequence, after which indentation will
// /// follow if pretty printing is in effect.
// pub(crate) fn pretty_sequence_item() {
// 	// List items need a new line, but no additional indentation.
// 	NEW_LINE.with(|new_line| new_line.store(true, Ordering::Relaxed));
// }

// /// When dropped, applies the opposite increment to the current indentation
// /// level.
// pub(crate) struct PrettyGuard {
// 	increment: i8,
// }

// impl PrettyGuard {
// 	fn new(increment: i8) -> Self {
// 		Self::raw(increment);
// 		PrettyGuard {
// 			increment,
// 		}
// 	}

// 	fn raw(increment: i8) {
// 		INDENT.with(|indent| {
// 			// Equivalent to `indent += increment` if signed numbers could be added to
// 			// unsigned numbers in stable, atomic Rust.
// 			if increment >= 0 {
// 				indent.fetch_add(increment as u32, Ordering::Relaxed);
// 			} else {
// 				indent.fetch_sub(increment.unsigned_abs() as u32, Ordering::Relaxed);
// 			}
// 		});
// 		NEW_LINE.with(|new_line| new_line.store(true, Ordering::Relaxed));
// 	}
// }

// impl Drop for PrettyGuard {
// 	fn drop(&mut self) {
// 		Self::raw(-self.increment)
// 	}
// }

// impl<W: std::fmt::Write> std::fmt::Write for Pretty<W> {
// 	fn write_str(&mut self, s: &str) -> std::fmt::Result {
// 		if self.active && NEW_LINE.with(|new_line| new_line.swap(false, Ordering::Relaxed)) {
// 			// Newline.
// 			self.inner.write_char('\n')?;
// 			for _ in 0..INDENT.with(|indent| indent.load(Ordering::Relaxed)) {
// 				// One level of indentation.
// 				self.inner.write_char('\t')?;
// 			}
// 		}
// 		// What we were asked to write.
// 		self.inner.write_str(s)
// 	}
// }

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
