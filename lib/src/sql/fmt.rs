use std::cell::Cell;
use std::fmt::{self, Display, Formatter, Write};
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

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

	/// Formats values with a comma and a space separating them or, if pretty printing is in
	/// effect, a comma, a newline, and indentation.
	pub(crate) fn pretty_comma_separated(into_iter: I) -> Self {
		Self::new(
			into_iter,
			if is_pretty() {
				fmt_pretty_comma_separated
			} else {
				fmt_comma_separated
			},
		)
	}

	/// Formats values with a new line separating them.
	pub(crate) fn pretty_new_line_separated(into_iter: I) -> Self {
		Self::new(into_iter, fmt_new_line_separated)
	}
}

fn fmt_comma_separated<T: Display>(
	into_iter: impl IntoIterator<Item = T>,
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

fn fmt_pretty_comma_separated<T: Display>(
	into_iter: impl IntoIterator<Item = T>,
	f: &mut Formatter,
) -> fmt::Result {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			// One of the few cases where the raw string data depends on is pretty i.e. we don't
			// need a space after the comma if we are going to have a newline.
			f.write_str(",")?;
			pretty_sequence_item();
		}
		Display::fmt(&v, f)?;
	}
	Ok(())
}

fn fmt_new_line_separated<T: Display>(
	into_iter: impl IntoIterator<Item = T>,
	f: &mut Formatter,
) -> fmt::Result {
	for (i, v) in into_iter.into_iter().enumerate() {
		if i > 0 {
			// One of the few cases where the raw string data depends on is pretty i.e. we don't
			// need a space after the comma if we are going to have a newline.
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

thread_local! {
	/// Whether pretty-printing.
	static PRETTY: AtomicBool = AtomicBool::new(false);
	/// The current level of indentation, in units of tabs.
	static INDENT: AtomicU32 = AtomicU32::new(0);
	/// Whether the next formatting action should be preceded by a newline and indentation.
	static NEW_LINE: AtomicBool = AtomicBool::new(false);
}

pub(crate) struct Pretty<W: std::fmt::Write> {
	inner: W,
	/// This is the active pretty printer, responsible for injecting formatting.
	active: bool,
}

impl<W: std::fmt::Write> Pretty<W> {
	#[allow(unused)]
	pub fn new(inner: W) -> Self {
		Self::conditional(inner, true)
	}

	pub fn conditional(inner: W, enable: bool) -> Self {
		let pretty_started_here = enable
			&& PRETTY.with(|pretty| {
				pretty.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok()
			});
		if pretty_started_here {
			NEW_LINE.with(|new_line| new_line.store(false, Ordering::Relaxed));
			INDENT.with(|indent| indent.store(0, Ordering::Relaxed));
		}
		Self {
			inner,
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
				debug_assert!(pretty.load(Ordering::Relaxed), "pretty status changed unexpectedly");
				pretty.store(false, Ordering::Relaxed);
			});
		}
	}
}

pub(crate) fn is_pretty() -> bool {
	PRETTY.with(|pretty| pretty.load(Ordering::Relaxed))
}

/// If pretty printing is in effect, increments the indentation level (until the return value
/// is dropped).
#[must_use = "hold for the span of the indent, then drop"]
pub(crate) fn pretty_indent() -> PrettyGuard {
	PrettyGuard::new(1)
}

/// Marks the end of an item in the sequence, after which indentation will follow during pretty printing.
pub(crate) fn pretty_sequence_item() {
	// List items need a new line, but no additional indentation.
	// We only care about the side-effects so forget the guard.
	mem::forget(PrettyGuard::new(0))
}

pub(crate) struct PrettyGuard {
	increment: i8,
}

impl PrettyGuard {
	fn new(increment: i8) -> Self {
		INDENT.with(|indent| {
			if increment >= 0 {
				indent.fetch_add(increment as u32, Ordering::Relaxed);
			} else {
				indent.fetch_sub(increment.unsigned_abs() as u32, Ordering::Relaxed);
			}
		});
		NEW_LINE.with(|new_line| new_line.store(true, Ordering::Relaxed));
		PrettyGuard {
			increment,
		}
	}
}

impl Drop for PrettyGuard {
	fn drop(&mut self) {
		// Use Self::new for the side effects only.
		mem::forget(Self::new(-self.increment));
	}
}

impl<W: std::fmt::Write> std::fmt::Write for Pretty<W> {
	fn write_str(&mut self, s: &str) -> std::fmt::Result {
		if self.active && NEW_LINE.with(|new_line| new_line.swap(false, Ordering::Relaxed)) {
			self.inner.write_char('\n')?;
			for _ in 0..INDENT.with(|indent| indent.load(Ordering::Relaxed)) {
				self.inner.write_char('\t')?;
			}
		}
		self.inner.write_str(s)
	}
}

#[cfg(test)]
mod tests {
	use crate::sql::parse;

	#[test]
	fn pretty_query() {
		let query = parse("SELECT * FROM {foo: [1, 2, 3]};").unwrap();
		assert_eq!(format!("{}", query), "SELECT * FROM { foo: [1, 2, 3] };");
		assert_eq!(
			format!("{:#}", query),
			"SELECT * FROM {\n\tfoo: [\n\t\t1,\n\t\t2,\n\t\t3\n\t]\n};"
		);
	}
}
