use std::cell::Cell;
use std::fmt::{self, Display, Formatter};

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
