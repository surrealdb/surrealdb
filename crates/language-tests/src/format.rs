/// Simple macro for composing ansi escape codes.
macro_rules! ansi {
    ($first:ident $(, $($t:tt)+)?) => {
        concat!(ansi!(@t $first) $(, ansi!($($t)*))?)
    };

    ($first:literal $(, $($t:tt)+)?) => {
        concat!($first $(, ansi!($($t)*))?)
    };

    (@t up) => {
        "\x1b[1A"
    };
    (@t down) => {
        "\x1b[1B"
    };
    (@t clear_line) => {
        "\x1b[2K"
    };
    (@t clear_after) => {
        "\x1b[0J"
    };
    (@t reset_format) => {
        "\x1b[0m"
    };
    (@t green) => {
        "\x1b[32m"
    };
    (@t red) => {
        "\x1b[31m"
    };
    (@t yellow) => {
        "\x1b[33m"
    };
    (@t blue) => {
        "\x1b[34m"
    };
    (@t bold) => {
        "\x1b[1m"
    };
}
use std::fmt;

pub(crate) use ansi;

/// A wrapper around a type implementing std::fmt::Write for indenting lines.
pub struct IndentFormatter<W> {
	line_buffer: String,
	w: W,
	depth: usize,
	indent_size: usize,
}

impl<W> IndentFormatter<W>
where
	W: fmt::Write,
{
	pub fn new(writer: W, indent_size: usize) -> Self {
		IndentFormatter {
			line_buffer: String::new(),
			w: writer,
			depth: 0,
			indent_size,
		}
	}

	pub fn increase_depth(&mut self) {
		self.depth += 1;
	}

	pub fn indent<F>(&mut self, f: F) -> fmt::Result
	where
		F: FnOnce(&mut IndentFormatter<W>) -> fmt::Result,
	{
		self.depth += 1;
		let res = f(self);
		self.depth -= 1;
		res
	}

	pub fn write_str(&mut self, str: &str) -> fmt::Result {
		let mut lines = str.split("\n");

		if let Some(x) = lines.next() {
			self.line_buffer.push_str(x);
		}

		for l in lines {
			for _ in 0..(self.indent_size * self.depth) {
				self.w.write_char(' ')?;
			}
			self.w.write_str(&self.line_buffer)?;
			self.w.write_char('\n')?;
			self.line_buffer.clear();
			self.line_buffer.push_str(l);
		}

		Ok(())
	}
}

impl<W> fmt::Write for IndentFormatter<W>
where
	W: fmt::Write,
{
	fn write_str(&mut self, s: &str) -> fmt::Result {
		self.write_str(s)
	}
}
