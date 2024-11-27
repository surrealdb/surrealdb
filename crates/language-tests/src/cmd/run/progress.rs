use std::{
	collections::{hash_map::Entry, HashMap},
	io::{self, BufWriter, Stderr, StderrLock, Write},
	time::Duration,
};

use atty::Stream;

use crate::format::ansi;

use super::report::TestGrade;

pub struct Progress<W> {
	items: Vec<String>,
	use_ansii: bool,
	writer: W,
	finised: usize,
	expected: usize,
}

impl<W> Progress<W> {
	pub fn from_writer(writer: W, use_ansii: bool, expected: usize) -> Self {
		Progress {
			items: Vec::new(),
			use_ansii,
			writer,
			finised: 0,
			expected,
		}
	}
}

impl Progress<BufWriter<Stderr>> {
	pub fn from_stderr(expected: usize) -> Self {
		Self::from_writer(BufWriter::new(io::stderr()), atty::is(Stream::Stderr), expected)
	}
}

impl<W: Write> Progress<W> {
	fn write_bar(&mut self) -> Result<(), io::Error> {
		const TOTAL_WIDTH: usize = 80;

		let num_width = (self.expected.max(1).ilog10() + 1) as usize;
		self.writer.write_fmt(format_args!(
			" {:>width$}/{:>width$} ",
			self.finised,
			self.expected,
			width = num_width
		))?;
		self.writer.write_all(b"[")?;

		let used_characters = 5 + num_width * 2;
		let remaining_width = TOTAL_WIDTH - used_characters;

		let ratio = (self.finised as f64 / self.expected as f64).min(1.0);
		let bar_width = ((remaining_width as f64 * ratio) as usize).max(1);
		for _ in 0..(bar_width - 1) {
			self.writer.write_all(b"=")?;
		}
		self.writer.write_all(b">")?;
		for _ in 0..(remaining_width - bar_width) {
			self.writer.write_all(b" ")?;
		}
		self.writer.write_all(b"]")
	}

	pub fn start_item(&mut self, name: &str) -> Result<(), io::Error> {
		if self.items.iter().any(|x| x == name) {
			return Ok(());
		}

		if !self.use_ansii {
			self.writer.write_fmt(format_args!("Running {}\n", name))?;
			return self.writer.flush();
		}

		self.writer.write_all(ansi!("\r", clear_line).as_bytes())?;
		self.writer.write_fmt(format_args!(
			ansi!(blue, "  Running", reset_format, " {}", reset_format, "\n"),
			name
		))?;
		self.items.push(name.to_string());
		self.write_bar()?;
		return self.writer.flush();
	}

	pub fn finish_item(&mut self, name: &str, result: TestGrade) -> Result<(), io::Error> {
		let Some(idx) = self.items.iter().position(|x| x == name) else {
			return Ok(());
		};

		if !self.use_ansii {
			self.items.remove(idx);
			self.writer.write_fmt(format_args!("Finished {}", name))?;
			let res = match result {
				TestGrade::Success => "Success ✓\n",
				TestGrade::Warning => "Warning ⚠\n",
				TestGrade::Failed => "Error ☓\n",
			};
			self.writer.write_all(res.as_bytes())?;
			return self.writer.flush();
		}

		let lines = self.items.len();
		let item = self.items.remove(idx);

		self.writer.write_all(b"\r")?;
		for _ in 0..lines {
			self.writer.write_all(ansi!(up).as_bytes())?;
		}

		self.writer.write_all(ansi!(clear_after).as_bytes())?;

		let res = match result {
			TestGrade::Success => {
				ansi!(green, "Success ✓", reset_format)
			}
			TestGrade::Warning => {
				ansi!(yellow, "Warning ⚠", reset_format)
			}
			TestGrade::Failed => {
				ansi!(red, "Error ☓", reset_format)
			}
		};

		self.writer.write_fmt(format_args!(
			ansi!(blue, " Finished", reset_format, " {} {}\n"),
			item, res
		))?;

		for name in self.items.iter() {
			self.writer
				.write_fmt(format_args!(ansi!(blue, "  Running", reset_format, " {}\n"), name))?;
		}
		self.finised += 1;
		self.write_bar()?;
		self.writer.flush()
	}
}
