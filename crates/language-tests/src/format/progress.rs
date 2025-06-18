use crate::tests::report::TestGrade;
use crate::{cli::ColorMode, format::ansi};
use std::io::{self, BufWriter, IsTerminal as _, Stderr, Write};

pub struct Progress<I, W> {
	items: Vec<(I, String)>,
	color_mode: ColorMode,
	use_ansii: bool,
	writer: W,
	finised: usize,
	expected: usize,
}

impl<I, W> Progress<I, W> {
	pub fn from_writer(writer: W, use_ansii: bool, color_mode: ColorMode, expected: usize) -> Self {
		Progress {
			items: Vec::new(),
			use_ansii,
			color_mode,
			writer,
			finised: 0,
			expected,
		}
	}
}

impl<I> Progress<I, BufWriter<Stderr>> {
	pub fn from_stderr(expected: usize, color_mode: ColorMode) -> Self {
		Self::from_writer(
			BufWriter::new(io::stderr()),
			io::stderr().is_terminal(),
			color_mode,
			expected,
		)
	}
}

impl<I: Eq, W: Write> Progress<I, W> {
	fn use_color(&self) -> bool {
		match self.color_mode {
			ColorMode::Always => true,
			ColorMode::Never => false,
			ColorMode::Auto => self.use_ansii,
		}
	}

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

	fn write_running(&mut self, name: &str) -> Result<(), io::Error> {
		if self.use_color() {
			self.writer.write_fmt(format_args!(
				ansi!(blue, "  Running", reset_format, " {:<80}", reset_format, "\n"),
				name
			))
		} else {
			self.writer.write_fmt(format_args!("Running {}\n", name))
		}
	}

	fn write_finished(&mut self, name: &str, result: TestGrade) -> Result<(), io::Error> {
		if self.use_color() {
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
				name, res
			))
		} else {
			self.writer.write_fmt(format_args!("Finished {} ", name))?;
			let res = match result {
				TestGrade::Success => "Success ✓\n",
				TestGrade::Warning => "Warning ⚠\n",
				TestGrade::Failed => "Error ☓\n",
			};
			self.writer.write_all(res.as_bytes())
		}
	}

	pub fn start_item(&mut self, id: I, name: &str) -> Result<(), io::Error> {
		if self.items.iter().any(|x| x.0 == id) {
			return Ok(());
		}

		if self.use_ansii {
			self.writer.write_all(b"\r")?;
			self.writer.write_all(ansi!(clear_line).as_bytes())?;
		}

		self.write_running(name)?;
		self.items.push((id, name.to_string()));
		if self.use_ansii {
			self.write_bar()?;
		}
		self.writer.flush()
	}

	pub fn finish_item(&mut self, id: I, result: TestGrade) -> Result<(), io::Error> {
		let Some(idx) = self.items.iter().position(|x| x.0 == id) else {
			return Ok(());
		};

		let lines = self.items.len();
		let (_, name) = self.items.remove(idx);

		if self.use_ansii {
			self.writer.write_all(b"\r")?;
			for _ in 0..lines {
				self.writer.write_all(ansi!(up).as_bytes())?;
			}
			self.writer.write_all(ansi!(clear_after).as_bytes())?;
		}

		self.write_finished(&name, result)?;

		if self.use_ansii {
			for (_, name) in self.items.iter() {
				self.writer.write_fmt(format_args!(
					ansi!(blue, "  Running", reset_format, " {}\n"),
					name
				))?;
			}
		}
		self.finised += 1;
		if self.use_ansii {
			self.write_bar()?;
		}
		self.writer.flush()
	}
}
