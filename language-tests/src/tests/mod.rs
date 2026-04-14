pub mod case;
pub mod cmp;
pub mod report;
pub mod run;
pub mod schema;

use std::fmt::Write;
use std::io::{self, IsTerminal as _};
use std::sync::Arc;

pub use case::{CaseSet, Origin};
pub use run::{RunSetBuilder, TestRun};

use crate::cli::ColorMode;
use crate::format::{IndentFormatter, ansi};

/// An error that happened during loading of a test case.
#[derive(Debug)]
pub struct TestLoadError {
	pub origin: Arc<Origin>,
	pub error: anyhow::Error,
}

impl TestLoadError {
	pub fn display(&self, color: ColorMode) {
		let use_color = match color {
			ColorMode::Always => true,
			ColorMode::Never => false,
			ColorMode::Auto => io::stdout().is_terminal(),
		};

		type Fmt<'a> = IndentFormatter<&'a mut String>;

		let mut buffer = String::new();
		let mut f = Fmt::new(&mut buffer, 2);
		f.indent(|f| {
			if use_color {
				writeln!(
					f,
					ansi!(
						" ==> ",
						red,
						"Error",
						reset_format,
						" loading ",
						bold,
						"{}",
						reset_format,
						":"
					),
					self.origin.path
				)?
			} else {
				writeln!(f, " ==> Error Loading {}:", self.origin.path)?
			}

			f.indent(|f| writeln!(f, "{:?}", self.error))
		})
		.unwrap();

		println!("{buffer}");
	}
}
