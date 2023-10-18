use std::fmt;

use super::Location;

#[derive(Clone, Debug)]
pub struct RenderedError {
	pub text: String,
	pub snippets: Vec<Snippet>,
}

impl fmt::Display for RenderedError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		writeln!(f, "{}", self.text)?;
		for s in self.snippets.iter() {
			writeln!(f, "{}", s)?;
		}
		Ok(())
	}
}

/// Whether the snippet was truncated.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum Truncation {
	/// The snippet wasn't truncated
	None,
	/// The snippet was truncated at the start
	Start,
	/// The snippet was truncated at the end
	End,
	/// Both sided of the snippet where truncated.
	Both,
}

/// A piece of the source code with a location and an optional explanation.
#[derive(Clone, Debug)]
pub struct Snippet {
	/// The part of the original source code,
	source: String,
	/// Whether part of the source line was truncated.
	truncation: Truncation,
	/// The location of the snippet in the original source code.
	location: Location,
	/// The offset, in chars, into the snippet where the location is.
	offset: usize,
	/// A possible explanation for this snippet.
	explain: Option<String>,
}

impl Snippet {
	/// How long with the source line have to be before it gets truncated.
	const MAX_SOURCE_DISPLAY_LEN: usize = 80;
	/// How far the will have to be in the source line before everything before it gets truncated.
	const MAX_ERROR_LINE_OFFSET: usize = 50;

	pub fn from_source_location(
		source: &str,
		location: Location,
		explain: Option<&'static str>,
	) -> Self {
		let line = source.split('\n').nth(location.line - 1).unwrap();
		let (line, truncation, offset) = Self::truncate_line(line, location.column - 1);

		Snippet {
			source: line.to_owned(),
			truncation,
			location,
			offset,
			explain: explain.map(|x| x.into()),
		}
	}

	/// Trims whitespace of an line and additionally truncates a string if it is too long.
	fn truncate_line(mut line: &str, around_offset: usize) -> (&str, Truncation, usize) {
		let full_line_length = line.chars().count();
		line = line.trim_start();
		// Saturate in case the error occurred in invalid leading whitespace.
		let mut offset = around_offset.saturating_sub(full_line_length - line.chars().count());
		line = line.trim_end();
		let mut truncation = Truncation::None;

		if around_offset > Self::MAX_ERROR_LINE_OFFSET {
			// Actual error is to far to the right, just truncated everything to the left.
			// show some prefix for some extra context.
			let extra_offset = around_offset - 10;
			let mut chars = line.chars();
			for _ in 0..extra_offset {
				chars.next();
			}
			offset -= extra_offset;
			line = chars.as_str();
			truncation = Truncation::Start;
		}

		if line.chars().count() > Self::MAX_SOURCE_DISPLAY_LEN {
			// Line is too long, truncate to source
			let mut size = Self::MAX_SOURCE_DISPLAY_LEN - 3;
			if truncation == Truncation::Start {
				truncation = Truncation::Both;
				size -= 3;
			} else {
				truncation = Truncation::End
			}

			// Unwrap because we just checked if the line length is longer then this.
			let truncate_index = line.char_indices().nth(size).unwrap().0;
			line = &line[..truncate_index];
		}

		(line, truncation, offset)
	}
}

impl fmt::Display for Snippet {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		// extra spacing for the line number
		let spacing = self.location.line.ilog10() as usize + 1;
		writeln!(f, "{:>spacing$} |", "")?;
		write!(f, "{:>spacing$} | ", self.location.line)?;
		match self.truncation {
			Truncation::None => {
				writeln!(f, "{}", self.source)?;
			}
			Truncation::Start => {
				writeln!(f, "...{}", self.source)?;
			}
			Truncation::End => {
				writeln!(f, "{}...", self.source)?;
			}
			Truncation::Both => {
				writeln!(f, "...{}...", self.source)?;
			}
		}
		let error_offset = self.offset
			+ if matches!(self.truncation, Truncation::Start | Truncation::Both) {
				3
			} else {
				0
			};
		write!(f, "{:>spacing$} | {:>error_offset$} ", "", "^",)?;
		if let Some(ref explain) = self.explain {
			write!(f, "{explain}")?;
		}
		Ok(())
	}
}
