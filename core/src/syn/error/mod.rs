use std::{fmt, ops::Range};

use super::common::Location;

mod nom_error;
pub use nom_error::ParseError;

#[derive(Clone, Debug)]
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
pub struct Snippet {
	/// The part of the original source code,
	source: String,
	/// Whether part of the source line was truncated.
	truncation: Truncation,
	/// The location of the snippet in the original source code.
	location: Location,
	/// The offset, in chars, into the snippet where the location is.
	offset: usize,
	/// The amount of characters that are part of area to be pointed to.
	length: usize,
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
			length: 1,
			explain: explain.map(|x| x.into()),
		}
	}

	pub fn from_source_location_range(
		source: &str,
		location: Range<Location>,
		explain: Option<&'static str>,
	) -> Self {
		let line = source.split('\n').nth(location.start.line - 1).unwrap();
		let (line, truncation, offset) = Self::truncate_line(line, location.start.column - 1);
		let length = if location.start.line == location.end.line {
			location.end.column - location.start.column
		} else {
			1
		};
		Snippet {
			source: line.to_owned(),
			truncation,
			location: location.start,
			offset,
			length,
			explain: explain.map(|x| x.into()),
		}
	}

	/// Trims whitespace of an line and additionally truncates the string around the target_col_offset if it is too long.
	///
	/// returns the trimmed string, how it is truncated, and the offset into truncated the string where the target_col is located.
	fn truncate_line(mut line: &str, target_col: usize) -> (&str, Truncation, usize) {
		// offset in characters from the start of the string.
		let mut offset = 0;
		for (i, (idx, c)) in line.char_indices().enumerate() {
			// if i == target_col the error is in the leading whitespace. so return early.
			if i == target_col || !c.is_whitespace() {
				line = &line[idx..];
				offset = target_col - i;
				break;
			}
		}

		line = line.trim_end();
		// truncation none because only truncated non-whitespace counts.
		let mut truncation = Truncation::None;

		if offset > Self::MAX_ERROR_LINE_OFFSET {
			// Actual error is to far to the right, just truncated everything to the left.
			// show some prefix for some extra context.
			let too_much_offset = offset - 10;
			let mut chars = line.chars();
			for _ in 0..too_much_offset {
				chars.next();
			}
			offset = 10;
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
		for _ in 0..spacing {
			f.write_str(" ")?;
		}
		f.write_str(" |\n")?;
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
		for _ in 0..spacing {
			f.write_str(" ")?;
		}
		f.write_str(" | ")?;
		for _ in 0..error_offset {
			f.write_str(" ")?;
		}
		for _ in 0..self.length {
			write!(f, "^")?;
		}
		write!(f, " ")?;
		if let Some(ref explain) = self.explain {
			write!(f, "{explain}")?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod test {
	use super::{RenderedError, Snippet, Truncation};
	use crate::syn::common::Location;

	#[test]
	fn truncate_whitespace() {
		let source = "\n\n\n\t      $     \t";
		let offset = source.char_indices().find(|(_, c)| *c == '$').unwrap().0;
		let error = &source[offset..];

		let location = Location::of_in(error, source);

		let snippet = Snippet::from_source_location(source, location, None);
		assert_eq!(snippet.truncation, Truncation::None);
		assert_eq!(snippet.offset, 0);
		assert_eq!(snippet.source.as_str(), "$");
	}

	#[test]
	fn truncate_start() {
		let source = "     aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa $     \t";
		let offset = source.char_indices().find(|(_, c)| *c == '$').unwrap().0;
		let error = &source[offset..];

		let location = Location::of_in(error, source);

		let snippet = Snippet::from_source_location(source, location, None);
		assert_eq!(snippet.truncation, Truncation::Start);
		assert_eq!(snippet.offset, 10);
		assert_eq!(snippet.source.as_str(), "aaaaaaaaa $");
	}

	#[test]
	fn truncate_end() {
		let source = "\n\n  a $ aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa    \t";
		let offset = source.char_indices().find(|(_, c)| *c == '$').unwrap().0;
		let error = &source[offset..];

		let location = Location::of_in(error, source);

		let snippet = Snippet::from_source_location(source, location, None);
		assert_eq!(snippet.truncation, Truncation::End);
		assert_eq!(snippet.offset, 2);
		assert_eq!(
			snippet.source.as_str(),
			"a $ aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		);
	}

	#[test]
	fn truncate_both() {
		let source = "\n\n\n\n  aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa $ aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa   \t";
		let offset = source.char_indices().find(|(_, c)| *c == '$').unwrap().0;
		let error = &source[offset..];

		let location = Location::of_in(error, source);

		let snippet = Snippet::from_source_location(source, location, None);
		assert_eq!(snippet.truncation, Truncation::Both);
		assert_eq!(snippet.offset, 10);
		assert_eq!(
			snippet.source.as_str(),
			"aaaaaaaaa $ aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		);
	}

	#[test]
	fn render() {
		let error = RenderedError {
			text: "some_error".to_string(),
			snippets: vec![Snippet {
				source: "hallo error".to_owned(),
				truncation: Truncation::Both,
				location: Location {
					line: 4,
					column: 10,
				},
				offset: 6,
				length: 5,
				explain: Some("this is wrong".to_owned()),
			}],
		};

		let error_string = format!("{}", error);
		let expected = r#"some_error
  |
4 | ...hallo error...
  |          ^^^^^ this is wrong
"#;
		assert_eq!(error_string, expected)
	}
}
