//! Module for rendering errors onto source code.

use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;

use super::{Location, MessageKind};

#[derive(Clone, Debug)]
pub struct RenderedError {
	pub errors: Vec<String>,
	pub snippets: Vec<Snippet>,
}

impl RenderedError {
	/// Offset the snippet locations within the rendered error by a given number
	/// of lines and columns.
	///
	/// The column offset is only applied to the any snippet which is at line 1
	pub fn offset_location(mut self, line: usize, col: usize) -> Self {
		for s in self.snippets.iter_mut() {
			if s.location.line == 1 {
				s.location.column += col;
			}
			s.location.line += line
		}
		self
	}
}

impl fmt::Display for RenderedError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self.errors.len().cmp(&1) {
			Ordering::Equal => writeln!(f, "{}", self.errors[0])?,
			Ordering::Greater => {
				writeln!(f, "- {}", self.errors[0])?;
				writeln!(f, "caused by:")?;
				for e in &self.errors[2..] {
					writeln!(f, "    - {}", e)?
				}
			}
			Ordering::Less => {}
		}
		for s in &self.snippets {
			writeln!(f, "{s}")?;
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
	/// The amount of characters that are part of area to be pointed to.
	length: usize,
	/// A possible explanation for this snippet.
	label: Option<String>,
	/// The kind of snippet,
	// Unused for now but could in the future be used to color snippets.
	#[expect(dead_code)]
	kind: MessageKind,
}

impl Snippet {
	/// How long with the source line have to be before it gets truncated.
	const MAX_SOURCE_DISPLAY_LEN: usize = 80;
	/// How far the will have to be in the source line before everything before
	/// it gets truncated.
	const MAX_ERROR_LINE_OFFSET: usize = 50;

	pub fn from_source_location(
		source: &str,
		location: Location,
		explain: Option<&'static str>,
		kind: MessageKind,
	) -> Self {
		let line = source.split('\n').nth(location.line - 1).unwrap();
		let (line, truncation, offset) = Self::truncate_line(line, location.column - 1);

		Snippet {
			source: line.to_owned(),
			truncation,
			location,
			offset,
			length: 1,
			label: explain.map(|x| x.into()),
			kind,
		}
	}

	pub fn from_source_location_range(
		source: &str,
		location: Range<Location>,
		explain: Option<&str>,
		kind: MessageKind,
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
			label: explain.map(|x| x.into()),
			kind,
		}
	}

	/// Trims whitespace of an line and additionally truncates the string around
	/// the target_col_offset if it is too long.
	///
	/// returns the trimmed string, how it is truncated, and the offset into
	/// truncated the string where the target_col is located.
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
		writeln!(f, "--> [{}:{}]", self.location.line, self.location.column)?;

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
		if let Some(ref explain) = self.label {
			write!(f, "{explain}")?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod test {
	use super::{RenderedError, Snippet, Truncation};
	use crate::syn::error::{Location, MessageKind};
	use crate::syn::token::Span;

	#[test]
	fn truncate_whitespace() {
		let source = "\n\n\n\t      $     \t";
		let offset = source.char_indices().find(|(_, c)| *c == '$').unwrap().0;

		let location = Location::range_of_span(
			source,
			Span {
				offset: offset as u32,
				len: 1,
			},
		);

		let snippet =
			Snippet::from_source_location(source, location.start, None, MessageKind::Error);
		assert_eq!(snippet.truncation, Truncation::None);
		assert_eq!(snippet.offset, 0);
		assert_eq!(snippet.source.as_str(), "$");
	}

	#[test]
	fn truncate_start() {
		let source = "     aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa $     \t";
		let offset = source.char_indices().find(|(_, c)| *c == '$').unwrap().0;

		let location = Location::range_of_span(
			source,
			Span {
				offset: offset as u32,
				len: 1,
			},
		);

		let snippet =
			Snippet::from_source_location(source, location.start, None, MessageKind::Error);
		assert_eq!(snippet.truncation, Truncation::Start);
		assert_eq!(snippet.offset, 10);
		assert_eq!(snippet.source.as_str(), "aaaaaaaaa $");
	}

	#[test]
	fn truncate_end() {
		let source = "\n\n  a $ aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa    \t";
		let offset = source.char_indices().find(|(_, c)| *c == '$').unwrap().0;

		let location = Location::range_of_span(
			source,
			Span {
				offset: offset as u32,
				len: 1,
			},
		);

		let snippet =
			Snippet::from_source_location(source, location.start, None, MessageKind::Error);
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

		let location = Location::range_of_span(
			source,
			Span {
				offset: offset as u32,
				len: 1,
			},
		);

		let snippet =
			Snippet::from_source_location(source, location.start, None, MessageKind::Error);
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
			errors: vec!["some_error".to_string()],
			snippets: vec![Snippet {
				source: "hallo error".to_owned(),
				truncation: Truncation::Both,
				location: Location {
					line: 4,
					column: 10,
				},
				offset: 6,
				length: 5,
				label: Some("this is wrong".to_owned()),
				kind: MessageKind::Error,
			}],
		};

		let error_string = format!("{}", error);
		let expected = r#"some_error
 --> [4:10]
  |
4 | ...hallo error...
  |          ^^^^^ this is wrong
"#;
		assert_eq!(error_string, expected)
	}
}
