use std::ops::Range;

use crate::syn::token::Span;

/// A human readable location inside a string.
///
/// Locations are 1 indexed, the first character on the first line being on line
/// 1 column 1.
#[derive(Clone, Copy, Debug)]
pub struct Location {
	pub line: usize,
	/// In chars.
	pub column: usize,
}

/// Safety: b must be a substring of a.
unsafe fn str_offset(a: &str, b: &str) -> usize {
	unsafe { b.as_ptr().offset_from(a.as_ptr()) as usize }
}

impl Location {
	fn range_of_source_end(source: &str) -> Range<Self> {
		let (line, column) = source
			.lines()
			.enumerate()
			.last()
			.map(|(idx, line)| {
				let idx = idx + 1;
				let line_idx = line.chars().count().max(1);
				(idx, line_idx)
			})
			.unwrap_or((1, 1));

		Self {
			line,
			column,
		}..Self {
			line,
			column: column + 1,
		}
	}
	pub fn range_of_span(source: &str, span: Span) -> Range<Self> {
		if source.len() <= span.offset as usize {
			return Self::range_of_source_end(source);
		}

		let mut prev_line = "";
		let mut lines = source.lines().enumerate().peekable();
		// Bytes of input prior to line being iteratated.
		let start_offset = span.offset as usize;
		let start = loop {
			let Some((line_idx, line)) = lines.peek().copied() else {
				// Couldn't find the line, give up and return the last
				return Self::range_of_source_end(source);
			};
			// Safety: line originates from source so it is a substring so calling
			// str_offset is valid.
			let line_offset = unsafe { str_offset(source, line) };

			if start_offset < line_offset {
				// Span is inside the previous line terminator, point to the end of the line.
				let len = prev_line.chars().count();
				break Self {
					line: line_idx,
					column: len + 1,
				};
			}

			if (line_offset..(line_offset + line.len())).contains(&start_offset) {
				let column_offset = start_offset - line_offset;
				let column = line
					.char_indices()
					.enumerate()
					.find(|(_, (char_idx, _))| *char_idx >= column_offset)
					.map(|(l, _)| l)
					.unwrap_or_else(|| {
						// give up, just point to the end.
						line.chars().count()
					});
				break Self {
					line: line_idx + 1,
					column: column + 1,
				};
			}

			lines.next();
			prev_line = line;
		};

		let end_offset = span.offset as usize + span.len as usize;
		let end = loop {
			let Some((line_idx, line)) = lines.peek().copied() else {
				// Couldn't find the line, give up and return the last
				break Self::range_of_source_end(source).end;
			};
			// Safety: line originates from source so it is a substring so calling
			// str_offset is valid.
			let line_offset = unsafe { str_offset(source, line) };

			if end_offset < line_offset {
				// Span is inside the previous line terminator, point to the end of the line.
				let len = prev_line.chars().count();
				break Self {
					line: line_idx,
					column: len + 1,
				};
			}

			if (line_offset..(line_offset + line.len())).contains(&end_offset) {
				let column_offset = end_offset - line_offset;
				let column = line
					.char_indices()
					.enumerate()
					.find(|(_, (char_idx, _))| *char_idx >= column_offset)
					.map(|(l, _)| l)
					.unwrap_or_else(|| {
						// give up, just point to the end.
						line.chars().count()
					});
				break Self {
					line: line_idx + 1,
					column: column + 1,
				};
			}

			lines.next();
			prev_line = line;
		};

		start..end
	}
}
