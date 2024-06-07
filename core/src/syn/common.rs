use super::token::Span;
use std::ops::Range;

/// A human readable location inside a string.
///
/// Locations are 1 indexed, the first character on the first line being on line 1 column 1.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Location {
	pub line: usize,
	/// In chars.
	pub column: usize,
}

impl Location {
	/// Returns the location of the start of substring in the larger input string.
	///
	/// Assumption: substr must be a subslice of input.
	pub fn of_in(substr: &str, input: &str) -> Self {
		// Bytes of input before substr.
		let offset = (substr.as_ptr() as usize)
			.checked_sub(input.as_ptr() as usize)
			.expect("tried to find location of substring in unrelated string");
		assert!(offset <= input.len(), "tried to find location of substring in unrelated string");
		// Bytes of input prior to line being iterated.
		let mut bytes_prior = 0;
		for (line_idx, (line, seperator_len)) in LineIterator::new(input).enumerate() {
			let bytes_so_far = bytes_prior + line.len() + seperator_len.unwrap_or(0) as usize;
			if bytes_so_far >= offset {
				// found line.
				let line_offset = offset - bytes_prior;

				let column = if line_offset > line.len() {
					// error is inside line terminator.
					line.chars().count() + 1
				} else {
					line[..line_offset].chars().count()
				};
				// +1 because line and column are 1 index.
				return Self {
					line: line_idx + 1,
					column: column + 1,
				};
			}
			bytes_prior = bytes_so_far;
		}
		unreachable!()
	}

	pub fn of_offset(source: &str, offset: usize) -> Self {
		assert!(offset <= source.len(), "tried to find location of substring in unrelated string");
		// Bytes of input prior to line being iterated.
		let mut bytes_prior = 0;
		for (line_idx, (line, seperator_len)) in LineIterator::new(source).enumerate() {
			let bytes_so_far = bytes_prior + line.len() + seperator_len.unwrap_or(0) as usize;
			if bytes_so_far >= offset {
				// found line.
				let line_offset = offset - bytes_prior;

				let column = if line_offset > line.len() {
					// error is inside line terminator.
					line.chars().count() + 1
				} else {
					line[..line_offset].chars().count()
				};
				// +1 because line and column are 1 index.
				return Self {
					line: line_idx + 1,
					column: column + 1,
				};
			}
			bytes_prior = bytes_so_far;
		}
		unreachable!()
	}

	pub fn of_span_start(source: &str, span: Span) -> Self {
		// Bytes of input before substr.

		let offset = span.offset as usize;
		Self::of_offset(source, offset)
	}

	pub fn of_span_end(source: &str, span: Span) -> Self {
		// Bytes of input before substr.
		let offset = span.offset as usize + span.len as usize;
		Self::of_offset(source, offset)
	}

	pub fn range_of_span(source: &str, span: Span) -> Range<Self> {
		if source.len() == span.offset as usize {
			// EOF span
			let (line_idx, column) = LineIterator::new(source)
				.map(|(l, _)| l.len())
				.enumerate()
				.last()
				.unwrap_or((0, 0));

			return Self {
				line: line_idx + 1,
				column: column + 1,
			}..Self {
				line: line_idx + 1,
				column: column + 2,
			};
		}

		// Bytes of input before substr.
		let offset = span.offset as usize;
		let end = offset + span.len as usize;

		// Bytes of input prior to line being iteratated.
		let mut bytes_prior = 0;
		let mut iterator = LineIterator::new(source).enumerate().peekable();
		let start = loop {
			let Some((line_idx, (line, seperator_offset))) = iterator.peek() else {
				panic!("tried to find location of span not belonging to string");
			};
			let bytes_so_far = bytes_prior + line.len() + seperator_offset.unwrap_or(0) as usize;
			if bytes_so_far > offset {
				// found line.
				let line_offset = offset - bytes_prior;
				let column = if line_offset > line.len() {
					line.chars().count() + 1
				} else {
					line[..line_offset.min(line.len())].chars().count()
				};
				// +1 because line and column are 1 index.
				if bytes_so_far >= end {
					// end is on the same line, finish immediatly.
					let line_offset = end - bytes_prior;
					let end_column = line[..line_offset].chars().count();
					return Self {
						line: line_idx + 1,
						column: column + 1,
					}..Self {
						line: line_idx + 1,
						column: end_column + 1,
					};
				} else {
					break Self {
						line: line_idx + 1,
						column: column + 1,
					};
				}
			}
			bytes_prior = bytes_so_far;
			iterator.next();
		};

		loop {
			let Some((line_idx, (line, seperator_offset))) = iterator.next() else {
				panic!("tried to find location of span not belonging to string");
			};
			let bytes_so_far = bytes_prior + line.len() + seperator_offset.unwrap_or(0) as usize;
			if bytes_so_far >= end {
				let line_offset = end - bytes_prior;
				let column = if line_offset > line.len() {
					line.chars().count() + 1
				} else {
					line[..line_offset.min(line.len())].chars().count()
				};
				return start..Self {
					line: line_idx + 1,
					column: column + 1,
				};
			}
			bytes_prior = bytes_so_far;
		}
	}
}

struct LineIterator<'a> {
	current: &'a str,
}

impl<'a> LineIterator<'a> {
	pub fn new(s: &'a str) -> Self {
		LineIterator {
			current: s,
		}
	}
}

impl<'a> Iterator for LineIterator<'a> {
	type Item = (&'a str, Option<u8>);

	fn next(&mut self) -> Option<Self::Item> {
		if self.current.is_empty() {
			return None;
		}
		let bytes = self.current.as_bytes();
		for i in 0..bytes.len() {
			match bytes[i] {
				b'\r' => {
					if let Some(b'\n') = bytes.get(i + 1) {
						let res = &self.current[..i];
						self.current = &self.current[i + 2..];
						return Some((res, Some(2)));
					}
					let res = &self.current[..i];
					self.current = &self.current[i + 1..];
					return Some((res, Some(1)));
				}
				0xb | 0xC | b'\n' => {
					// vertical tab VT and form feed FF.
					let res = &self.current[..i];
					self.current = &self.current[i + 1..];
					return Some((res, Some(1)));
				}
				0xc2 => {
					// next line NEL
					if bytes.get(i + 1).copied() != Some(0x85) {
						continue;
					}
					let res = &self.current[..i];
					self.current = &self.current[i + 2..];
					return Some((res, Some(2)));
				}
				0xe2 => {
					// line separator and paragraph seperator.
					if bytes.get(i + 1).copied() != Some(0x80) {
						continue;
					}
					let next_byte = bytes.get(i + 2).copied();
					if next_byte != Some(0xA8) && next_byte != Some(0xA9) {
						continue;
					}

					// vertical tab VT, next line NEL and form feed FF.
					let res = &self.current[..i];
					self.current = &self.current[i + 3..];
					return Some((res, Some(3)));
				}
				_ => {}
			}
		}
		Some((std::mem::take(&mut self.current), None))
	}
}

#[cfg(test)]
mod test {
	use super::LineIterator;

	#[test]
	fn test_line_iterator() {
		let lines = "foo\nbar\r\nfoo\rbar\u{000B}foo\u{000C}bar\u{0085}foo\u{2028}bar\u{2029}\n";
		let mut iterator = LineIterator::new(lines);
		assert_eq!(iterator.next(), Some(("foo", Some(1))));
		assert_eq!(iterator.next(), Some(("bar", Some(2))));
		assert_eq!(iterator.next(), Some(("foo", Some(1))));
		assert_eq!(iterator.next(), Some(("bar", Some(1))));
		assert_eq!(iterator.next(), Some(("foo", Some(1))));
		assert_eq!(iterator.next(), Some(("bar", Some(2))));
		assert_eq!(iterator.next(), Some(("foo", Some(3))));
		assert_eq!(iterator.next(), Some(("bar", Some(3))));
		assert_eq!(iterator.next(), Some(("", Some(1))));
		assert_eq!(iterator.next(), None);
	}
}
