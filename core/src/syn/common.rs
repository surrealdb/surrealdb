/// A human readable location inside a string.
///
/// Locations are 1 indexed, the first character on the first line being on line 1 column 1.
#[derive(Clone, Copy, Debug)]
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
		// Bytes of input prior to line being iteratated.
		let mut bytes_prior = 0;
		for (line_idx, line) in input.split('\n').enumerate() {
			// +1 for the '\n'
			let bytes_so_far = bytes_prior + line.len() + 1;
			if bytes_so_far > offset {
				// found line.
				let line_offset = offset - bytes_prior;
				let column = line[..line_offset].chars().count();
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

	#[cfg(feature = "experimental_parser")]
	pub fn of_span_start(source: &str, span: Span) -> Self {
		// Bytes of input before substr.
		let offset = span.offset as usize;
		// Bytes of input prior to line being iteratated.
		let mut bytes_prior = 0;
		for (line_idx, line) in source.split('\n').enumerate() {
			// +1 for the '\n'
			let bytes_so_far = bytes_prior + line.len() + 1;
			if bytes_so_far > offset {
				// found line.
				let line_offset = offset - bytes_prior;
				let column = line[..line_offset].chars().count();
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

	#[cfg(feature = "experimental_parser")]
	pub fn of_span_end(source: &str, span: Span) -> Self {
		// Bytes of input before substr.
		let offset = span.offset as usize + span.len as usize;
		// Bytes of input prior to line being iteratated.
		let mut bytes_prior = 0;
		for (line_idx, line) in source.split('\n').enumerate() {
			// +1 for the '\n'
			let bytes_so_far = bytes_prior + line.len() + 1;
			if bytes_so_far > offset {
				// found line.
				let line_offset = offset - bytes_prior;
				let column = line[..line_offset].chars().count();
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

	#[cfg(feature = "experimental_parser")]
	pub fn range_of_span(source: &str, span: Span) -> Range<Self> {
		// Bytes of input before substr.
		let offset = span.offset as usize;
		let end = offset + span.len as usize;

		// Bytes of input prior to line being iteratated.
		let mut bytes_prior = 0;
		let mut iterator = source.split('\n').enumerate();
		let start = loop {
			let Some((line_idx, line)) = iterator.next() else {
				panic!("tried to find location of span not belonging to string");
			};
			// +1 for the '\n'
			let bytes_so_far = bytes_prior + line.len() + 1;
			if bytes_so_far > offset {
				// found line.
				let line_offset = offset - bytes_prior;
				let column = line[..line_offset].chars().count();
				// +1 because line and column are 1 index.
				if bytes_so_far > end {
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
		};

		loop {
			let Some((line_idx, line)) = iterator.next() else {
				panic!("tried to find location of span not belonging to string");
			};
			// +1 for the '\n'
			let bytes_so_far = bytes_prior + line.len() + 1;
			if bytes_so_far > end {
				let line_offset = end - bytes_prior;
				let column = line[..line_offset].chars().count();
				return start..Self {
					line: line_idx + 1,
					column: column + 1,
				};
			}
		}
	}
}
