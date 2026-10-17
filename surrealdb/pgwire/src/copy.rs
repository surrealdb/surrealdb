//! Streaming parse of `COPY ... FROM STDIN` data.
//!
//! The data arrives as CopyData messages cut anywhere: inside a record, inside a
//! quoted CSV field, inside a multi-byte character. The caller keeps the bytes it
//! has not yet consumed and repeatedly asks [`Format::scan`] where the next
//! complete record ends. The records are then parsed and inserted in slices and
//! dropped, so the memory a `COPY` holds is bounded by the constants below
//! however much data is sent.

use std::str::from_utf8;

use super::error::PgError;

/// The longest single record, in bytes. A longer record is refused.
pub(super) const MAX_RECORD_BYTES: usize = 1 << 20;

/// The most fields a record may hold, as many as a PostgreSQL table has columns.
pub(super) const MAX_FIELDS: usize = 1600;

/// The amount of input, in bytes, parsed in one go. A slice ends at a record
/// boundary at or after this size.
pub(super) const SLICE_BYTES: usize = 64 * 1024;

/// The most rows inserted by one statement.
pub(super) const BATCH_ROWS: usize = 500;

/// The most text, in bytes, one INSERT statement is built from: the field data
/// plus the column names repeated for every row.
pub(super) const BATCH_BYTES: usize = 1 << 20;

/// One parsed record: a value per field, `None` for `NULL`.
pub(super) type Row = Vec<Option<String>>;

/// The options that shape how `COPY` data is split into records and fields.
pub(super) struct Format {
	/// Whether the data is CSV rather than the text format.
	pub csv: bool,
	pub delimiter: u8,
	pub quote: u8,
	pub escape: u8,
	/// The string that stands for `NULL`.
	pub null: String,
}

/// Where the next record in a run of bytes ends.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Scan {
	/// The records in this many leading bytes are complete.
	Boundary(usize),
	/// No complete record at or beyond the requested size yet, or the end of the
	/// bytes is too close to a quote to tell where a record ends.
	NeedMore,
	/// A record is longer than [`MAX_RECORD_BYTES`]. The records in the leading
	/// `complete` bytes ahead of it are whole.
	TooLong {
		complete: usize,
	},
}

/// Why a slice of data could not be parsed.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum DataError {
	InvalidUtf8,
	TooManyFields,
}

impl DataError {
	pub(super) fn into_pg(self) -> PgError {
		match self {
			DataError::InvalidUtf8 => PgError::new("22P04", "invalid UTF-8 sequence in COPY data"),
			DataError::TooManyFields => {
				PgError::new("54011", format!("COPY record has more than {MAX_FIELDS} fields"))
			}
		}
	}
}

/// How far [`Format::scan`] has read into the unconsumed bytes, so a later call
/// with more bytes carries on from there instead of starting over.
#[derive(Default)]
pub(super) struct ScanState {
	pos: usize,
	in_quotes: bool,
	record_start: usize,
}

impl Format {
	/// Finds the end of the first complete record that ends at or beyond `min`
	/// bytes, so a slice is at least that large unless the data runs out.
	///
	/// A record ends at a newline (CSV also at a carriage return) outside quotes.
	/// Quoting follows [`parse_csv_stream`], including its one-character
	/// look-ahead, so a boundary is only reported once the characters that decide
	/// it are present. Pass `usize::MAX` to check the record lengths of everything
	/// without cutting it.
	///
	/// `state` records how far the bytes were read when more are needed, and the
	/// next call, on the same bytes with more appended, resumes there. It is reset
	/// when a boundary is returned, because the caller then drops the bytes up to
	/// it.
	pub(super) fn scan(&self, data: &[u8], min: usize, state: &mut ScanState) -> Scan {
		let quote = self.quote;
		let escape = self.escape;
		let mut in_quotes = state.in_quotes;
		let mut record_start = state.record_start;
		let mut i = state.pos;
		while i < data.len() {
			if i - record_start >= MAX_RECORD_BYTES {
				// A record starts where the last one ended, so everything ahead of it is whole.
				return Scan::TooLong {
					complete: record_start,
				};
			}
			let c = data[i];
			if self.csv && in_quotes {
				if c == escape && escape != quote {
					match data.get(i + 1) {
						None => break,
						Some(&next) if next == quote => i += 2,
						Some(_) => i += 1,
					}
				} else if c == quote {
					match data.get(i + 1) {
						None => break,
						Some(&next) if next == quote => i += 2,
						Some(_) => {
							in_quotes = false;
							i += 1;
						}
					}
				} else {
					i += 1;
				}
				continue;
			}
			if self.csv && c == quote {
				in_quotes = true;
				i += 1;
				continue;
			}
			let ends_record = c == b'\n' || (self.csv && c == b'\r');
			i += 1;
			if ends_record {
				record_start = i;
				if i >= min {
					*state = ScanState::default();
					return Scan::Boundary(i);
				}
			}
		}
		*state = ScanState {
			pos: i,
			in_quotes,
			record_start,
		};
		Scan::NeedMore
	}

	/// Parses a run of complete records. `ended` is set once the text format's
	/// `\.` end marker is seen; the data from there on is ignored.
	pub(super) fn parse_records(
		&self,
		slice: &[u8],
		ended: &mut bool,
	) -> Result<Vec<Row>, DataError> {
		if *ended {
			return Ok(Vec::new());
		}
		let text = from_utf8(slice).map_err(|_| DataError::InvalidUtf8)?;
		if self.csv {
			return parse_csv_stream(text, self.delimiter, self.quote, self.escape, &self.null);
		}
		let mut rows = Vec::new();
		for line in text.lines() {
			let trimmed = line.trim_end_matches(['\r', '\n']);
			if trimmed == "\\." {
				*ended = true;
				break;
			}
			if trimmed.is_empty() {
				continue;
			}
			let row: Row = trimmed
				.split(self.delimiter as char)
				.map(|field| parse_text_field(field, &self.null))
				.take(MAX_FIELDS + 1)
				.collect();
			if row.len() > MAX_FIELDS {
				return Err(DataError::TooManyFields);
			}
			rows.push(row);
		}
		Ok(rows)
	}
}

/// The state of one `COPY ... FROM STDIN`, carried across CopyData messages.
pub(super) struct Ingest {
	pub format: Format,
	/// The target table as written, for looking up its columns.
	pub table_name: String,
	/// The target table rendered for the generated INSERT.
	pub table: String,
	/// The column names, from the statement, the header row or the table.
	pub columns: Vec<String>,
	/// Whether the next record names the columns rather than holding data.
	pub header_pending: bool,
	/// Whether the column names have been settled against the first data row.
	pub columns_settled: bool,
	/// Whether the text format's end marker has been seen.
	pub ended: bool,
	/// The rendered length of each column's key in the generated INSERT.
	pub key_lens: Vec<usize>,
	/// How far the unconsumed bytes have been scanned.
	pub scan: ScanState,
	/// The rows waiting for the next INSERT.
	pub batch: Vec<Row>,
	/// The text the rows in `batch` take up in the generated INSERT, in bytes.
	pub batch_bytes: usize,
	/// The rows inserted so far.
	pub total: usize,
}

impl Ingest {
	pub(super) fn new(
		format: Format,
		table_name: String,
		table: String,
		columns: Vec<String>,
		header: bool,
	) -> Self {
		Self {
			format,
			table_name,
			table,
			columns,
			header_pending: header,
			columns_settled: false,
			ended: false,
			key_lens: Vec::new(),
			scan: ScanState::default(),
			batch: Vec::new(),
			batch_bytes: 0,
			total: 0,
		}
	}
}

/// The text a row takes up in the generated INSERT, in bytes, used to size a
/// batch. `key_lens` holds the rendered length of each column's key; a field
/// past the named columns is keyed `col`.
pub(super) fn row_cost(row: &[Option<String>], key_lens: &[usize]) -> usize {
	row.iter()
		.enumerate()
		.map(|(i, field)| {
			key_lens.get(i).copied().unwrap_or(5) + field.as_ref().map_or(4, String::len) + 6
		})
		.sum()
}

/// Decodes one field of the text format: `None` for the null string, otherwise
/// the field with backslash escapes resolved.
pub(super) fn parse_text_field(raw: &str, null_str: &str) -> Option<String> {
	if raw == null_str {
		return None;
	}
	let mut out = String::with_capacity(raw.len());
	let mut chars = raw.chars().peekable();
	while let Some(c) = chars.next() {
		if c == '\\' {
			if let Some(next) = chars.next() {
				match next {
					'b' => out.push('\x08'),
					'f' => out.push('\x0c'),
					'n' => out.push('\n'),
					'r' => out.push('\r'),
					't' => out.push('\t'),
					'v' => out.push('\x0b'),
					'\\' => out.push('\\'),
					'0'..='7' => {
						let mut oct = next.to_digit(8).unwrap_or(0);
						for _ in 0..2 {
							if let Some(&d) = chars.peek() {
								if ('0'..='7').contains(&d) {
									oct = oct * 8 + d.to_digit(8).unwrap_or(0);
									chars.next();
								} else {
									break;
								}
							}
						}
						if let Some(ch) = char::from_u32(oct) {
							out.push(ch);
						}
					}
					other => out.push(other),
				}
			} else {
				out.push('\\');
			}
		} else {
			out.push(c);
		}
	}
	Some(out)
}

/// Pushes the field that has just ended onto `row`: `None` for an unquoted field
/// equal to the null string.
fn end_field(row: &mut Row, field: &mut String, was_quoted: bool, null_str: &str) {
	let value = std::mem::take(field);
	if !was_quoted && value == null_str {
		row.push(None);
	} else {
		row.push(Some(value));
	}
}

/// Parses CSV into records. Quoted fields may hold the delimiter and newlines,
/// and an unquoted field equal to the null string is `NULL`. A record with more
/// than [`MAX_FIELDS`] fields is refused as soon as it is seen.
pub(super) fn parse_csv_stream(
	data: &str,
	delimiter: u8,
	quote: u8,
	escape: u8,
	null_str: &str,
) -> Result<Vec<Row>, DataError> {
	let delim = delimiter as char;
	let q = quote as char;
	let esc = escape as char;

	let mut rows = Vec::new();
	let mut current_row = Vec::new();
	let mut current_field = String::new();
	let mut in_quotes = false;
	let mut was_quoted = false;
	let mut chars = data.chars().peekable();

	while let Some(c) = chars.next() {
		if in_quotes {
			if c == esc && esc != q && chars.peek() == Some(&q) {
				if let Some(next) = chars.next() {
					current_field.push(next);
				}
			} else if c == q {
				if chars.peek() == Some(&q) {
					chars.next();
					current_field.push(q);
				} else {
					in_quotes = false;
				}
			} else {
				current_field.push(c);
			}
		} else if c == q {
			in_quotes = true;
			was_quoted = true;
		} else if c == delim {
			end_field(&mut current_row, &mut current_field, was_quoted, null_str);
			was_quoted = false;
			if current_row.len() > MAX_FIELDS {
				return Err(DataError::TooManyFields);
			}
		} else if c == '\n' || c == '\r' {
			if c == '\r' && chars.peek() == Some(&'\n') {
				chars.next();
			}
			if was_quoted || !current_field.is_empty() || !current_row.is_empty() {
				end_field(&mut current_row, &mut current_field, was_quoted, null_str);
				if current_row.len() > MAX_FIELDS {
					return Err(DataError::TooManyFields);
				}
				rows.push(std::mem::take(&mut current_row));
				was_quoted = false;
			}
		} else {
			current_field.push(c);
		}
	}

	if was_quoted || !current_field.is_empty() || !current_row.is_empty() {
		end_field(&mut current_row, &mut current_field, was_quoted, null_str);
		if current_row.len() > MAX_FIELDS {
			return Err(DataError::TooManyFields);
		}
		rows.push(current_row);
	}

	Ok(rows)
}

#[cfg(test)]
mod tests {
	use super::{
		DataError, Format, MAX_FIELDS, MAX_RECORD_BYTES, Row, Scan, ScanState, parse_csv_stream,
		parse_text_field, row_cost,
	};

	fn text() -> Format {
		Format {
			csv: false,
			delimiter: b'\t',
			quote: b'"',
			escape: b'\\',
			null: "\\N".to_string(),
		}
	}

	fn csv() -> Format {
		Format {
			csv: true,
			delimiter: b',',
			quote: b'"',
			escape: b'"',
			null: String::new(),
		}
	}

	fn scan(f: &Format, data: &[u8], min: usize) -> Scan {
		f.scan(data, min, &mut ScanState::default())
	}

	fn row(fields: &[&str]) -> Row {
		fields.iter().map(|f| Some((*f).to_string())).collect()
	}

	#[test]
	fn text_records_end_at_a_newline() {
		let f = text();
		assert_eq!(scan(&f, b"a\nb\nc", 0), Scan::Boundary(2));
		assert_eq!(scan(&f, b"a\nb\nc", 3), Scan::Boundary(4));
		assert_eq!(scan(&f, b"a\nb\nc", usize::MAX), Scan::NeedMore);
		assert_eq!(scan(&f, b"no newline yet", 0), Scan::NeedMore);
		assert_eq!(scan(&f, b"", 0), Scan::NeedMore);
	}

	#[test]
	fn text_ignores_quotes_and_carriage_returns() {
		let f = text();
		assert_eq!(scan(&f, b"a\"b\r\nc\n", 0), Scan::Boundary(5));
		assert_eq!(scan(&f, b"a\rb\n", 0), Scan::Boundary(4));
	}

	#[test]
	fn csv_records_end_at_a_newline_or_carriage_return_outside_quotes() {
		let f = csv();
		assert_eq!(scan(&f, b"1,a\n2,b\n", 0), Scan::Boundary(4));
		assert_eq!(scan(&f, b"1,a\r2,b\r", 0), Scan::Boundary(4));
		assert_eq!(scan(&f, b"1,a\r\n2\n", 0), Scan::Boundary(4));
		assert_eq!(scan(&f, b"\n2\n", 0), Scan::Boundary(1));
	}

	#[test]
	fn csv_newlines_inside_quotes_do_not_end_a_record() {
		let f = csv();
		assert_eq!(scan(&f, b"1,\"a\nb\"\n2\n", 0), Scan::Boundary(8));
		assert_eq!(scan(&f, b"1,\"a\nb", 0), Scan::NeedMore);
	}

	#[test]
	fn csv_waits_for_the_character_after_a_quote() {
		let f = csv();
		// A quote at the very end could be a closing quote or the first of a doubled pair.
		assert_eq!(scan(&f, b"1,\"a\"", 0), Scan::NeedMore);
		assert_eq!(scan(&f, b"1,\"a\"\n", 0), Scan::Boundary(6));
		// A doubled quote stays inside the field.
		assert_eq!(scan(&f, b"1,\"a\"\"b\"\n", 0), Scan::Boundary(9));
		assert_eq!(scan(&f, b"1,\"a\"\"\n", 0), Scan::NeedMore);
	}

	#[test]
	fn csv_honours_a_distinct_escape_character() {
		let f = Format {
			escape: b'\\',
			..csv()
		};
		assert_eq!(scan(&f, b"\"a\\\"\n\"\n", 0), Scan::Boundary(7));
		assert_eq!(scan(&f, b"\"a\\", 0), Scan::NeedMore);
	}

	#[test]
	fn a_record_over_the_limit_is_too_long() {
		let f = text();
		let mut data = vec![b'x'; MAX_RECORD_BYTES + 1];
		assert_eq!(
			scan(&f, &data, 0),
			Scan::TooLong {
				complete: 0
			}
		);
		data.truncate(MAX_RECORD_BYTES - 1);
		data.push(b'\n');
		assert_eq!(scan(&f, &data, 0), Scan::Boundary(MAX_RECORD_BYTES));
		data.insert(0, b'x');
		assert_eq!(
			scan(&f, &data, 0),
			Scan::TooLong {
				complete: 0
			}
		);
		let mut quoted = vec![b'"'];
		quoted.extend(vec![b'\n'; MAX_RECORD_BYTES]);
		assert_eq!(
			scan(&csv(), &quoted, 0),
			Scan::TooLong {
				complete: 0
			}
		);
	}

	#[test]
	fn a_too_long_record_reports_the_whole_records_before_it() {
		let mut data = b"1\n2\n".to_vec();
		data.extend(vec![b'x'; MAX_RECORD_BYTES + 1]);
		// Even when `min` is past them, so that no boundary was returned.
		assert_eq!(
			scan(&text(), &data, usize::MAX),
			Scan::TooLong {
				complete: 4
			}
		);
		let mut state = ScanState::default();
		let mut found = Scan::NeedMore;
		for n in (0..data.len()).step_by(777).chain([data.len()]) {
			found = text().scan(&data[..n], usize::MAX, &mut state);
			if matches!(found, Scan::TooLong { .. }) {
				break;
			}
		}
		assert_eq!(
			found,
			Scan::TooLong {
				complete: 4
			}
		);
	}

	#[test]
	fn a_scan_resumes_where_the_last_one_stopped() {
		for (f, data) in [
			(text(), b"ab\ncd\nef".as_slice()),
			(csv(), b"1,\"a\nb\"\r\n2,\"c\"\"d\"\n3,e\n".as_slice()),
			(csv(), b"\"never closed\n and more".as_slice()),
		] {
			for min in [0, 3, usize::MAX] {
				let whole = scan(&f, data, min);
				let mut state = ScanState::default();
				let mut last = Scan::NeedMore;
				for n in 1..=data.len() {
					last = f.scan(&data[..n], min, &mut state);
					if matches!(last, Scan::Boundary(_)) {
						break;
					}
					// The bytes read so far are never read again, bar the one a look-ahead is
					// waiting on.
					assert!(state.pos + 1 >= n, "scan fell behind at {n}: {}", state.pos);
				}
				assert_eq!(last, whole, "{data:?} with min {min}");
			}
		}
	}

	#[test]
	fn a_long_record_fed_a_byte_at_a_time_is_read_once() {
		let f = csv();
		let mut data = vec![b'"'];
		data.extend(vec![b'x'; 100_000]);
		let mut state = ScanState::default();
		let mut work = 0;
		for n in 1..=data.len() {
			let before = state.pos;
			assert_eq!(f.scan(&data[..n], 0, &mut state), Scan::NeedMore);
			work += state.pos - before;
		}
		assert_eq!(work, data.len());
	}

	#[test]
	fn text_records_parse_fields_nulls_and_escapes() {
		let f = text();
		let mut ended = false;
		let rows = f.parse_records(b"1\talice\n\n2\t\\N\n3\ta\\tb\\n\\101\n", &mut ended).unwrap();
		assert!(!ended);
		assert_eq!(
			rows,
			vec![row(&["1", "alice"]), vec![Some("2".into()), None], row(&["3", "a\tb\nA"]),]
		);
	}

	#[test]
	fn text_records_with_crlf_line_ends_parse() {
		let mut ended = false;
		let rows = text().parse_records(b"1\ta\r\n2\tb\r\n\\.\r\n3\tc\r\n", &mut ended).unwrap();
		assert!(ended);
		assert_eq!(rows, vec![row(&["1", "a"]), row(&["2", "b"])]);
	}

	#[test]
	fn the_text_end_marker_stops_the_data() {
		let f = text();
		let mut ended = false;
		let rows = f.parse_records(b"1\n\\.\n2\n", &mut ended).unwrap();
		assert!(ended);
		assert_eq!(rows, vec![row(&["1"])]);
		assert!(f.parse_records(b"3\n", &mut ended).unwrap().is_empty());
		assert!(f.parse_records(b"\xff", &mut ended).unwrap().is_empty());
	}

	#[test]
	fn csv_records_parse_quoted_fields_and_nulls() {
		let f = Format {
			null: "NULL".to_string(),
			..csv()
		};
		let mut ended = false;
		let rows =
			f.parse_records(b"1,\"a\nb\",NULL\n2,\"c\"\"d\",\"NULL\"\n", &mut ended).unwrap();
		assert_eq!(
			rows,
			vec![vec![Some("1".into()), Some("a\nb".into()), None], row(&["2", "c\"d", "NULL"]),]
		);
	}

	#[test]
	fn a_null_does_not_leak_into_the_next_field_or_record() {
		let f = Format {
			null: "NULL".to_string(),
			..csv()
		};
		let mut ended = false;
		let rows = f.parse_records(b"NULL,x\nNULL\nb\n", &mut ended).unwrap();
		assert_eq!(rows, vec![vec![None, Some("x".into())], vec![None], row(&["b"])]);
	}

	#[test]
	fn a_final_record_without_a_newline_is_parsed() {
		let mut ended = false;
		assert_eq!(text().parse_records(b"1\t2", &mut ended).unwrap(), vec![row(&["1", "2"])]);
		assert_eq!(csv().parse_records(b"1,2", &mut ended).unwrap(), vec![row(&["1", "2"])]);
	}

	#[test]
	fn invalid_utf8_and_too_many_fields_are_refused() {
		let mut ended = false;
		assert_eq!(text().parse_records(b"\xff\n", &mut ended), Err(DataError::InvalidUtf8));
		let wide = vec!["x"; MAX_FIELDS + 1].join("\t") + "\n";
		assert_eq!(
			text().parse_records(wide.as_bytes(), &mut ended),
			Err(DataError::TooManyFields)
		);
		let ok = vec!["x"; MAX_FIELDS].join("\t") + "\n";
		assert_eq!(text().parse_records(ok.as_bytes(), &mut ended).unwrap()[0].len(), MAX_FIELDS);

		let wide = vec!["x"; MAX_FIELDS + 1].join(",") + "\n";
		assert_eq!(csv().parse_records(wide.as_bytes(), &mut ended), Err(DataError::TooManyFields));
		let ok = vec!["x"; MAX_FIELDS].join(",");
		assert_eq!(csv().parse_records(ok.as_bytes(), &mut ended).unwrap()[0].len(), MAX_FIELDS);
		// Delimiters inside a quoted field are not fields.
		let quoted = format!("\"{}\"\n", ",".repeat(MAX_FIELDS * 2));
		assert_eq!(csv().parse_records(quoted.as_bytes(), &mut ended).unwrap()[0].len(), 1);
	}

	#[test]
	fn fields_decode_escapes_and_nulls() {
		assert_eq!(parse_text_field("\\N", "\\N"), None);
		assert_eq!(parse_text_field("a\\\\b", "\\N").as_deref(), Some("a\\b"));
		assert_eq!(parse_text_field("tab\\there", "\\N").as_deref(), Some("tab\there"));
		assert_eq!(
			parse_csv_stream("a,\"b,c\"\n", b',', b'"', b'"', ""),
			Ok(vec![row(&["a", "b,c"])])
		);
	}

	#[test]
	fn a_rows_cost_counts_its_keys_and_values() {
		assert_eq!(row_cost(&[Some("ab".into()), None], &[3, 3]), (3 + 2 + 6) + (3 + 4 + 6));
		// A field past the named columns is keyed `col`.
		assert_eq!(
			row_cost(&[Some("a".into()), Some("b".into())], &[3]),
			(3 + 1 + 6) + (5 + 1 + 6)
		);
	}

	/// What the connection does with a COPY stream: collect the bytes as messages arrive, cut
	/// complete slices at record boundaries as soon as there are enough, and take the rest at
	/// the end.
	fn stream(f: &Format, data: &[u8], message: usize, min: usize) -> Vec<Row> {
		let mut pending: Vec<u8> = Vec::new();
		let mut state = ScanState::default();
		let mut ended = false;
		let mut rows = Vec::new();
		let mut consume = |pending: &mut Vec<u8>, state: &mut ScanState, finish: bool| {
			let min = if finish {
				usize::MAX
			} else {
				min
			};
			let mut consumed = 0;
			loop {
				let rest = &pending[consumed..];
				if rest.is_empty() {
					break;
				}
				let end = match f.scan(rest, min, state) {
					Scan::Boundary(end) => end,
					Scan::NeedMore if finish => rest.len(),
					Scan::NeedMore => break,
					Scan::TooLong {
						..
					} => panic!("too long"),
				};
				rows.extend(f.parse_records(&rest[..end], &mut ended).unwrap());
				consumed += end;
			}
			pending.drain(..consumed);
		};
		for piece in data.chunks(message) {
			pending.extend_from_slice(piece);
			consume(&mut pending, &mut state, false);
		}
		consume(&mut pending, &mut state, true);
		rows
	}

	#[test]
	fn streaming_in_any_messages_equals_parsing_the_whole_input() {
		let alphabet: [&str; 12] =
			["a", "b", ",", "\"", "\n", "\r", "é", "\\", "N", "\\.", "\t", "NULL"];
		let formats = [
			text(),
			csv(),
			Format {
				escape: b'\\',
				..csv()
			},
			Format {
				null: "NULL".to_string(),
				..csv()
			},
		];
		let mut seed = 0x2545_f491_4f6c_dd1d_u64;
		let mut next = move || {
			seed = seed
				.wrapping_mul(6_364_136_223_846_793_005)
				.wrapping_add(1_442_695_040_888_963_407);
			(seed >> 33) as usize
		};
		for _ in 0..300 {
			let len = 20 + next() % 100;
			let data: String = (0..len).map(|_| alphabet[next() % alphabet.len()]).collect();
			for f in &formats {
				let whole = f.parse_records(data.as_bytes(), &mut false).unwrap();
				for message in [1, 2, 3, 5, 8, 13, 1000] {
					for min in [0, 1, 4, 16] {
						assert_eq!(
							stream(f, data.as_bytes(), message, min),
							whole,
							"{data:?} (csv {}, escape {}) in messages of {message} with min {min}",
							f.csv,
							f.escape as char
						);
					}
				}
			}
		}
	}
}
