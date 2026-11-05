//! Statement-at-a-time parsing of input that arrives in pieces, for imports.
//!
//! [`StatementStream`] parses one statement at a time from the front of a
//! buffer the caller keeps filling. A statement is returned only once more
//! input could not change it; a rejection is rendered with its position in
//! the whole stream, not the current buffer.

use bytes::BytesMut;
use parse_common::error::{RenderedError, SyntaxError};

use super::entry::{self, Statement};
use super::query::TopLevelExpr;
use super::settings::ParserSettings;

/// Parses statements one at a time from a buffer that grows as input arrives.
pub struct StatementStream {
	settings: ParserSettings,
	col_offset: usize,
	line_offset: usize,
}

impl Default for StatementStream {
	fn default() -> Self {
		Self::new()
	}
}

impl StatementStream {
	pub fn new() -> Self {
		Self::new_with_settings(ParserSettings::default())
	}

	pub fn new_with_settings(settings: ParserSettings) -> Self {
		StatementStream {
			settings,
			col_offset: 0,
			line_offset: 0,
		}
	}

	/// Updates the line and column offset after consuming `bytes`, which the
	/// parser has read as UTF-8.
	fn accumulate_line_col(&mut self, bytes: &[u8]) {
		let (line_num, remaining) = std::str::from_utf8(bytes)
			.expect("parser validated utf8")
			.lines()
			.enumerate()
			.last()
			.unwrap_or((0, ""));
		self.line_offset += line_num;
		if line_num > 0 {
			self.col_offset = 0;
		}
		self.col_offset += remaining.chars().count();
	}

	/// Parses a statement if the buffer holds enough input to know it.
	///
	/// A parsed statement is removed from the front of the buffer and
	/// returned as `Ok(Some(_))`. `Ok(None)` means more input is needed; the
	/// buffer may still have lost leading whitespace and empty statements. A
	/// rejection consumes nothing.
	pub fn parse_partial(
		&mut self,
		buffer: &mut BytesMut,
	) -> Result<Option<TopLevelExpr>, RenderedError> {
		self.parse(buffer, false)
	}

	/// Parses a statement from a buffer that holds the rest of the input.
	///
	/// As [`Self::parse_partial`], except that the end of the buffer ends the
	/// input, so `Ok(None)` means no statements remain.
	pub fn parse_complete(
		&mut self,
		buffer: &mut BytesMut,
	) -> Result<Option<TopLevelExpr>, RenderedError> {
		self.parse(buffer, true)
	}

	fn parse(
		&mut self,
		buffer: &mut BytesMut,
		complete: bool,
	) -> Result<Option<TopLevelExpr>, RenderedError> {
		let slice = &buffer[..buffer.len().min(u32::MAX as usize)];
		// Bytes that are not UTF-8 end the input that can be read: statements
		// before them still parse, and a statement that reaches them fails.
		let mut invalid = false;
		let src = match std::str::from_utf8(slice) {
			Ok(s) => s,
			Err(e) => {
				// A character split at the end of a partial buffer waits for
				// its remaining bytes.
				invalid = e.error_len().is_some() || complete;
				std::str::from_utf8(&slice[..e.valid_up_to()]).expect("validated prefix")
			}
		};
		let complete = complete && !invalid && src.len() == buffer.len();
		let parsed = entry::statement(src, &self.settings.next(), complete);
		if invalid && matches!(parsed, Statement::NeedMore | Statement::Empty { .. }) {
			return Err(SyntaxError::new("Invalid, non valid UTF-8 bytes, in source")
				.render_on(src)
				.offset_location(self.line_offset, self.col_offset));
		}
		match parsed {
			Statement::Empty {
				consumed,
			} => {
				let eaten = buffer.split_to(consumed as usize);
				self.accumulate_line_col(&eaten);
				Ok(None)
			}
			Statement::NeedMore => Ok(None),
			Statement::Parsed {
				stmt,
				consumed,
			} => {
				let eaten = buffer.split_to(consumed as usize);
				self.accumulate_line_col(&eaten);
				Ok(Some(*stmt))
			}
			Statement::Rejected(e) => {
				Err(super::api::render(src, &e).offset_location(self.line_offset, self.col_offset))
			}
		}
	}
}
