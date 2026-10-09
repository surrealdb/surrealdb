//! Statement-at-a-time parsing of input that arrives in pieces.
//!
//! [`StreamParser`] buffers bytes as they are fed and yields each top-level
//! statement as soon as the `;` that ends it has arrived, or at the end of
//! input. Each statement is parsed into its own tree over its own window of
//! the buffer; both are released when the caller asks for the next
//! statement, so memory is bounded by the largest statement, not by the
//! input.
//!
//! A statement is re-parsed from its first byte when more input arrives.
//! Nothing is resumed mid-statement, so the parser keeps no suspended state
//! and a streamed statement parses exactly as it does in a whole-input
//! parse. Re-parsing is bounded two ways:
//! - A statement can only become complete when a `;` byte arrives: a `;` already buffered was
//!   inside a string, comment or bracket the last time the statement was parsed, and appending
//!   bytes cannot move it out. Feeds with no `;` therefore cause no parse.
//! - Past [`REPARSE_LINEAR_LIMIT`] bytes, a statement is re-parsed only once its buffered length
//!   has grown by half since the last attempt, so the total work on one statement stays linear in
//!   its length.

use crate::ast::{Ast, NodeId};
use crate::parse::{Diag, Limits, Scratch, StatementEnd, parse_statement};

/// Statements up to this many bytes are re-parsed whenever a `;` arrives;
/// longer ones re-parse geometrically.
pub const REPARSE_LINEAR_LIMIT: usize = 4 * 1024;

/// One parsed top-level statement, borrowed from the stream until the next
/// call to [`StreamParser::next_statement`].
pub struct Statement<'a> {
	/// The statement's window: its spans are offsets into this text.
	pub src: &'a str,
	/// Byte offset of `src` in the whole input.
	pub offset: u64,
	pub ast: &'a Ast,
	/// The statement node; [`crate::Tag::Error`] when it failed to parse.
	pub root: NodeId,
	/// Diagnostics, with spans relative to `src`.
	pub diags: &'a [Diag],
}

pub enum Step<'a> {
	Statement(Statement<'a>),
	/// The buffered input ends inside a statement: feed more, or call
	/// [`StreamParser::finish`].
	NeedMore,
	/// All input was consumed.
	End,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamError {
	/// The input is not UTF-8; `offset` is the first invalid byte.
	InvalidUtf8 {
		offset: u64,
	},
	/// A statement grew past the configured maximum without ending.
	StatementTooLarge {
		offset: u64,
		max: usize,
	},
}

pub struct StreamParser {
	buf: Vec<u8>,
	/// Start of the pending statement in `buf`.
	start: usize,
	/// Length of `buf`'s prefix known to be valid UTF-8.
	valid: usize,
	/// Byte offset of `buf[0]` in the whole input.
	base: u64,
	/// Bytes the caller released by asking for the next statement.
	release: usize,
	/// Window length of the last parse that needed more input, and whether
	/// a `;` arrived since.
	tried: usize,
	semi_since: bool,
	finished: bool,
	failed: Option<StreamError>,
	limits: Limits,
	max_statement: usize,
	ast: Ast,
	scratch: Scratch,
}

impl StreamParser {
	/// `max_statement` bounds how many bytes one statement may span; it must
	/// be below 4 GiB, since spans are `u32`.
	pub fn new(limits: Limits, max_statement: usize) -> Self {
		StreamParser {
			buf: Vec::new(),
			start: 0,
			valid: 0,
			base: 0,
			release: 0,
			tried: 0,
			semi_since: true,
			finished: false,
			failed: None,
			limits,
			max_statement: max_statement.min(u32::MAX as usize),
			ast: Ast::new(),
			scratch: Scratch::default(),
		}
	}

	/// Appends input.
	pub fn feed(&mut self, bytes: &[u8]) {
		debug_assert!(!self.finished, "fed after finish");
		self.compact();
		if bytes.contains(&b';') {
			self.semi_since = true;
		}
		self.buf.extend_from_slice(bytes);
	}

	/// Marks the end of input.
	pub fn finish(&mut self) {
		self.finished = true;
	}

	/// Bytes buffered and not yet released.
	pub fn buffered(&self) -> usize {
		self.buf.len() - self.start.max(self.release)
	}

	/// Drops released bytes once they are at least half the buffer, so the
	/// copy is amortised over the bytes that were consumed.
	fn compact(&mut self) {
		let cut = self.start;
		if cut > 0 && cut * 2 >= self.buf.len() {
			self.buf.drain(..cut);
			self.base += cut as u64;
			self.start = 0;
			self.valid -= cut;
			self.release = 0;
		}
	}

	/// Extends the valid UTF-8 prefix over newly fed bytes. An incomplete
	/// sequence at the end waits for more input unless input has finished.
	fn validate(&mut self) -> Result<(), StreamError> {
		match core::str::from_utf8(&self.buf[self.valid..]) {
			Ok(_) => self.valid = self.buf.len(),
			Err(e) => {
				let upto = self.valid + e.valid_up_to();
				if e.error_len().is_some() || self.finished {
					return Err(StreamError::InvalidUtf8 {
						offset: self.base + upto as u64,
					});
				}
				self.valid = upto;
			}
		}
		Ok(())
	}

	/// Parses the next statement, releasing the previous one.
	pub fn next_statement(&mut self) -> Result<Step<'_>, StreamError> {
		if let Some(e) = self.failed {
			return Err(e);
		}
		self.start = self.start.max(self.release);
		if let Err(e) = self.validate() {
			self.failed = Some(e);
			return Err(e);
		}
		let window = self.valid - self.start;
		let complete = self.finished && self.valid == self.buf.len();
		if !complete {
			// Skip a parse that cannot complete the statement.
			let grown = window.saturating_sub(self.tried);
			if !self.semi_since || (self.tried > REPARSE_LINEAR_LIMIT && grown < self.tried / 2) {
				return self.need_more(window);
			}
		}
		// SAFETY: `buf[start..valid]` was validated as UTF-8 above, and
		// `start` only ever lands on the end of a `;` token or of a run of
		// trivia the parser skipped, both character boundaries.
		let src = unsafe { core::str::from_utf8_unchecked(&self.buf[self.start..self.valid]) };
		let truncated = src.len() > self.max_statement;
		let mut cut = src.len().min(self.max_statement);
		while !src.is_char_boundary(cut) {
			cut -= 1;
		}
		let src = &src[..cut];
		let cut = src.len();
		let end = parse_statement(src, self.limits, &mut self.ast, &mut self.scratch);
		match end {
			StatementEnd::Empty {
				consumed,
			} => {
				if truncated {
					// More than `max_statement` bytes of `;` and trivia.
					return self.too_large();
				}
				self.start += consumed as usize;
				self.release = self.start;
				self.tried = 0;
				if complete {
					self.start = self.valid;
					self.release = self.start;
					return Ok(Step::End);
				}
				self.semi_since = false;
				Ok(Step::NeedMore)
			}
			StatementEnd::Statement {
				final_,
				consumed,
			} => {
				if truncated && consumed as usize == cut {
					return self.too_large();
				}
				if !final_ && !complete {
					return self.need_more(window);
				}
				let offset = self.base + self.start as u64;
				let s = self.start;
				self.release = s + consumed as usize;
				self.tried = 0;
				self.semi_since = true;
				// SAFETY: as above; the slice ends at or before `valid`.
				let src =
					unsafe { core::str::from_utf8_unchecked(&self.buf[s..s + consumed as usize]) };
				Ok(Step::Statement(Statement {
					src,
					offset,
					ast: &self.ast,
					root: self.ast.stmts[0],
					diags: &self.scratch.diags,
				}))
			}
		}
	}

	fn need_more(&mut self, window: usize) -> Result<Step<'_>, StreamError> {
		if window > self.max_statement {
			return self.too_large();
		}
		self.tried = window;
		self.semi_since = false;
		Ok(Step::NeedMore)
	}

	fn too_large(&mut self) -> Result<Step<'_>, StreamError> {
		let e = StreamError::StatementTooLarge {
			offset: self.base + self.start as u64,
			max: self.max_statement,
		};
		self.failed = Some(e);
		Err(e)
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::Tag;

	/// Feeds `chunks` and returns each statement's text and whether it
	/// failed, with `None` marking each `NeedMore`.
	fn run(chunks: &[&[u8]], max: usize) -> Result<Vec<Option<(String, bool)>>, StreamError> {
		let mut sp = StreamParser::new(Limits::default(), max);
		let mut out = Vec::new();
		for (i, c) in chunks.iter().enumerate() {
			sp.feed(c);
			if i + 1 == chunks.len() {
				sp.finish();
			}
			loop {
				match sp.next_statement()? {
					Step::Statement(s) => {
						let (a, b) = s.ast.spans[0];
						let failed = s.ast.node(s.root).tag == Tag::Error;
						out.push(Some((s.src[a as usize..b as usize].to_owned(), failed)));
					}
					Step::NeedMore => {
						out.push(None);
						break;
					}
					Step::End => return Ok(out),
				}
			}
		}
		Ok(out)
	}

	fn ok(s: &str) -> Option<(String, bool)> {
		Some((s.to_owned(), false))
	}

	#[test]
	fn yields_each_statement_when_its_semicolon_arrives() {
		let out = run(&[b"SELECT * FR", b"OM a; RETURN 1", b"; RETURN 2"], 1 << 20).unwrap();
		assert_eq!(out, [None, ok("SELECT * FROM a"), None, ok("RETURN 1"), ok("RETURN 2")]);
	}

	#[test]
	fn waits_on_strings_comments_and_brackets_split_across_chunks() {
		let out = run(&[b"RETURN 'a;", b"b'; /* x;", b" */ RETURN [1;", b"]"], 1 << 20).unwrap();
		assert_eq!(out[0], None);
		assert_eq!(out[1], ok("RETURN 'a;b'"));
		assert!(out.iter().flatten().any(|s| s.0 == "RETURN [1;]" && s.1));
	}

	#[test]
	fn keeps_a_character_split_across_chunks() {
		let s = "RETURN 'é';".as_bytes();
		let at = s.iter().position(|&b| b >= 0x80).unwrap() + 1;
		let out = run(&[&s[..at], &s[at..]], 1 << 20).unwrap();
		assert_eq!(out, [None, ok("RETURN 'é'")]);
	}

	#[test]
	fn rejects_invalid_utf8_and_oversized_statements() {
		assert_eq!(
			run(&[b"RETURN 1; RETURN '\xff';"], 1 << 20),
			Err(StreamError::InvalidUtf8 {
				offset: 18
			})
		);
		assert_eq!(
			run(&[b"RETURN 1; RETURN [1, 2, 3, 4, 5, 6, 7, 8", b"]"], 16),
			Err(StreamError::StatementTooLarge {
				offset: 9,
				max: 16
			})
		);
	}
}
