use anyhow::{Result, bail};
use surrealdb_strand::Strand;

use crate::Error;
use crate::expr::tokenizer::Tokenizer as SqlTokenizer;
use crate::ft::Position;
use crate::ft::analyzer::filter::{Filter, FilterResult, Term};
use crate::ft::analyzer::segmenter::Segmenter;
use crate::ft::offset::Offset;
use crate::val::Value;

pub(in crate::ft) struct Tokens {
	/// The input string. Held as a `Strand` so that inline (short) inputs do
	/// not force a heap allocation during FT indexing, and heap (long) inputs
	/// are shared via `Arc<str>` rather than copied.
	i: Strand,
	/// The final list of tokens
	t: Vec<Token>,
}

impl Tokens {
	pub(in crate::ft) fn new(i: Strand) -> Self {
		Self {
			i,
			t: Vec::new(),
		}
	}

	pub(in crate::ft) fn get_token_string<'a>(&'a self, t: &'a Token) -> Result<&'a str> {
		t.get_str(&self.i)
	}

	pub(super) fn filter(self, f: &Filter) -> Result<Tokens> {
		let mut tks = Vec::new();
		for tk in self.t {
			if tk.is_empty() {
				continue;
			}
			let c = tk.get_str(&self.i)?;
			match f.apply_filter(c) {
				FilterResult::Term(t) => match t {
					Term::Unchanged => tks.push(tk),
					Term::NewTerm(t, s) => tks.push(tk.new_token(t, s)),
				},
				FilterResult::Terms(ts) => {
					let mut already_pushed = false;
					for t in ts {
						match t {
							Term::Unchanged => {
								if !already_pushed {
									tks.push(tk.clone());
									already_pushed = true;
								}
							}
							Term::NewTerm(t, s) => tks.push(tk.new_token(t, s)),
						}
					}
				}
				FilterResult::Ignore => {}
			};
		}
		Ok(Tokens {
			i: self.i,
			t: tks,
		})
	}

	pub(in crate::ft) fn list(&self) -> &Vec<Token> {
		&self.t
	}

	pub(in crate::ft) fn try_contains(&self, s: &str) -> Result<bool> {
		for t in &self.t {
			if self.get_token_string(t)?.eq(s) {
				return Ok(true);
			}
		}
		Ok(false)
	}
}

impl TryFrom<Tokens> for Value {
	type Error = anyhow::Error;

	fn try_from(tokens: Tokens) -> Result<Self> {
		let mut vec: Vec<Value> = Vec::with_capacity(tokens.t.len());
		for token in tokens.t {
			vec.push(token.get_str(&tokens.i)?.into())
		}
		Ok(vec.into())
	}
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub(in crate::ft) enum Token {
	Ref {
		chars: (Position, Position, Position),
		bytes: (Position, Position),
		len: u32,
	},
	String {
		chars: (Position, Position, Position),
		bytes: (Position, Position),
		term: String,
		len: u32,
	},
}

impl Token {
	fn new_token(&self, term: String, start: Position) -> Self {
		let len = term.chars().count() as u32;
		match self {
			Token::Ref {
				chars,
				bytes,
				..
			} => Token::String {
				chars: (chars.0, chars.1 + start, chars.2),
				bytes: *bytes,
				term,
				len,
			},
			Token::String {
				chars,
				bytes,
				..
			} => Token::String {
				chars: (chars.0, chars.1 + start, chars.2),
				bytes: *bytes,
				term,
				len,
			},
		}
	}

	pub(in crate::ft) fn new_offset(&self, i: u32) -> Offset {
		match self {
			Token::Ref {
				chars,
				..
			} => Offset::new(i, chars.0, chars.1, chars.2),
			Token::String {
				chars,
				..
			} => Offset::new(i, chars.0, chars.1, chars.2),
		}
	}

	fn is_empty(&self) -> bool {
		match self {
			Token::Ref {
				chars,
				..
			} => chars.0 == chars.2,
			Token::String {
				term,
				..
			} => term.is_empty(),
		}
	}

	pub(in crate::ft) fn get_char_len(&self) -> u32 {
		match self {
			Token::Ref {
				len,
				..
			} => *len,
			Token::String {
				len,
				..
			} => *len,
		}
	}

	pub(super) fn get_str<'a>(&'a self, i: &'a str) -> Result<&'a str> {
		match self {
			Token::Ref {
				bytes,
				..
			} => {
				let s = bytes.0 as usize;
				let e = bytes.1 as usize;
				let l = i.len();
				if s >= l || e > l {
					bail!(Error::AnalyzerError(format!(
						"Unable to extract the token. The offset position ({s},{e}) is out of range ({l})."
					)));
				}
				Ok(&i[s..e])
			}
			Token::String {
				term,
				..
			} => Ok(term),
		}
	}
}

pub(super) struct Tokenizer {
	splitters: Vec<Splitter>,
}

impl Tokenizer {
	pub(in crate::ft) fn new(t: &[SqlTokenizer]) -> Self {
		Self {
			splitters: t.iter().map(|t| t.into()).collect(),
		}
	}

	fn character_role(&mut self, c: char) -> CharacterRole {
		let cl: CharacterClass = c.into();
		// If a character class is not supported, we can safely ignore the character
		if !cl.is_valid() {
			return CharacterRole::NotTokenizable;
		}
		// At this stage, by default, we consider a character being part of the current
		// token
		let mut r = CharacterRole::PartOfCurrentToken;
		for s in &mut self.splitters {
			match s.character_role(cl) {
				// If a tokenizer considers the character being an isolated token we can immediately
				// return
				CharacterRole::IsolatedToken => return CharacterRole::IsolatedToken,
				// The character is part of a new token
				CharacterRole::StartsNewToken => r = CharacterRole::StartsNewToken,
				// If a tokenizer considers the character being not tokenizable we can immediately
				// return
				CharacterRole::NotTokenizable => return CharacterRole::NotTokenizable,
				// We keep the character being part of the current token
				CharacterRole::PartOfCurrentToken => {}
			}
		}
		r
	}

	pub(super) fn tokenize(
		t: &[SqlTokenizer],
		segmenter: Option<&Segmenter>,
		i: Strand,
	) -> Result<Tokens> {
		let mut w = Tokenizer::new(t);
		let mut last_char_pos = 0;
		let mut last_byte_pos = 0;
		let mut current_char_pos = 0;
		let mut current_byte_pos = 0;
		let mut previous_character_role = CharacterRole::PartOfCurrentToken;
		let mut t = Vec::new();
		for c in i.chars() {
			let char_len = c.len_utf8() as Position;
			let cr = w.character_role(c);
			// if the new character is not part of the current token,
			if !matches!(cr, CharacterRole::PartOfCurrentToken)
				|| matches!(previous_character_role, CharacterRole::IsolatedToken)
			{
				// we add a new token (if there is a pending one)
				if last_char_pos < current_char_pos {
					t.push(Token::Ref {
						chars: (last_char_pos, last_char_pos, current_char_pos),
						bytes: (last_byte_pos, current_byte_pos),
						len: current_char_pos - last_char_pos,
					});
				}
				last_char_pos = current_char_pos;
				last_byte_pos = current_byte_pos;
				// If the character is not valid for indexing (space, control...)
				// Then we increase the last position to the next character
				if matches!(cr, CharacterRole::NotTokenizable) {
					last_char_pos += 1;
					last_byte_pos += char_len;
				}
			}
			previous_character_role = cr;
			current_char_pos += 1;
			current_byte_pos += char_len;
		}
		// Do we have a pending token?
		if current_char_pos != last_char_pos {
			t.push(Token::Ref {
				chars: (last_char_pos, last_char_pos, current_char_pos),
				bytes: (last_byte_pos, current_byte_pos),
				len: current_char_pos - last_char_pos,
			});
		}
		let tokens = Tokens {
			i,
			t,
		};
		match segmenter {
			Some(s) => segment_tokens(tokens, s),
			None => Ok(tokens),
		}
	}
}

/// Splits every token further into the words a morphological dictionary finds
/// inside it.
///
/// Runs after the character-class tokenizers, on the spans they produced: the
/// segmenter needs a whole span at once, so it cannot take part in their
/// per-character loop. Offsets are rebuilt from the segmenter's byte ranges
/// rather than inherited from the enclosing token, so a highlighted match
/// covers the word and not the whole span that contained it.
fn segment_tokens(tokens: Tokens, segmenter: &Segmenter) -> Result<Tokens> {
	let Tokens {
		i,
		t,
	} = tokens;
	let mut out = Vec::with_capacity(t.len());
	for tk in t {
		let (char_base, byte_base) = match &tk {
			Token::Ref {
				chars,
				bytes,
				..
			} => (chars.0, bytes.0),
			// The token's text has already been rewritten, so byte offsets into
			// it would not index the input. Filters run after tokenizing, so
			// this cannot arise today; keeping the token whole is the safe
			// reading if that ever changes.
			Token::String {
				..
			} => {
				out.push(tk);
				continue;
			}
		};
		let span = tk.get_str(&i)?;
		// Words are returned in order and do not overlap, so one forward scan
		// over the span converts every byte offset to a char offset.
		let mut boundaries = span.char_indices();
		let mut next = boundaries.next();
		let mut char_pos: Position = 0;
		for w in segmenter.segment(span)? {
			// Count the chars that start before the word does.
			while let Some((b, _)) = next {
				if b >= w.start {
					break;
				}
				char_pos += 1;
				next = boundaries.next();
			}
			let word_char_start = char_pos;
			// Then the chars the word itself spans.
			while let Some((b, _)) = next {
				if b >= w.end {
					break;
				}
				char_pos += 1;
				next = boundaries.next();
			}
			let len = char_pos - word_char_start;
			if len == 0 {
				continue;
			}
			out.push(Token::Ref {
				chars: (
					char_base + word_char_start,
					char_base + word_char_start,
					char_base + char_pos,
				),
				bytes: (byte_base + w.start as Position, byte_base + w.end as Position),
				len,
			});
		}
	}
	Ok(Tokens {
		i,
		t: out,
	})
}

struct Splitter {
	t: SqlTokenizer,
	state: CharacterClass,
}

/// Define the character class
#[derive(Clone, Copy)]
enum CharacterClass {
	Unknown,
	Whitespace,
	// True if uppercase
	Alphabetic(bool),
	Numeric,
	Punctuation,
	Other,
}

impl From<char> for CharacterClass {
	fn from(c: char) -> Self {
		if c.is_alphabetic() {
			Self::Alphabetic(c.is_uppercase())
		} else if c.is_numeric() {
			Self::Numeric
		} else if c.is_whitespace() {
			Self::Whitespace
		} else if c.is_ascii_punctuation() {
			Self::Punctuation
		} else {
			Self::Other
		}
	}
}

impl CharacterClass {
	/// Te be valid a character is either alphanumeric, punctuation or
	/// whitespace
	fn is_valid(self) -> bool {
		matches!(self, Self::Alphabetic(_) | Self::Numeric | Self::Punctuation | Self::Whitespace)
	}
}

/// Defines the role of a character in the tokenization process
enum CharacterRole {
	/// The character is a token on its own
	IsolatedToken,
	/// The character is the first character of a new token
	StartsNewToken,
	/// The character can't be part of a token and should be ignored
	NotTokenizable,
	/// The character is part of the current token
	PartOfCurrentToken,
}

impl From<&SqlTokenizer> for Splitter {
	fn from(t: &SqlTokenizer) -> Self {
		Self {
			t: *t,
			state: CharacterClass::Unknown,
		}
	}
}

impl Splitter {
	fn character_role(&mut self, cl: CharacterClass) -> CharacterRole {
		match &self.t {
			SqlTokenizer::Blank => self.blank_role(cl),
			SqlTokenizer::Camel => self.camel_role(cl),
			SqlTokenizer::Class => self.class_role(cl),
			SqlTokenizer::Punct => self.punct_role(cl),
			// A segmenting tokenizer has no per-character rule: it decides its
			// boundaries from a whole span, in the pass that runs after this
			// state machine. Declaring one therefore leaves the spans this
			// machine produces untouched.
			SqlTokenizer::Segment(_) => CharacterRole::PartOfCurrentToken,
		}
	}

	fn blank_role(&self, cl: CharacterClass) -> CharacterRole {
		if matches!(cl, CharacterClass::Whitespace) {
			CharacterRole::NotTokenizable
		} else {
			CharacterRole::PartOfCurrentToken
		}
	}

	fn class_role(&mut self, cl: CharacterClass) -> CharacterRole {
		let r = match (cl, self.state) {
			(CharacterClass::Alphabetic(_), CharacterClass::Alphabetic(_))
			| (CharacterClass::Numeric, CharacterClass::Numeric)
			| (CharacterClass::Punctuation, CharacterClass::Punctuation) => {
				CharacterRole::PartOfCurrentToken
			}
			(CharacterClass::Other, _)
			| (CharacterClass::Whitespace, _)
			| (CharacterClass::Unknown, _) => CharacterRole::NotTokenizable,
			(_, _) => CharacterRole::StartsNewToken,
		};
		self.state = cl;
		r
	}

	fn punct_role(&self, cl: CharacterClass) -> CharacterRole {
		match cl {
			CharacterClass::Whitespace
			| CharacterClass::Alphabetic(_)
			| CharacterClass::Numeric => CharacterRole::PartOfCurrentToken,
			CharacterClass::Punctuation => CharacterRole::IsolatedToken,
			CharacterClass::Other | CharacterClass::Unknown => CharacterRole::NotTokenizable,
		}
	}

	fn camel_role(&mut self, cl: CharacterClass) -> CharacterRole {
		let r = match cl {
			CharacterClass::Alphabetic(next_upper) => {
				if let CharacterClass::Alphabetic(previous_upper) = self.state {
					if next_upper && !previous_upper {
						CharacterRole::StartsNewToken
					} else {
						CharacterRole::PartOfCurrentToken
					}
				} else {
					CharacterRole::StartsNewToken
				}
			}
			CharacterClass::Numeric | CharacterClass::Punctuation => {
				CharacterRole::PartOfCurrentToken
			}
			CharacterClass::Other | CharacterClass::Whitespace | CharacterClass::Unknown => {
				CharacterRole::NotTokenizable
			}
		};
		self.state = cl;
		r
	}
}

#[cfg(test)]
mod tests {
	use crate::ft::analyzer::tests::test_analyzer;
	#[cfg(feature = "cjk-embed-korean")]
	use crate::ft::analyzer::tests::test_analyzer_tokens;
	#[cfg(feature = "cjk-embed-korean")]
	use crate::ft::analyzer::tokenizer::Token;

	/// Korean writes each word with its grammatical particles attached, so the
	/// character-class tokenizers can only split on the spaces between phrases:
	/// `한국어를` stays whole and a search for `한국어` never matches it.
	/// `SEGMENT(KOREAN)` recovers the noun from the particle.
	#[cfg(feature = "cjk-embed-korean")]
	#[tokio::test]
	async fn test_tokenize_korean_segment() {
		test_analyzer(
			"ANALYZER test TOKENIZERS BLANK, SEGMENT(KOREAN)",
			"한국어를 배우고 있습니다",
			&["한국어", "를", "배우", "고", "있", "습니다"],
		)
		.await;
	}

	/// Japanese and Chinese write a clause with no separator at all, so without
	/// a dictionary the whole clause is a single term.
	#[cfg(feature = "cjk-embed-japanese")]
	#[tokio::test]
	async fn test_tokenize_japanese_segment() {
		test_analyzer(
			"ANALYZER test TOKENIZERS SEGMENT(JAPANESE)",
			"東京都に住んでいます",
			&["東京", "都", "に", "住ん", "で", "い", "ます"],
		)
		.await;
	}

	#[cfg(feature = "cjk-embed-chinese")]
	#[tokio::test]
	async fn test_tokenize_chinese_segment() {
		test_analyzer(
			"ANALYZER test TOKENIZERS SEGMENT(CHINESE)",
			"我喜欢数据库",
			&["我", "喜欢", "数据库"],
		)
		.await;
	}

	/// Segmented tokens carry the offsets of the word, not of the span it came
	/// out of. This is what a filter-based segmenter could not do: a filter's
	/// generated tokens inherit their source token's end offset, so every
	/// highlight would run to the end of the enclosing phrase.
	#[cfg(feature = "cjk-embed-korean")]
	#[tokio::test]
	async fn test_segment_offsets_cover_the_word_only() {
		// `한국어를` occupies chars 0..4 and bytes 0..12 of the input; the two
		// words inside it must land on 0..3 / 0..9 and 3..4 / 9..12.
		test_analyzer_tokens(
			"ANALYZER test TOKENIZERS BLANK, SEGMENT(KOREAN)",
			"한국어를",
			&[
				Token::Ref {
					chars: (0, 0, 3),
					bytes: (0, 9),
					len: 3,
				},
				Token::Ref {
					chars: (3, 3, 4),
					bytes: (9, 12),
					len: 1,
				},
			],
		)
		.await;
	}

	#[tokio::test]
	async fn test_tokenize_blank_class() {
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class FILTERS lowercase",
			"Abc12345xYZ DL1809 item123456 978-3-16-148410-0 1HGCM82633A123456",
			&[
				"abc", "12345", "xyz", "dl", "1809", "item", "123456", "978", "-", "3", "-", "16",
				"-", "148410", "-", "0", "1", "hgcm", "82633", "a", "123456",
			],
		)
		.await;
	}

	#[tokio::test]
	async fn test_tokenize_source_code() {
		test_analyzer(
			"ANALYZER test TOKENIZERS blank,class,camel,punct FILTERS lowercase",
			r#"struct MyRectangle {
    // specified by corners
    top_left: Point,
    bottom_right: Point,
}
static LANGUAGE: &str = "Rust";"#,
			&[
				"struct",
				"my",
				"rectangle",
				"{",
				"/",
				"/",
				"specified",
				"by",
				"corners",
				"top",
				"_",
				"left",
				":",
				"point",
				",",
				"bottom",
				"_",
				"right",
				":",
				"point",
				",",
				"}",
				"static",
				"language",
				":",
				"&",
				"str",
				"=",
				"\"",
				"rust",
				"\"",
				";",
			],
		)
		.await;
	}

	#[tokio::test]
	async fn test_tokenize_punct() {
		test_analyzer(
			"ANALYZER test TOKENIZERS punct",
			";anD pAss...leaving Memories-",
			&[";", "anD pAss", ".", ".", ".", "leaving Memories", "-"],
		)
		.await;
	}
}
