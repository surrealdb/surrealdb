use anyhow::{Result, bail};

use crate::err::Error;
use crate::expr::tokenizer::Tokenizer as SqlTokenizer;
use crate::idx::ft::Position;
use crate::idx::ft::analyzer::filter::{Filter, FilterResult, Term};
use crate::idx::ft::offset::Offset;
use crate::val::Value;

pub(in crate::idx::ft) struct Tokens {
	/// The input string
	i: String,
	/// The final list of tokens
	t: Vec<Token>,
}

impl Tokens {
	pub(in crate::idx::ft) fn new(i: String) -> Self {
		Self {
			i,
			t: Vec::new(),
		}
	}

	pub(in crate::idx::ft) fn get_token_string<'a>(&'a self, t: &'a Token) -> Result<&'a str> {
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

	pub(in crate::idx::ft) fn list(&self) -> &Vec<Token> {
		&self.t
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
pub(in crate::idx::ft) enum Token {
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

	pub(in crate::idx::ft) fn new_offset(&self, i: u32) -> Offset {
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

	pub(in crate::idx::ft) fn get_char_len(&self) -> u32 {
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
	pub(in crate::idx::ft) fn new(t: &[SqlTokenizer]) -> Self {
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

	pub(super) fn tokenize(t: &[SqlTokenizer], i: String) -> Tokens {
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
		Tokens {
			i,
			t,
		}
	}
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
	fn is_valid(&self) -> bool {
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
			t: t.clone(),
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
	use crate::idx::ft::analyzer::tests::test_analyzer;

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
