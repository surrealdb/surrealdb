use crate::err;
use crate::err::Error;
use crate::idx::ft::analyzer::filter::{Filter, FilterResult, Term};
use crate::idx::ft::offsets::{Offset, Position};
use crate::sql::tokenizer::Tokenizer as SqlTokenizer;
use crate::sql::Value;

pub(in crate::idx) struct Tokens {
	/// The input string
	i: String,
	/// The final list of tokens
	t: Vec<Token>,
}

impl Tokens {
	pub(super) fn new(i: String) -> Self {
		Self {
			i,
			t: Vec::new(),
		}
	}

	pub(super) fn get_token_string<'a>(&'a self, t: &'a Token) -> Result<&str, Error> {
		t.get_str(&self.i)
	}

	pub(super) fn filter(self, f: &Filter) -> Result<Tokens, Error> {
		let mut tks = Vec::new();
		let mut res = vec![];
		for t in self.t {
			if t.is_empty() {
				continue;
			}
			let c = t.get_str(&self.i)?;
			let r = f.apply_filter(c);
			res.push((t, r));
		}
		for (tk, fr) in res {
			match fr {
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

	pub(super) fn list(&self) -> &Vec<Token> {
		&self.t
	}
}

impl TryFrom<Tokens> for Value {
	type Error = err::Error;

	fn try_from(tokens: Tokens) -> Result<Self, Error> {
		let mut vec: Vec<Value> = Vec::with_capacity(tokens.t.len());
		for token in tokens.t {
			vec.push(token.get_str(&tokens.i)?.into())
		}
		Ok(vec.into())
	}
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub(super) enum Token {
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

	pub(super) fn new_offset(&self, i: u32) -> Offset {
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

	pub(super) fn get_char_len(&self) -> u32 {
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

	pub(super) fn get_str<'a>(&'a self, i: &'a str) -> Result<&str, Error> {
		match self {
			Token::Ref {
				bytes,
				..
			} => {
				let s = bytes.0 as usize;
				let e = bytes.1 as usize;
				let l = i.len();
				if s >= l || e > l {
					return Err(Error::AnalyzerError(format!(
						"Unable to extract the token. The offset position ({s},{e}) is out of range ({l})."
					)));
				}
				Ok(&i[(bytes.0 as usize)..(bytes.1 as usize)])
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

	fn is_valid(c: char) -> bool {
		c.is_alphanumeric() || c.is_ascii_punctuation()
	}

	fn should_split(&mut self, c: char) -> bool {
		let mut res = false;
		for s in &mut self.splitters {
			if s.should_split(c) {
				res = true;
			}
		}
		res
	}

	pub(super) fn tokenize(t: &[SqlTokenizer], i: String) -> Tokens {
		let mut w = Tokenizer::new(t);
		let mut last_char_pos = 0;
		let mut last_byte_pos = 0;
		let mut current_char_pos = 0;
		let mut current_byte_pos = 0;
		let mut t = Vec::new();
		for c in i.chars() {
			let char_len = c.len_utf8() as Position;
			let is_valid = Self::is_valid(c);
			let should_split = w.should_split(c);
			if should_split || !is_valid {
				// The last pos may be more advanced due to the is_valid process
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
				if !is_valid {
					last_char_pos += 1;
					last_byte_pos += char_len;
				}
			}
			current_char_pos += 1;
			current_byte_pos += char_len;
		}
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
	state: u8,
}

impl From<&SqlTokenizer> for Splitter {
	fn from(t: &SqlTokenizer) -> Self {
		Self {
			t: t.clone(),
			state: 0,
		}
	}
}

impl Splitter {
	fn should_split(&mut self, c: char) -> bool {
		match &self.t {
			SqlTokenizer::Blank => self.blank_state(c),
			SqlTokenizer::Camel => self.camel_state(c),
			SqlTokenizer::Class => self.class_state(c),
			SqlTokenizer::Punct => self.punct_state(c),
		}
	}

	#[inline]
	fn state_check(&mut self, s: u8) -> bool {
		if s != self.state {
			let res = self.state != 0;
			self.state = s;
			res
		} else {
			false
		}
	}

	#[inline]
	fn blank_state(&mut self, c: char) -> bool {
		let s = if c.is_whitespace() {
			1
		} else {
			9
		};
		self.state_check(s)
	}

	#[inline]
	fn class_state(&mut self, c: char) -> bool {
		let s = if c.is_alphabetic() {
			1
		} else if c.is_numeric() {
			2
		} else if c.is_whitespace() {
			3
		} else if c.is_ascii_punctuation() {
			4
		} else {
			9
		};
		self.state_check(s)
	}

	#[inline]
	fn punct_state(&mut self, c: char) -> bool {
		c.is_ascii_punctuation()
	}

	#[inline]
	fn camel_state(&mut self, c: char) -> bool {
		let s = if c.is_lowercase() {
			1
		} else if c.is_uppercase() {
			2
		} else {
			9
		};
		if s != self.state {
			self.state = s;
			s == 2
		} else {
			false
		}
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
}
