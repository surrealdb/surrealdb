use crate::idx::ft::analyzer::filter::{Filter, FilterResult, Term};
use crate::sql::tokenizer::Tokenizer as SqlTokenizer;

pub(super) struct Tokens {
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

	pub(super) fn get_token_string<'a>(&'a self, t: &'a Token) -> &str {
		t.get_str(&self.i)
	}

	pub(super) fn filter(self, f: &Filter) -> Tokens {
		let mut tks = Vec::new();
		let mut res = vec![];
		for t in self.t {
			if t.is_empty() {
				continue;
			}
			let c = t.get_str(&self.i);
			let r = f.apply_filter(c);
			res.push((t, r));
		}
		for (tk, fr) in res {
			match fr {
				FilterResult::Term(t) => match t {
					Term::Unchanged => tks.push(tk),
					Term::NewTerm(s) => tks.push(Token::String(s)),
				},
				FilterResult::Terms(ts) => {
					let mut tk = Some(tk);
					for t in ts {
						match t {
							Term::Unchanged => {
								if let Some(tk) = tk.take() {
									tks.push(tk)
								}
							}
							Term::NewTerm(s) => tks.push(Token::String(s)),
						}
					}
				}
				FilterResult::Ignore => {}
			};
		}
		Tokens {
			i: self.i,
			t: tks,
		}
	}

	pub(super) fn list(&self) -> &Vec<Token> {
		&self.t
	}
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub(super) enum Token {
	Ref(usize, usize),
	String(String),
}

impl Token {
	fn is_empty(&self) -> bool {
		match self {
			Token::Ref(start, end) => start == end,
			Token::String(s) => s.is_empty(),
		}
	}

	pub(super) fn get_str<'a>(&'a self, i: &'a str) -> &str {
		match self {
			Token::Ref(s, e) => &i[*s..*e],
			Token::String(s) => s,
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

	pub(super) fn tokenize(t: &Vec<SqlTokenizer>, i: String) -> Tokens {
		let mut w = Tokenizer::new(t);
		let mut last_pos = 0;
		let mut current_pos = 0;
		let mut t = Vec::new();
		for c in i.chars() {
			let is_valid = Self::is_valid(c);
			let should_split = w.should_split(c);
			if should_split || !is_valid {
				// The last pos may be more advanced due to the is_valid process
				if last_pos < current_pos {
					t.push(Token::Ref(last_pos, current_pos));
				}
				last_pos = current_pos;
				// If the character is not valid for indexing (space, control...)
				// Then we increase the last position to the next character
				if !is_valid {
					last_pos += c.len_utf8();
				}
			}
			current_pos += c.len_utf8();
		}
		if current_pos != last_pos {
			t.push(Token::Ref(last_pos, current_pos));
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
	use crate::idx::ft::analyzer::tests::test_analyser;

	#[test]
	fn test_tokenize_blank_class() {
		test_analyser(
			"DEFINE ANALYZER test TOKENIZERS blank,class FILTERS lowercase",
			"Abc12345xYZ DL1809 item123456 978-3-16-148410-0 1HGCM82633A123456",
			&[
				"abc", "12345", "xyz", "dl", "1809", "item", "123456", "978", "-", "3", "-", "16",
				"-", "148410", "-", "0", "1", "hgcm", "82633", "a", "123456",
			],
		);
	}

	#[test]
	fn test_tokenize_source_code() {
		test_analyser(
			"DEFINE ANALYZER test TOKENIZERS blank,class,camel,punct FILTERS lowercase",
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
		);
	}
}
