use crate::idx::ft::analyzer::filter::Filter;
use crate::sql::tokenizer::Tokenizer;

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

	pub(in crate::idx::ft) fn add(&mut self, f: &Option<Vec<Filter>>, s: usize, e: usize) {
		let mut t = Token::Ref(s, e);
		if let Some(f) = f {
			for f in f {
				let c = self.get_token_string(&t);
				let s = f.filter(c);
				if s.is_empty() {
					break;
				}
				// If the new token is equal to the old one, we keep the old one
				t = if s.eq(c) {
					t
				} else {
					Token::String(s.into())
				}
			}
		}
		if !t.is_empty() {
			self.t.push(t);
		}
	}

	pub(in crate::idx::ft) fn get_token_string<'a>(&'a self, t: &'a Token) -> &str {
		match t {
			Token::Ref(s, e) => &self.i[*s..*e],
			Token::String(s) => s,
		}
	}

	pub(in crate::idx::ft) fn list(&self) -> &Vec<Token> {
		&self.t
	}
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub(in crate::idx::ft) enum Token {
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
}

pub(in crate::idx::ft) struct Walker {
	splitters: Vec<Splitter>,
}

impl Walker {
	pub(in crate::idx::ft) fn new(t: &[Tokenizer]) -> Self {
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

	pub(in crate::idx::ft) fn walk(
		t: &Vec<Tokenizer>,
		f: &Option<Vec<Filter>>,
		input: &mut Tokens,
	) {
		let mut w = Walker::new(t);
		let mut last_pos = 0;
		let mut current_pos = 0;
		let mut tks = Vec::new();
		for c in input.i.chars() {
			let is_valid = Self::is_valid(c);
			let should_split = w.should_split(c);
			if should_split || !is_valid {
				// The last pos may be more advanced due to the is_valid process
				if last_pos < current_pos {
					tks.push((last_pos, current_pos));
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
			tks.push((last_pos, current_pos));
		}

		for (s, e) in tks {
			input.add(f, s, e);
		}
	}
}

struct Splitter {
	t: Tokenizer,
	state: u8,
}

impl From<&Tokenizer> for Splitter {
	fn from(t: &Tokenizer) -> Self {
		Self {
			t: t.clone(),
			state: 0,
		}
	}
}

impl Splitter {
	fn should_split(&mut self, c: char) -> bool {
		let new_state = match &self.t {
			Tokenizer::Blank => Self::blank_state(c),
			Tokenizer::Case => Self::case_state(c),
			Tokenizer::Class => Self::class_state(c),
		};
		if new_state != self.state {
			let res = self.state != 0;
			self.state = new_state;
			res
		} else {
			false
		}
	}

	#[inline]
	fn blank_state(c: char) -> u8 {
		if c.is_whitespace() {
			1
		} else {
			9
		}
	}

	#[inline]
	fn case_state(c: char) -> u8 {
		if c.is_lowercase() {
			1
		} else if c.is_uppercase() {
			2
		} else {
			9
		}
	}

	#[inline]
	fn class_state(c: char) -> u8 {
		if c.is_alphabetic() {
			1
		} else if c.is_numeric() {
			2
		} else if c.is_whitespace() {
			3
		} else if c.is_ascii_punctuation() {
			4
		} else {
			9
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::analyzer::tests::test_analyser;

	#[test]
	fn test_split() {
		test_analyser(
			"DEFINE ANALYZER test TOKENIZERS blank,class FILTERS lowercase",
			"Abc12345xYZ DL1809 item123456 978-3-16-148410-0 1HGCM82633A123456",
			&[
				"abc", "12345", "xyz", "dl", "1809", "item", "123456", "978", "-", "3", "-", "16",
				"-", "148410", "-", "0", "1", "hgcm", "82633", "a", "123456",
			],
		);
	}
}
