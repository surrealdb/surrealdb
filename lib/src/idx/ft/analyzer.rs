use crate::err::Error;
use crate::idx::ft::doclength::DocLength;
use crate::idx::ft::postings::TermFrequency;
use crate::idx::ft::terms::{TermId, Terms};
use crate::kvs::Transaction;
use crate::sql::filter::Filter;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::tokenizer::Tokenizer;
use crate::sql::Array;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::str::Chars;

pub(crate) struct Analyzers {}

impl Analyzers {
	pub(crate) const LIKE: &'static str = "like";
}

pub(super) struct Analyzer {
	t: Option<Vec<Tokenizer>>,
	f: Option<Vec<Filter>>,
}

impl From<DefineAnalyzerStatement> for Analyzer {
	fn from(az: DefineAnalyzerStatement) -> Self {
		Self {
			t: az.tokenizers,
			f: az.filters,
		}
	}
}

impl Analyzer {
	// TODO: This is currently a place holder. It has to be replaced by the full analyzer/token/filter logic.
	pub(super) async fn extract_terms_with_frequencies(
		&self,
		terms: &mut Terms,
		tx: &mut Transaction,
		field_content: &Array,
	) -> Result<(DocLength, HashMap<TermId, TermFrequency>), Error> {
		let mut doc_length = 0;
		let mut terms_map = HashMap::new();
		for v in &field_content.0 {
			let input = v.clone().convert_to_string()?;
			let tokens = self.walk(&input);
			for token in tokens.t {
				let term_id = terms.resolve_term_id(tx, token.as_ref()).await?;
				doc_length += 1;
				match terms_map.entry(term_id) {
					Entry::Vacant(e) => {
						e.insert(1);
					}
					Entry::Occupied(mut e) => {
						e.insert(*e.get() + 1);
					}
				}
			}
		}
		Ok((doc_length, terms_map))
	}

	pub(super) fn walk<'a>(&'a self, i: &'a str) -> Tokens<'a> {
		let mut tokens = Tokens::new(i, &self.f);
		if let Some(t) = &self.t {
			if !t.is_empty() {
				tokens = Walker::new(t, tokens).walk();
			}
		}
		tokens
	}
}

struct Walker<'a> {
	splitters: Vec<Splitter>,
	chars: Chars<'a>,
	tokens: Tokens<'a>,
	last_pos: usize,
	current_pos: usize,
}

impl<'a> Walker<'a> {
	fn new(t: &Vec<Tokenizer>, tokens: Tokens<'a>) -> Self {
		Self {
			splitters: t.iter().map(|t| t.into()).collect(),
			chars: tokens.i.chars(),
			last_pos: 0,
			current_pos: 0,
			tokens,
		}
	}

	fn is_valid(c: char) -> bool {
		c.is_ascii_alphanumeric() || c.is_ascii_punctuation()
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

	fn walk(mut self) -> Tokens<'a> {
		while let Some(c) = self.chars.next() {
			let is_valid = Self::is_valid(c);
			let should_split = self.should_split(c);
			if should_split || !is_valid {
				// The last pos may be more advanced due to the is_valid process
				if self.last_pos < self.current_pos {
					self.tokens.add(self.last_pos, self.current_pos);
				}
				self.last_pos = self.current_pos;
				// If the character is not valid for indexing (space, control...)
				// Then we increase the last position to the next character
				if !is_valid {
					self.last_pos += 1;
				}
			}
			self.current_pos += c.len_utf8();
		}
		if self.current_pos != self.last_pos {
			self.tokens.add(self.last_pos, self.current_pos);
		}
		self.tokens
	}
}

pub(super) struct Tokens<'a> {
	/// Then input string
	i: &'a str,
	/// The possible filters
	f: &'a Option<Vec<Filter>>,
	/// The final list of tokens
	t: Vec<Token<'a>>,
}

impl<'a> Tokens<'a> {
	fn new(i: &'a str, f: &'a Option<Vec<Filter>>) -> Self {
		Self {
			f,
			i,
			t: vec![],
		}
	}

	fn add(&mut self, s: usize, e: usize) {
		let mut t = (&self.i[s..e]).into();
		if let Some(f) = self.f {
			for f in f {
				t = self.filter(f, t);
			}
		}
		self.t.push(t);
	}

	fn filter(&mut self, f: &'a Filter, t: Token<'a>) -> Token<'a> {
		let s: Token = match f {
			Filter::EdgeNgram(_, _) => {
				todo!()
			}
			Filter::Lowercase => t.as_ref().to_lowercase().into(),
			Filter::Snowball(_) => {
				todo!()
			}
		};
		// If the new token is equal to the old one, we keep the old one
		if t.as_ref().eq(s.as_ref()) {
			t
		} else {
			s
		}
	}
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Hash)]
enum Token<'a> {
	Ref(&'a str),
	String(String),
}

impl<'a> From<&'a str> for Token<'a> {
	fn from(s: &'a str) -> Self {
		Self::Ref(s)
	}
}

impl<'a> From<String> for Token<'a> {
	fn from(s: String) -> Self {
		Self::String(s)
	}
}

impl<'a> AsRef<str> for Token<'a> {
	fn as_ref(&self) -> &str {
		match self {
			Token::Ref(s) => s.as_ref(),
			Token::String(s) => s.as_ref(),
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
	use super::Analyzer;
	use crate::sql::statements::define::analyzer;

	#[test]
	fn test_split() {
		let (_, az) =
			analyzer("DEFINE ANALYZER test TOKENIZERS blank,class FILTERS lowercase,ascii;")
				.unwrap();
		let a: Analyzer = az.into();

		let tokens = a.walk("Abc12345xYZ DL1809 item123456 978-3-16-148410-0 1HGCM82633A123456");
		assert_eq!(
			tokens.t,
			vec![
				"abc".to_string().into(),
				"12345".into(),
				"xyz".to_string().into(),
				"dl".to_string().into(),
				"1809".into(),
				"item".into(),
				"123456".into(),
				"978".into(),
				"-".into(),
				"3".into(),
				"-".into(),
				"16".into(),
				"-".into(),
				"148410".into(),
				"-".into(),
				"0".into(),
				"1".into(),
				"hgcm".to_string().into(),
				"82633".into(),
				"a".to_string().into(),
				"123456".into()
			],
			"{} => {:?}",
			tokens.i,
			tokens.t
		);
	}
}
