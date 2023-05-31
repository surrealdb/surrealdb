use crate::err::Error;
use crate::idx::ft::doclength::DocLength;
use crate::idx::ft::filter::Filter;
use crate::idx::ft::postings::TermFrequency;
use crate::idx::ft::terms::{TermId, Terms};
use crate::kvs::Transaction;
use crate::sql::statements::DefineAnalyzerStatement;
use crate::sql::tokenizer::Tokenizer;
use crate::sql::Array;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

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
			f: Filter::from(az.filters),
		}
	}
}

impl Analyzer {
	pub(super) async fn extract_terms(
		&self,
		t: &Terms,
		tx: &mut Transaction,
		query_string: String,
	) -> Result<Vec<Option<TermId>>, Error> {
		let mut tokens = Tokens::new(query_string);
		self.walk(&mut tokens);
		// We first collect every unique terms
		// as it can contains duplicates
		let mut terms = HashSet::new();
		for token in &tokens.t {
			terms.insert(token);
		}
		// Now we can extract the term ids
		let mut res = Vec::with_capacity(terms.len());
		for term in terms {
			let term_id = t.get_term_id(tx, tokens.get_token_string(term)).await?;
			res.push(term_id);
		}
		Ok(res)
	}

	/// This method is used for indexing.
	/// It will create new term ids for non already existing terms.
	pub(super) async fn extract_terms_with_frequencies(
		&self,
		t: &mut Terms,
		tx: &mut Transaction,
		field_content: &Array,
	) -> Result<(DocLength, Vec<(TermId, TermFrequency)>), Error> {
		let mut doc_length = 0;
		// Let's first collect all the inputs, and collect the tokens.
		// We need to store them because everything after is zero-copy
		let mut inputs = Vec::with_capacity(field_content.0.len());
		for v in &field_content.0 {
			let input = v.to_owned().convert_to_string()?;
			let mut input = Tokens::new(input);
			self.walk(&mut input);
			inputs.push(input);
		}
		// We then collect every unique terms and count the frequency
		let mut terms = HashMap::new();
		for tokens in &inputs {
			for token in &tokens.t {
				doc_length += 1;
				match terms.entry(tokens.get_token_string(token)) {
					Entry::Vacant(e) => {
						e.insert(1);
					}
					Entry::Occupied(mut e) => {
						e.insert(*e.get() + 1);
					}
				}
			}
		}
		// Now we can extract the term ids
		let mut res = Vec::with_capacity(terms.len());
		for (term, freq) in terms {
			res.push((t.resolve_term_id(tx, term).await?, freq));
		}
		Ok((doc_length, res))
	}

	fn walk(&self, input: &mut Tokens) {
		if let Some(t) = &self.t {
			if !t.is_empty() {
				Walker::walk(t, &self.f, input);
			}
		}
	}
}

struct Walker {
	splitters: Vec<Splitter>,
}

impl Walker {
	fn new(t: &[Tokenizer]) -> Self {
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

	fn walk(t: &Vec<Tokenizer>, f: &Option<Vec<Filter>>, input: &mut Tokens) {
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

pub(super) struct Tokens {
	/// Then input string
	i: String,
	/// The final list of tokens
	t: Vec<Token>,
}

impl Tokens {
	fn new(i: String) -> Self {
		Self {
			i,
			t: Vec::new(),
		}
	}

	fn add(&mut self, f: &Option<Vec<Filter>>, s: usize, e: usize) {
		let mut t = Token::Ref(s, e);
		if let Some(f) = f {
			for f in f {
				let c = self.get_token_string(&t);
				let s = f.filter(c);
				// If the new token is equal to the old one, we keep the old one
				t = if s.eq(c) {
					t
				} else {
					Token::String(s.into())
				}
			}
		}
		self.t.push(t);
	}

	fn get_token_string<'a>(&'a self, t: &'a Token) -> &str {
		match t {
			Token::Ref(s, e) => &self.i[*s..*e],
			Token::String(s) => s,
		}
	}
}

#[derive(Debug, PartialOrd, PartialEq, Eq, Ord, Hash)]
pub(super) enum Token {
	Ref(usize, usize),
	String(String),
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
	use crate::idx::ft::analyzer::Tokens;
	use crate::sql::statements::define::analyzer;

	fn test_analyser(def: &str, input: &str, expected: Vec<&str>) {
		let (_, az) = analyzer(def).unwrap();
		let a: Analyzer = az.into();

		let mut tokens = Tokens::new(input.to_string());
		a.walk(&mut tokens);
		let mut res = vec![];
		for t in &tokens.t {
			res.push(tokens.get_token_string(t));
		}
		assert_eq!(res, expected, "{:?} => {:?}", tokens.i, tokens.t);
	}

	#[test]
	fn test_split() {
		test_analyser(
			"DEFINE ANALYZER test TOKENIZERS blank,class FILTERS lowercase",
			"Abc12345xYZ DL1809 item123456 978-3-16-148410-0 1HGCM82633A123456",
			vec![
				"abc", "12345", "xyz", "dl", "1809", "item", "123456", "978", "-", "3", "-", "16",
				"-", "148410", "-", "0", "1", "hgcm", "82633", "a", "123456",
			],
		);
	}

	#[test]
	fn test_stemmer() {
		test_analyser("DEFINE ANALYZER test TOKENIZERS blank,class FILTERS snowball(french);",
					  "Les chiens adorent courir dans le parc, mais mon petit chien aime plutôt se blottir sur le canapé que de courir",vec![
			"le", "chien", "adorent", "cour", "dan", "le", "parc", ",", "mais", "mon", "pet",
			"chien", "aim", "plutôt", "se", "blott", "sur", "le", "canap", "que", "de", "cour"
		]);
	}
}
