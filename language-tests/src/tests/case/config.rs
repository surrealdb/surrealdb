use std::ops::Range;
use std::str::CharIndices;

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use serde::de::IntoDeserializer;
use toml_edit::DocumentMut;

use crate::tests::schema::TestConfig;

#[derive(Debug)]
pub struct CaseConfig {
	/// The toml document from which the config is serialized,
	/// Is non if no config was found.
	pub toml: Option<DocumentMut>,
	/// The offset within origin in which the config can be found.
	/// Is none if no config was found.
	pub range: Option<Range<usize>>,
	/// The serialized config.
	pub parsed: TestConfig,
}

struct Parser<'a> {
	source: &'a str,
	chars: CharIndices<'a>,
	peek: Option<(usize, char)>,
}

impl<'a> Parser<'a> {
	pub fn new(source: &'a str) -> Parser<'a> {
		Parser {
			chars: source.char_indices(),
			source,
			peek: None,
		}
	}

	pub fn next(&mut self) -> Option<char> {
		if let Some((_, x)) = self.peek.take() {
			return Some(x);
		}
		self.chars.next().map(|x| x.1)
	}

	pub fn peek(&mut self) -> Option<char> {
		if let Some((_, x)) = self.peek {
			return Some(x);
		}
		Some(self.peek.insert(self.chars.next()?).1)
	}

	pub fn eat(&mut self, c: char) -> bool {
		if let Some(x) = self.peek()
			&& x == c
		{
			self.next();
			return true;
		}
		false
	}

	pub fn offset(&mut self) -> usize {
		self.peek();
		self.peek.map(|x| x.0).unwrap_or(self.source.len())
	}

	pub fn extract_config(source: &'a str) -> Result<Option<Range<usize>>> {
		let mut parser = Self::new(source);
		let mut res = None;

		while let Some(x) = parser.next() {
			if x == '/' && parser.eat('*') && parser.eat('*') {
				ensure!(res.is_none(), "Test case contains multiple config sections");

				let start = parser.offset();

				let end = loop {
					let offset = parser.offset();

					let Some(x) = parser.next() else {
						bail!("Test case config was not closed")
					};

					if x == '*' && parser.eat('/') {
						break offset;
					}
				};

				res = Some(start..end);
			}
		}

		Ok(res)
	}
}

impl CaseConfig {
	pub fn parse(source: &str) -> Result<Self> {
		if let Some(config_range) = Parser::extract_config(source)? {
			let config_source = &source[config_range.clone()];

			let toml: DocumentMut =
				config_source.parse().context("Could not parse test case config toml")?;

			let config = TestConfig::deserialize(toml.clone().into_deserializer())
				.context("Could not deserialize test case config")?;

			Ok(Self {
				range: Some(config_range),
				toml: Some(toml),
				parsed: config,
			})
		} else {
			Ok(Self {
				range: None,
				toml: None,
				parsed: Default::default(),
			})
		}
	}
}
