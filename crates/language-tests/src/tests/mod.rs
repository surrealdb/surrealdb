use anyhow::{Context, Result, bail};
use schema::TestConfig;
use serde::{Deserialize, de::IntoDeserializer};
use set::TestId;
pub use set::TestSet;
use std::ops::Range;
use std::sync::Arc;
use toml_edit::DocumentMut;

pub mod cmp;
pub mod report;
pub mod schema;
pub mod set;

struct Parser<'a> {
	chars: &'a [u8],
	offset: usize,
	peek: Option<char>,
}

impl<'a> Parser<'a> {
	pub fn new(source: &'a [u8]) -> Parser<'a> {
		Parser {
			chars: source,
			offset: 0,
			peek: None,
		}
	}

	pub fn next(&mut self) -> Option<u8> {
		let res = self.chars.get(self.offset).copied()?;
		self.offset += 1;
		Some(res)
	}

	pub fn peek(&mut self) -> Option<u8> {
		self.chars.get(self.offset).copied()
	}

	pub fn eat(&mut self, c: u8) -> bool {
		if let Some(x) = self.peek() {
			if x == c {
				self.offset += 1;
				self.peek = None;
				return true;
			}
		}
		false
	}

	pub fn offset(&self) -> usize {
		self.offset
	}
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum ConfigKind {
	SingleLine,
	MultiLine,
	None,
}

pub struct ResolvedImport {
	pub id: TestId,
	pub path: String,
}

pub struct TestCase {
	pub path: String,
	pub toml: DocumentMut,
	pub config: Arc<TestConfig>,
	pub source: Vec<u8>,
	pub config_slice: Range<usize>,
	pub config_kind: ConfigKind,
	pub imports: Vec<ResolvedImport>,
	pub contains_error: bool,
}

impl TestCase {
	pub fn from_source_path(path: String, source: Vec<u8>) -> Result<Self> {
		let (range, config_kind, config_source) = Self::extract_config_text(&source)?;

		let config_source =
			String::from_utf8(config_source).context("Test configuration was not valid utf8")?;

		let toml: DocumentMut =
			config_source.parse().context("Failed to parse test case config")?;

		let config = TestConfig::deserialize(toml.clone().into_deserializer())
			.context("Failed to parse test case config")
			.inspect_err(|_| {
				println!("{config_source}");
			})?;

		let config = Arc::new(config);

		Ok(TestCase {
			toml,
			config,
			source,
			path,
			config_slice: range,
			config_kind,
			imports: Vec::new(),
			contains_error: false,
		})
	}

	fn extract_config_text(config: &[u8]) -> Result<(Range<usize>, ConfigKind, Vec<u8>)> {
		let mut res = Vec::with_capacity(config.len());

		let mut config_kind = ConfigKind::None;
		let mut next_should_be_config = false;

		let mut starts = 0;
		let mut end = 0;

		let mut tokens = Parser::new(config);
		'main: while let Some(n) = tokens.next() {
			match n {
				b'/' => {
					if tokens.eat(b'/') && tokens.eat(b'!') {
						if config_kind != ConfigKind::None {
							bail!("Found two config comments in the same file!");
						}

						if !next_should_be_config {
							starts = tokens.offset() - 3;
						}

						while let Some(x) = tokens.next() {
							if x == b'\n' {
								res.push(b'\n');
								next_should_be_config = true;
								break;
							}
							if x == b'\r' {
								tokens.eat(b'\n');
								res.push(b'\n');
								next_should_be_config = true;
								break;
							}
							res.push(x);
						}
					}

					if tokens.eat(b'*') && tokens.eat(b'*') {
						if config_kind != ConfigKind::None {
							bail!("Found two config comments in the same file!");
						}

						config_kind = ConfigKind::MultiLine;

						starts = tokens.offset();

						while let Some(x) = tokens.next() {
							if x == b'*' && tokens.eat(b'/') {
								continue 'main;
							}
							res.push(x);
							end = tokens.offset();
						}
						bail!("Invalid test, multi-line-test comment wasn't closed");
					}
				}
				_ => {
					if next_should_be_config {
						end = tokens.offset() - 1;
						next_should_be_config = false;
						config_kind = ConfigKind::SingleLine;
					}
				}
			}
		}

		if next_should_be_config {
			end = tokens.offset();
		}

		Ok((starts..end, config_kind, res))
	}
}
