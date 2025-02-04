use crate::err::Error;
use crate::idx::ft::analyzer::filter::{FilterResult, Term};
#[cfg(target_family = "wasm")]
use std::fs::File;
#[cfg(target_family = "wasm")]
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
use tokio::fs::File;
#[cfg(not(target_family = "wasm"))]
use tokio::io::{AsyncBufReadExt, BufReader};
use vart::art::Tree;
use vart::VariableSizeKey;
#[derive(Clone, Default)]
pub(in crate::idx) struct Mapper {
	terms: Arc<Tree<VariableSizeKey, String>>,
}

impl Mapper {
	pub(in crate::idx) async fn new(path: &Path) -> Result<Self, Error> {
		let mut terms = Tree::new();
		Self::iterate_file(&mut terms, path).await?;
		Ok(Self {
			terms: Arc::new(terms),
		})
	}

	fn add_line_tree(
		terms: &mut Tree<VariableSizeKey, String>,
		line: String,
		line_number: usize,
	) -> Result<(), Error> {
		let Some((word, rest)) = line.split_once('\t') else {
			return Err(Error::AnalyzerError(format!(
				"Expected two terms separated by a tab line {line_number}: {}",
				line
			)));
		};

		if rest.contains('\t') {
			return Err(Error::AnalyzerError(format!(
				"Expected two terms to not contain more then one tab {line_number}: {}\t{}",
				word, rest
			)));
		}
		let key = VariableSizeKey::from_str(rest.trim())
			.map_err(|_| Error::AnalyzerError(format!("Can't create key from {word}")))?;
		terms
			.insert_unchecked(&key, word.trim().to_string(), 0, 0)
			.map_err(|e| Error::AnalyzerError(e.to_string()))?;

		Ok(())
	}

	#[cfg(not(target_family = "wasm"))]
	async fn iterate_file(
		terms: &mut Tree<VariableSizeKey, String>,
		path: &Path,
	) -> Result<(), Error> {
		let file = File::open(path).await?;
		let reader = BufReader::new(file);
		let mut lines = reader.lines();
		let mut line_number = 0;
		while let Some(line) = lines.next_line().await? {
			Self::add_line_tree(terms, line, line_number)?;
			line_number += 1;
		}
		Ok(())
	}

	#[cfg(target_family = "wasm")]
	async fn iterate_file(
		terms: &mut Tree<VariableSizeKey, String>,
		path: &Path,
	) -> Result<(), Error> {
		let file = File::open(path)?;
		let reader = BufReader::new(file);
		let mut line_number = 0;
		for line_result in reader.lines() {
			let line = line_result?;
			Self::add_line_tree(terms, line, line_number)?;
			line_number += 1;
		}
		Ok(())
	}

	pub(super) fn map(&self, token: &str) -> FilterResult {
		if let Ok(key) = VariableSizeKey::from_str(token) {
			if let Some((lemme, _, _)) = self.terms.get(&key, 0) {
				return FilterResult::Term(Term::NewTerm(lemme, 0));
			}
		}
		FilterResult::Term(Term::Unchanged)
	}
}
