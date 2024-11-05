use crate::err::Error;
use crate::idx::ft::analyzer::filter::{FilterResult, Term};
#[cfg(target_arch = "wasm32")]
use std::fs::File;
#[cfg(target_arch = "wasm32")]
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use tokio::fs::File;
#[cfg(not(target_arch = "wasm32"))]
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

	fn add_line_tree(terms: &mut Tree<VariableSizeKey, String>, line: String) -> Result<(), Error> {
		let mut parts = line.splitn(2, '\t');
		if let Some(lemme) = parts.next() {
			if let Some(word) = parts.next() {
				let key = VariableSizeKey::from_str(word)
					.map_err(|_| Error::Internal(format!("Can't create key from {word}")))?;
				terms
					.insert(&key, lemme.to_string(), 0, 0)
					.map_err(|e| Error::Internal(e.to_string()))?;
			}
		}
		Ok(())
	}

	#[cfg(not(target_arch = "wasm32"))]
	async fn iterate_file(
		terms: &mut Tree<VariableSizeKey, String>,
		path: &Path,
	) -> Result<(), Error> {
		let file = File::open(path).await?;
		let reader = BufReader::new(file);
		let mut lines = reader.lines();
		while let Some(line) = lines.next_line().await? {
			Self::add_line_tree(terms, line)?;
		}
		Ok(())
	}

	#[cfg(target_arch = "wasm32")]
	async fn iterate_file(
		terms: &mut Tree<VariableSizeKey, String>,
		path: &Path,
	) -> Result<(), Error> {
		let file = File::open(path)?;
		let reader = BufReader::new(file);
		for line_result in reader.lines() {
			let line = line_result?;
			Self::add_line_tree(terms, line)?;
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
