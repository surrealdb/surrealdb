use crate::err::Error;
use crate::idx::ft::analyzer::filter::{FilterResult, Term};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use vart::art::Tree;
use vart::VariableSizeKey;

#[derive(Clone, Default)]
pub(in crate::idx) struct Mapper {
	terms: Arc<Tree<VariableSizeKey, String>>,
}

impl Mapper {
	pub(in crate::idx) async fn new(path: &Path) -> Result<Self, Error> {
		let file = File::open(path).await?;
		let reader = BufReader::new(file);

		let mut lines = reader.lines();
		let mut terms = Tree::new();
		while let Some(line) = lines.next_line().await? {
			// Split the line on tab
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
		}
		Ok(Self {
			terms: Arc::new(terms),
		})
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
