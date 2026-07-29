#[cfg(target_family = "wasm")]
use std::fs::File;
#[cfg(target_family = "wasm")]
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
#[cfg(not(target_family = "wasm"))]
use tokio::fs::File;
#[cfg(not(target_family = "wasm"))]
use tokio::io::{AsyncBufReadExt, BufReader};
use vart::VariableSizeKey;
use vart::art::Tree;

use crate::iam::file::check_is_path_allowed;
use crate::idx::Error;
use crate::idx::ft::analyzer::filter::{FilterResult, Term};

#[derive(Clone, Default)]
pub(in crate::idx) struct Mapper {
	terms: Arc<Tree<VariableSizeKey, String>>,
}

impl Mapper {
	pub(in crate::idx) async fn new(path: &Path, allow_list: &[PathBuf]) -> Result<Self> {
		let mut terms = Tree::new();
		let path = check_is_path_allowed(path, allow_list)?;
		Self::iterate_file(&mut terms, &path).await?;
		Ok(Self {
			terms: Arc::new(terms),
		})
	}

	fn add_line_tree(
		terms: &mut Tree<VariableSizeKey, String>,
		line: &str,
		line_number: usize,
	) -> Result<()> {
		// Error messages must not echo the file content: the mapped file is
		// operator-controlled and embedding its bytes in the response would
		// leak file contents to the caller (see SECURITY_GUIDE.md).
		let Some((word, rest)) = line.split_once('\t') else {
			bail!(Error::AnalyzerError(format!(
				"Expected two terms separated by a tab on line {line_number}"
			)));
		};

		ensure!(
			!rest.contains('\t'),
			Error::AnalyzerError(format!(
				"Expected two terms to not contain more than one tab on line {line_number}"
			))
		);

		let key = VariableSizeKey::from_str(rest.trim()).map_err(|_| {
			Error::AnalyzerError(format!("Can't create key from term on line {line_number}"))
		})?;
		terms
			.insert_unchecked(&key, word.trim().to_string(), 0, 0)
			.map_err(|e| Error::AnalyzerError(e.to_string()))?;

		Ok(())
	}

	#[cfg(not(target_family = "wasm"))]
	async fn iterate_file(terms: &mut Tree<VariableSizeKey, String>, path: &Path) -> Result<()> {
		let file = File::open(path).await?;
		let reader = BufReader::new(file);
		let mut lines = reader.lines();
		let mut line_number = 0;
		while let Some(line) = lines.next_line().await? {
			yield_now!();
			Self::add_line_tree(terms, &line, line_number)?;
			line_number += 1;
		}
		Ok(())
	}

	#[cfg(target_family = "wasm")]
	async fn iterate_file(terms: &mut Tree<VariableSizeKey, String>, path: &Path) -> Result<()> {
		let file = File::open(path)?;
		let reader = BufReader::new(file);
		let mut line_number = 0;
		for line_result in reader.lines() {
			let line = line_result?;
			Self::add_line_tree(terms, &line, line_number)?;
			line_number += 1;
		}
		Ok(())
	}

	pub(super) fn map(&self, token: &str) -> FilterResult {
		if let Ok(key) = VariableSizeKey::from_str(token)
			&& let Some((lemme, _, _)) = self.terms.get(&key, 0)
		{
			return FilterResult::Term(Term::NewTerm(lemme, 0));
		}
		FilterResult::Term(Term::Unchanged)
	}
}

#[cfg(test)]
mod tests {
	use vart::VariableSizeKey;
	use vart::art::Tree;

	use super::Mapper;

	/// A malformed mapper line must not have its raw content echoed back in the
	/// error: doing so would leak the contents of the mapped file to the caller.
	///
	/// We assert the *exact* error strings (not just the absence of the secret)
	/// so that a future change which interpolates line content back into a
	/// message is caught regardless of which marker happens to be present.
	#[test]
	fn test_parse_error_does_not_leak_file_content() {
		const SECRET: &str = "super-secret-passwd-content";
		let mut terms: Tree<VariableSizeKey, String> = Tree::new();

		// A line without a tab separator triggers the parse error path.
		let secret = format!("root:x:0:0:{SECRET}:/root:/bin/sh");
		let err = Mapper::add_line_tree(&mut terms, &secret, 0)
			.expect_err("a line without a tab must fail to parse");
		let msg = err.to_string();
		assert!(!msg.contains(SECRET), "parser error leaked file content: {msg}");
		assert!(
			msg.contains("Expected two terms separated by a tab on line 0"),
			"unexpected error message: {msg}"
		);

		// A line with too many tabs must also not echo its content.
		let secret_tabs = format!("a\tb\t{SECRET}");
		let err = Mapper::add_line_tree(&mut terms, &secret_tabs, 1)
			.expect_err("a line with multiple tabs must fail to parse");
		let msg = err.to_string();
		assert!(!msg.contains(SECRET), "parser error leaked file content: {msg}");
		assert!(
			msg.contains("Expected two terms to not contain more than one tab on line 1"),
			"unexpected error message: {msg}"
		);
	}

	/// The remaining two error branches in `add_line_tree` — the
	/// `VariableSizeKey::from_str` failure and the `Tree::insert_unchecked`
	/// failure — cannot be reached with the current `vart` types:
	/// `VariableSizeKey::from_str` is infallible (always `Ok`), and
	/// `insert_unchecked` at version `0` does not error for a well-formed key.
	/// They are nonetheless constructed to be content-free: the `from_str`
	/// branch discards the error with `|_|` and emits only the line number, and
	/// the `insert_unchecked` branch stringifies `vart::TrieError`, whose
	/// `Display` only emits fixed messages (no key/value bytes). This test
	/// documents that a well-formed line carrying secret-looking content in the
	/// key position inserts successfully — i.e. it does not spuriously surface
	/// those branches and leak content.
	#[test]
	fn test_wellformed_line_with_secret_in_key_does_not_error() {
		const SECRET: &str = "super-secret-passwd-content";
		let mut terms: Tree<VariableSizeKey, String> = Tree::new();
		let line = format!("word\t{SECRET}");
		Mapper::add_line_tree(&mut terms, &line, 0)
			.expect("a well-formed mapper line must parse without error");
	}
}
