use std::collections::HashMap;
#[cfg(target_family = "wasm")]
use std::fs::File;
#[cfg(target_family = "wasm")]
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
#[cfg(not(target_family = "wasm"))]
use tokio::fs::File;
#[cfg(not(target_family = "wasm"))]
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::Error;
use crate::file::check_is_path_allowed;
use crate::ft::analyzer::filter::{FilterResult, Term};

#[derive(Clone, Default)]
pub(crate) struct Mapper {
	terms: Arc<HashMap<String, String>>,
}

impl Mapper {
	pub(crate) async fn new(path: &Path, allow_list: &[PathBuf]) -> Result<Self> {
		let mut terms = HashMap::new();
		let path = check_is_path_allowed(path, allow_list)?;
		Self::iterate_file(&mut terms, &path).await?;
		Ok(Self {
			terms: Arc::new(terms),
		})
	}

	fn add_line(terms: &mut HashMap<String, String>, line: &str, line_number: usize) -> Result<()> {
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

		terms.insert(rest.trim().to_string(), word.trim().to_string());

		Ok(())
	}

	#[cfg(not(target_family = "wasm"))]
	async fn iterate_file(terms: &mut HashMap<String, String>, path: &Path) -> Result<()> {
		let file = File::open(path).await?;
		let reader = BufReader::new(file);
		let mut lines = reader.lines();
		let mut line_number = 0;
		while let Some(line) = lines.next_line().await? {
			yield_now!();
			Self::add_line(terms, &line, line_number)?;
			line_number += 1;
		}
		Ok(())
	}

	#[cfg(target_family = "wasm")]
	async fn iterate_file(terms: &mut HashMap<String, String>, path: &Path) -> Result<()> {
		let file = File::open(path)?;
		let reader = BufReader::new(file);
		for (line_number, line_result) in reader.lines().enumerate() {
			let line = line_result?;
			Self::add_line(terms, &line, line_number)?;
		}
		Ok(())
	}

	pub(super) fn map(&self, token: &str) -> FilterResult {
		if let Some(lemme) = self.terms.get(token) {
			return FilterResult::Term(Term::NewTerm(lemme.clone(), 0));
		}
		FilterResult::Term(Term::Unchanged)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;
	use std::sync::Arc;

	use super::Mapper;
	use crate::ft::analyzer::filter::{FilterResult, Term};

	/// A malformed mapper line must not have its raw content echoed back in the
	/// error: doing so would leak the contents of the mapped file to the caller.
	///
	/// We assert the *exact* error strings (not just the absence of the secret)
	/// so that a future change which interpolates line content back into a
	/// message is caught regardless of which marker happens to be present.
	#[test]
	fn test_parse_error_does_not_leak_file_content() {
		const SECRET: &str = "super-secret-passwd-content";
		let mut terms = HashMap::new();

		// A line without a tab separator triggers the parse error path.
		let secret = format!("root:x:0:0:{SECRET}:/root:/bin/sh");
		let err = Mapper::add_line(&mut terms, &secret, 0)
			.expect_err("a line without a tab must fail to parse");
		let msg = err.to_string();
		assert!(!msg.contains(SECRET), "parser error leaked file content: {msg}");
		assert!(
			msg.contains("Expected two terms separated by a tab on line 0"),
			"unexpected error message: {msg}"
		);

		// A line with too many tabs must also not echo its content.
		let secret_tabs = format!("a\tb\t{SECRET}");
		let err = Mapper::add_line(&mut terms, &secret_tabs, 1)
			.expect_err("a line with multiple tabs must fail to parse");
		let msg = err.to_string();
		assert!(!msg.contains(SECRET), "parser error leaked file content: {msg}");
		assert!(
			msg.contains("Expected two terms to not contain more than one tab on line 1"),
			"unexpected error message: {msg}"
		);
	}

	/// A well-formed line carrying sensitive content in the key position must
	/// parse and insert successfully without error.
	#[test]
	fn test_wellformed_line_with_secret_in_key_does_not_error() {
		const SECRET: &str = "super-secret-passwd-content";
		let mut terms = HashMap::new();
		let line = format!("word\t{SECRET}");
		Mapper::add_line(&mut terms, &line, 0)
			.expect("a well-formed mapper line must parse without error");
		assert_eq!(terms.get(SECRET).map(String::as_str), Some("word"));
	}

	#[test]
	fn test_mapper_replaces_known_term() {
		let mut terms = HashMap::new();
		Mapper::add_line(&mut terms, "better\tgood", 0).unwrap();
		let mapper = Mapper {
			terms: Arc::new(terms),
		};
		match mapper.map("good") {
			FilterResult::Term(Term::NewTerm(replacement, pos)) => {
				assert_eq!(replacement, "better");
				assert_eq!(pos, 0);
			}
			_ => panic!("expected term replacement"),
		}
		assert!(matches!(mapper.map("unknown"), FilterResult::Term(Term::Unchanged)));
	}
}
