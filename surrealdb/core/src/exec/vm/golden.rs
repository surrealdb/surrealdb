//! Expected outcomes the unit tests of this module compare against, kept as
//! text beside them in `goldens/`.
//!
//! A test that runs a corpus of statements records the outcome of each under a
//! key and calls [`Golden::check`], which compares the whole record with the
//! file of the same name. A file holds one entry per key, in the order the test
//! records them:
//!
//! ```text
//! ### <key>
//! <body>
//! ```
//!
//! A mismatch reports each entry that differs, with the expected and the actual
//! body. Setting `UPDATE_VM_GOLDENS` rewrites the files from the outcomes the
//! tests record, and the rewritten files are the diff to review.

use std::fmt::Write as _;
use std::path::PathBuf;

use surrealdb_types::ToSql;

use crate::val::Value;

/// Rows shown before and after the elision of a long result.
const SHOWN_AT_EACH_END: usize = 6;

/// Results longer than this are elided to their ends and a digest of the whole.
const ELIDED_ABOVE: usize = 24;

/// The outcomes a test records, for [`Golden::check`] to compare with the
/// file named `name`.
pub(super) struct Golden {
	name: &'static str,
	entries: Vec<(String, String)>,
}

impl Golden {
	pub(super) fn new(name: &'static str) -> Self {
		Self {
			name,
			entries: Vec::new(),
		}
	}

	/// Record `body` as the outcome of the case `key`. A case recorded again
	/// is keyed with its ordinal, so a corpus that runs a statement twice
	/// records both runs.
	pub(super) fn record(&mut self, key: impl AsRef<str>, body: impl AsRef<str>) {
		let key = key.as_ref().replace('\n', "\\n");
		let repeat = format!("{key} #");
		let seen = self
			.entries
			.iter()
			.filter(|(recorded, _)| {
				*recorded == key
					|| recorded
						.strip_prefix(&repeat)
						.is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
			})
			.count();
		let key = if seen == 0 {
			key
		} else {
			format!("{key} #{}", seen + 1)
		};
		self.entries.push((key, body.as_ref().trim_end().to_owned()));
	}

	/// Record the rows or the error of a statement as the outcome of `key`.
	pub(super) fn rows(&mut self, key: impl AsRef<str>, outcome: &Result<Vec<Value>, String>) {
		self.record(key, render_outcome(outcome));
	}

	fn path(&self) -> PathBuf {
		PathBuf::from(env!("CARGO_MANIFEST_DIR"))
			.join("src/exec/vm/goldens")
			.join(format!("{}.txt", self.name))
	}

	fn render(&self) -> String {
		let mut out = String::new();
		for (key, body) in &self.entries {
			let _ = writeln!(out, "### {key}");
			if !body.is_empty() {
				let _ = writeln!(out, "{body}");
			}
		}
		out
	}

	/// Compare the recorded outcomes with the file, or rewrite it when
	/// `UPDATE_VM_GOLDENS` is set.
	pub(super) fn check(self) {
		let path = self.path();
		let actual = self.render();
		if std::env::var_os("UPDATE_VM_GOLDENS").is_some() {
			std::fs::create_dir_all(path.parent().expect("a directory")).expect("creates goldens");
			std::fs::write(&path, actual).expect("writes the golden");
			return;
		}
		let Ok(expected) = std::fs::read_to_string(&path) else {
			panic!("no golden at {}; record it with UPDATE_VM_GOLDENS=1", path.display());
		};
		if expected == actual {
			return;
		}
		let expected = parse(&expected);
		let mut report = String::new();
		let mut differing = 0;
		for (key, body) in &self.entries {
			match expected.iter().find(|(k, _)| k == key) {
				Some((_, want)) if want == body => {}
				Some((_, want)) => {
					differing += 1;
					if differing <= 8 {
						let _ = writeln!(report, "`{key}`\n  expected: {want}\n  actual:   {body}");
					}
				}
				None => {
					differing += 1;
					if differing <= 8 {
						let _ =
							writeln!(report, "`{key}` has no expected outcome\n  actual: {body}");
					}
				}
			}
		}
		for (key, _) in &expected {
			if !self.entries.iter().any(|(k, _)| k == key) {
				differing += 1;
				if differing <= 8 {
					let _ = writeln!(report, "`{key}` is expected but was not run");
				}
			}
		}
		if differing == 0 {
			report.push_str("the entries are the same but their order differs\n");
		}
		panic!("{differing} outcomes differ from {}:\n{report}", path.display());
	}

	/// The entries of the golden file `name`, as recorded.
	pub(super) fn recorded(name: &'static str) -> Vec<(String, String)> {
		let path = Self::new(name).path();
		let text = std::fs::read_to_string(&path)
			.unwrap_or_else(|_| panic!("no golden at {}", path.display()));
		parse(&text)
	}
}

/// The entries of a golden file.
fn parse(text: &str) -> Vec<(String, String)> {
	let mut entries: Vec<(String, String)> = Vec::new();
	for line in text.lines() {
		if let Some(key) = line.strip_prefix("### ") {
			entries.push((key.to_owned(), String::new()));
		} else if let Some((_, body)) = entries.last_mut() {
			if !body.is_empty() {
				body.push('\n');
			}
			body.push_str(line);
		}
	}
	entries
}

/// A row on one line.
fn render_row(value: &Value) -> String {
	value.to_sql().replace('\n', "\\n")
}

/// The rows of a result, one per line; a long result shows its ends and a
/// digest of every row.
pub(super) fn render_rows(rows: &[Value]) -> String {
	let lines: Vec<String> = rows.iter().map(render_row).collect();
	if lines.len() <= ELIDED_ABOVE {
		return lines.join("\n");
	}
	let mut digest: u64 = 0xcbf2_9ce4_8422_2325;
	for byte in lines.iter().flat_map(|line| line.bytes().chain(std::iter::once(b'\n'))) {
		digest = (digest ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
	}
	let mut out = lines[..SHOWN_AT_EACH_END].join("\n");
	let _ = write!(
		out,
		"\n... {} rows, digest {digest:016x} ...\n{}",
		lines.len(),
		lines[lines.len() - SHOWN_AT_EACH_END..].join("\n")
	);
	out
}

/// The rows or the error of a statement.
pub(super) fn render_outcome(outcome: &Result<Vec<Value>, String>) -> String {
	match outcome {
		Ok(rows) if rows.is_empty() => "(no rows)".to_owned(),
		Ok(rows) => render_rows(rows),
		Err(message) => format!("error: {}", message.replace('\n', "\\n")),
	}
}
