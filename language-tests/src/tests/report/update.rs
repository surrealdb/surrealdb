use std::cmp::Ordering;
use std::ops::Range;

use anyhow::{Context, Result};
use surrealdb_types::{ToSql, Value as SurValue};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table};

use super::TestReport;
use crate::tests::report::{TestGrade, TestOutputs};

impl TestReport {
	pub async fn update_result_set(root_path: &str, overwrite: bool, set: &[Self]) -> Result<()> {
		let mut subset: Vec<&_> = if overwrite {
			set.iter()
				.filter(|x| {
					matches!(x.grade(), TestGrade::Failed | TestGrade::Warning) && !x.is_wip()
				})
				.collect()
		} else {
			set.iter().filter(|x| x.is_unspecified_test() && !x.is_wip()).collect()
		};

		subset.sort_by(|a, b| {
			let a_origin = &a.case.test.origin;
			let b_origin = &b.case.test.origin;

			a_origin
				.path
				.cmp(&b_origin.path)
				// Make tests later in the file are sorted first to ensure
				// that when modifying the test later modifications will still have the correct
				// offset.
				.then_with(|| {
					a_origin
						.subset
						.as_ref()
						.map(|x| x.end)
						.cmp(&b_origin.subset.as_ref().map(|x| x.end))
						.reverse()
				})
		});

		// The same test should have the same result,
		// so no need to write multiple time per test origin.
		subset.dedup_by(|a, b| {
			let a_origin = &a.case.test.origin;
			let b_origin = &b.case.test.origin;

			a_origin.path == b_origin.path && a_origin.subset == b_origin.subset
		});

		for s in subset {
			s.update_config_results(root_path).await?;
		}

		Ok(())
	}

	async fn update_config_results(&self, root_path: &str) -> Result<()> {
		//TODO: Improve for multiple test cases in a single file.
		let Some(values) = self.outputs.as_ref() else {
			println!("tried to update test {} without results", self.case.test.origin.path);
			return Ok(());
		};

		let mut doc = self.case.test.config.toml.clone().unwrap_or_default();

		println!("Updating for test `{}`", self.name);

		match values {
			TestOutputs::Values(values) => apply_results(&mut doc, values, self.traces()),
			TestOutputs::ParsingError(error) => apply_error(&mut doc, "parsing-error", error),
			TestOutputs::SigninError(error) => apply_error(&mut doc, "signin-error", error),
			TestOutputs::SignupError(error) => apply_error(&mut doc, "signup-error", error),
			TestOutputs::RequestError(error) => apply_error(&mut doc, "request-error", error),
		}

		let mut existing = self.case.test.source.clone().into_bytes();

		if let Some(slice) = self.case.test.config.range.clone() {
			insert_slice(&mut existing, slice.clone(), doc.to_string().as_bytes());
		} else {
			insert_slice(&mut existing, 0..0, format!("/**\n{}\n*/\n", doc).as_bytes());
		}

		let full_path = if root_path.ends_with("/") {
			format!("{}{}", root_path, self.case.test.origin.path)
		} else {
			format!("{}/{}", root_path, self.case.test.origin.path)
		};

		if let Some(range) = self.case.test.origin.subset.as_ref() {
			let mut full_source = tokio::fs::read(&full_path)
				.await
				.context("Could not read file to insert modified test")?;

			assert_eq!(
				&full_source[range.clone()],
				self.case.test.source.as_bytes(),
				"Source changed while trying to update the file"
			);

			insert_slice(&mut full_source, range.clone(), &existing);
		}

		let mut f = fs::OpenOptions::new()
			.write(true)
			.truncate(true)
			.create(false)
			.open(&full_path)
			.await
			.with_context(|| {
				format!("Could not open test file `{full_path}` to insert modified test")
			})?;
		f.write_all(&existing).await?;
		f.flush().await?;
		Ok(())
	}
}

pub fn insert_slice(bytes: &mut Vec<u8>, at: Range<usize>, src: &[u8]) {
	match src.len().cmp(&at.len()) {
		Ordering::Less => {
			let diff = at.len() - src.len();
			let dest = at.start + src.len();
			bytes.copy_within(at.end.., dest);
			bytes[at.start..dest].copy_from_slice(src);
			bytes.truncate(bytes.len() - diff);
		}
		Ordering::Greater => {
			let diff = src.len() - at.len();
			let copy_range = at.end..bytes.len();

			bytes.resize(bytes.len() + diff, 0);

			let dest = at.start + src.len();
			bytes.copy_within(copy_range, dest);
			bytes[at.start..dest].copy_from_slice(src);
		}
		Ordering::Equal => {
			bytes[at].copy_from_slice(src);
		}
	}
}

pub fn apply_error(doc: &mut DocumentMut, error_field: &str, error: &str) {
	let mut table = Table::new();
	table.insert(error_field, error.into());

	*doc.entry("test")
		.or_insert_with(toml_edit::table)
		.as_table_mut()
		.unwrap()
		.entry("results")
		.or_insert(Item::None) = Item::Table(table);
}

/// The TOML entry an expected value is written as.
///
/// A string whose text spans lines — what an `EXPLAIN` renders — is written
/// as a multi-line literal holding the text as it stands, so the expectation
/// reads as the plan tree it is rather than as one line of escapes. Every
/// other value, and any text a literal string cannot hold (one carrying a
/// literal-string delimiter of its own, or a carriage return), is written on
/// one line.
fn expectation(value: &SurValue) -> Item {
	let one_line = || toml_edit::value(value.to_sql());
	let SurValue::String(text) = value else {
		return one_line();
	};
	if !text.as_str().contains('\n') {
		return one_line();
	}
	// The pretty rendering writes a string's newlines as they are.
	let pretty = value.to_sql_pretty();
	if pretty.contains("'''") || pretty.contains('\r') {
		return one_line();
	}
	// A multi-line literal keeps the representation it was parsed with, and no
	// constructor sets one: parse the entry and take the value it holds. The
	// newline after the opening delimiter is not part of the string.
	let Ok(parsed) = format!("value = '''\n{pretty}'''").parse::<DocumentMut>() else {
		return one_line();
	};
	match parsed.get("value").and_then(Item::as_value) {
		Some(value) => Item::Value(value.clone()),
		None => one_line(),
	}
}

/// The `trace` a result table records for statement `index`, where it sent one.
fn trace_of(traces: Option<&[Option<SurValue>]>, index: usize) -> Option<&SurValue> {
	traces.and_then(|traces| traces.get(index)).and_then(Option::as_ref)
}

pub fn apply_results(
	doc: &mut DocumentMut,
	values: &[Result<SurValue, String>],
	traces: Option<&[Option<SurValue>]>,
) {
	let results_array = doc
		.entry("test")
		.or_insert_with(toml_edit::table)
		.as_table_mut()
		.unwrap()
		.entry("results")
		.or_insert_with(|| Item::ArrayOfTables(ArrayOfTables::new()));

	if let Some(arr) = results_array.as_array_of_tables_mut() {
		arr.clear();
		for (idx, r) in values.iter().enumerate() {
			if let Some(x) = arr.get_mut(idx) {
				match r {
					Ok(r) => {
						x["value"] = expectation(r);
					}
					Err(e) => {
						x["error"] = toml_edit::value(e.to_string());
					}
				}
				if let Some(trace) = trace_of(traces, idx) {
					x["trace"] = expectation(trace);
				}
			} else {
				let mut table = Table::default();
				match r {
					Ok(r) => {
						table["value"] = expectation(r);
					}
					Err(e) => {
						table["error"] = toml_edit::value(e.to_string());
					}
				}
				if let Some(trace) = trace_of(traces, idx) {
					table["trace"] = expectation(trace);
				}
				arr.push(table);
			}
		}
	} else {
		let Some(arr) = results_array.as_array_mut() else {
			panic!("Results should have been an array or a array of tables")
		};

		arr.clear();

		// A trace is a key of a result table, so a traced result is written as one.
		let traced = traces.is_some_and(|traces| traces.iter().any(Option::is_some));
		if values.iter().any(|x| x.is_err()) || traced {
			let mut t = ArrayOfTables::new();
			for (idx, r) in values.iter().enumerate() {
				let mut table = toml_edit::Table::new();
				match r {
					Ok(x) => {
						table.insert("value", expectation(x));
					}
					Err(e) => {
						table.insert("error", toml_edit::value(e.to_string()));
					}
				}
				if let Some(trace) = trace_of(traces, idx) {
					table.insert("trace", expectation(trace));
				}
				t.push(table);
			}
			*results_array = Item::ArrayOfTables(t);
		} else {
			for (idx, r) in values.iter().enumerate() {
				let v = expectation(r.as_ref().unwrap()).into_value().unwrap();
				if let Some(x) = arr.get_mut(idx) {
					*x = v;
				} else {
					arr.push(v)
				}
			}
		}
	}
}
