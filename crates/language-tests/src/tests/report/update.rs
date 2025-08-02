use std::{cmp::Ordering, fmt::Write, ops::Range};

use crate::tests::{ConfigKind, TestSet, report::TestOutputs};
use anyhow::Result;
use surrealdb_core::val::Value as SurValue;
use tokio::{fs, io::AsyncWriteExt};
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table};

use super::TestReport;

impl TestReport {
	pub async fn update_config_results(&self, set: &TestSet) -> Result<()> {
		let Some(values) = self.outputs.as_ref() else {
			println!("tried to update test {} without results", set[self.id].path);
			return Ok(());
		};

		let mut doc = set[self.id].toml.clone();
		println!("Updating test `{}`", set[self.id].path);

		match values {
			TestOutputs::Values(values) => apply_results(&mut doc, values),
			TestOutputs::ParsingError(error) => apply_error(&mut doc, "parsing-error", error),
			TestOutputs::SigninError(error) => apply_error(&mut doc, "signin-error", error),
			TestOutputs::SignupError(error) => apply_error(&mut doc, "signup-error", error),
		}

		let mut existing = set[self.id].source.clone();

		match set[self.id].config_kind {
			ConfigKind::SingleLine => {
				let doc: String =
					doc.to_string().trim_end().lines().fold(String::new(), |mut acc, x| {
						writeln!(&mut acc, "//! {x}").unwrap();
						acc
					});
				insert_slice(&mut existing, set[self.id].config_slice.clone(), doc.as_bytes());
			}
			ConfigKind::MultiLine => {
				insert_slice(
					&mut existing,
					set[self.id].config_slice.clone(),
					doc.to_string().as_bytes(),
				);
			}
			ConfigKind::None => {
				insert_slice(
					&mut existing,
					set[self.id].config_slice.clone(),
					format!("/**\n{}\n*/\n", doc).as_bytes(),
				);
			}
		}

		let mut f = fs::OpenOptions::new()
			.write(true)
			.create(false)
			.truncate(true)
			.open(&set[self.id].path)
			.await?;

		f.write_all(&existing).await?;

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

pub fn apply_results(doc: &mut DocumentMut, values: &[Result<SurValue, String>]) {
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
						x["value"] = toml_edit::value(r.to_string());
					}
					Err(e) => {
						x["error"] = toml_edit::value(e.to_string());
					}
				}
			} else {
				let mut table = Table::default();
				match r {
					Ok(r) => {
						table["value"] = toml_edit::value(r.to_string());
					}
					Err(e) => {
						table["error"] = toml_edit::value(e.to_string());
					}
				}
				arr.push(table);
			}
		}
	} else {
		let Some(arr) = results_array.as_array_mut() else {
			panic!("Results should have been an array or a array of tables")
		};

		arr.clear();

		if values.iter().any(|x| x.is_err()) {
			let mut t = ArrayOfTables::new();
			for r in values.iter() {
				let mut table = toml_edit::Table::new();
				match r {
					Ok(x) => {
						table.insert("value", toml_edit::value(x.to_string()));
					}
					Err(e) => {
						table.insert("error", toml_edit::value(e.to_string()));
					}
				}
				t.push(table);
			}
		} else {
			for (idx, r) in values.iter().enumerate() {
				let v = toml_edit::value(r.as_ref().unwrap().to_string()).into_value().unwrap();
				if let Some(x) = arr.get_mut(idx) {
					*x = v;
				} else {
					arr.push(v)
				}
			}
		}
	}
}
