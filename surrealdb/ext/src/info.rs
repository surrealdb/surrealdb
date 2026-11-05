use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{Value, json};
use surrealism_runtime::exports::FunctionExport;
use surrealism_runtime::package::SurrealismPackage;

use crate::json::{self, Failure, Kind};

pub async fn init(file: PathBuf, json: bool) -> Result<()> {
	let package = load(&file)?;
	let meta = &package.config.meta;

	if json {
		return json::print(&json!({
			"organisation": meta.organisation,
			"name": meta.name,
			"version": meta.version.to_string(),
			"repository": meta.repository,
			"logo_bytes": package.logo.as_ref().map(Vec::len),
			"exports": package.exports.functions.iter().map(export_document).collect::<Vec<_>>(),
		}));
	}

	let title = format!("Info for @{}/{}@{}", meta.organisation, meta.name, meta.version,);
	println!("\n{title}");
	println!("{}\n", "=".repeat(title.len() + 2));

	if let Some(ref logo) = package.logo {
		println!("Logo: included ({} bytes)", logo.len());
	}

	for export in &package.exports.functions {
		let name = match &export.name {
			None => "<mod>".to_string(),
			Some(n) => format!("<mod>::{n}"),
		};

		let mode = if export.writeable {
			"writeable"
		} else {
			"readonly"
		};
		println!("- {name}({}) -> {} [{mode}]", export.args_display(), export.returns_display());
		if let Some(comment) = &export.comment {
			println!("  COMMENT: {comment}");
		}
	}

	Ok(())
}

/// Load the archive at `file`: a missing file is kind `not_found`, and one the
/// loader refuses is kind `invalid`.
pub(crate) fn load(file: &Path) -> Result<SurrealismPackage> {
	if !file.exists() {
		return Err(
			Failure::new(Kind::NotFound, format!("{} does not exist", file.display())).into()
		);
	}
	SurrealismPackage::from_file(file.to_path_buf()).map_err(|e| {
		Failure::new(Kind::Invalid, format!("Failed to load Surrealism package: {e}")).into()
	})
}

/// One export as `--json` shows it. `name` is `null` for the default export,
/// and the kinds are SurrealQL's spelling, as `DEFINE FUNCTION` reads them.
pub(crate) fn export_document(export: &FunctionExport) -> Value {
	json!({
		"name": export.name,
		"args": export
			.args
			.iter()
			.map(|(name, kind)| json!({ "name": name, "kind": kind.to_string() }))
			.collect::<Vec<_>>(),
		"returns": export.returns.to_string(),
		"writeable": export.writeable,
		"comment": export.comment,
	})
}

#[cfg(test)]
mod tests {
	use surrealdb_types::Kind as SqlKind;

	use super::*;

	#[test]
	fn an_export_document_names_its_arguments_and_kinds() {
		let export = FunctionExport {
			name: Some("mix".into()),
			args: vec![("a".into(), SqlKind::String), ("b".into(), SqlKind::Int)],
			returns: SqlKind::Bool,
			args_text: None,
			returns_text: None,
			writeable: true,
			declares_transaction: false,
			comment: Some("Mixes".into()),
		};
		assert_eq!(
			export_document(&export),
			json!({
				"name": "mix",
				"args": [{"name": "a", "kind": "string"}, {"name": "b", "kind": "int"}],
				"returns": "bool",
				"writeable": true,
				"comment": "Mixes",
			})
		);
	}

	/// `info --json` reads the package and every export from the archive.
	#[test]
	fn the_info_document_describes_the_archive() {
		let dir = tempfile::tempdir().expect("a temporary directory");
		let file = dir.path().join("color.surli");
		let archive = crate::install::fixture::pack(&crate::install::fixture::files(), None);
		std::fs::write(&file, archive).expect("write the fixture");
		let Ok(package) = load(&file) else {
			panic!("the fixture did not load");
		};
		assert_eq!(package.config.meta.name, "color");
		let exports: Vec<_> = package.exports.functions.iter().map(export_document).collect();
		assert_eq!(exports.len(), 2);
		assert_eq!(exports[1]["name"], "recolor");
		assert_eq!(exports[1]["writeable"], true);
		assert_eq!(exports[1]["returns"], "string");
	}

	#[test]
	fn a_missing_archive_is_not_found() {
		let Err(err) = load(Path::new("does-not-exist.surli")) else {
			panic!("a missing archive loaded");
		};
		assert_eq!(err.downcast_ref::<Failure>().map(|f| f.kind), Some(Kind::NotFound));
	}
}
