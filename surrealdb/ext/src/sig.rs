use std::path::PathBuf;

use anyhow::Result;

use crate::info::{export_document, load};
use crate::json::{self, Failure, Kind};

pub async fn init(file: PathBuf, fnc: Option<String>, json: bool) -> Result<()> {
	let package = load(&file)?;

	let export = package.exports.get_signature(fnc.as_deref()).ok_or_else(|| {
		let name = fnc.as_deref().unwrap_or("<default>");
		Failure::new(Kind::NotFound, format!("function '{name}' not found in exports manifest"))
	})?;

	if json {
		return json::print(&export_document(export));
	}

	let mode = if export.writeable {
		"writeable"
	} else {
		"readonly"
	};
	println!(
		"\nSignature:\n - {}({}) -> {} [{mode}]",
		fnc.as_deref().unwrap_or("<default>"),
		export.args_display(),
		export.returns_display()
	);
	if let Some(comment) = &export.comment {
		println!("   COMMENT: {comment}");
	}

	Ok(())
}
