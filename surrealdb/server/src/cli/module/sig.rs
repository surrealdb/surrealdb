use std::path::PathBuf;

use anyhow::Result;
use surrealism_runtime::PrefixErr;
use surrealism_runtime::package::SurrealismPackage;

pub async fn init(file: PathBuf, fnc: Option<String>) -> Result<()> {
	let package =
		SurrealismPackage::from_file(file).prefix_err(|| "Failed to load Surrealism package")?;

	let export = package.exports.get_signature(fnc.as_deref()).ok_or_else(|| {
		let name = fnc.as_deref().unwrap_or("<default>");
		anyhow::anyhow!("function '{name}' not found in exports manifest")
	})?;

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
