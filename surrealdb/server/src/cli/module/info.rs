use std::path::PathBuf;

use anyhow::Result;
use surrealism_runtime::PrefixErr;
use surrealism_runtime::package::SurrealismPackage;

pub async fn init(file: PathBuf) -> Result<()> {
	let package =
		SurrealismPackage::from_file(file).prefix_err(|| "Failed to load Surrealism package")?;
	let meta = &package.config.meta;

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
