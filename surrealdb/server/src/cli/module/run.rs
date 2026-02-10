use std::path::PathBuf;

use anyhow::Result;
use surrealdb_types::ToSql;
use surrealism_runtime::controller::Runtime;
use surrealism_runtime::package::SurrealismPackage;
use surrealism_types::err::PrefixError;

use crate::cli::module::host::DemoHost;

pub async fn init(
	file: PathBuf,
	fnc: Option<String>,
	args: Vec<surrealdb_types::Value>,
) -> Result<()> {
	let package = SurrealismPackage::from_file(file)?;

	// Load the WASM module
	let runtime = Runtime::new(package)?;
	let host = Box::new(DemoHost::new());
	let mut controller =
		runtime.new_controller(host).await.prefix_err(|| "Failed to load WASM module")?;

	controller.init().await?;

	// Invoke the function with the provided arguments
	let result = controller.invoke(fnc, args).await;

	match result {
		Ok(result) => {
			println!("✅ {:#}", result.to_sql());
			Ok(())
		}
		Err(e) => {
			eprintln!("❌ {}", e);
			Err(e)
		}
	}
}
