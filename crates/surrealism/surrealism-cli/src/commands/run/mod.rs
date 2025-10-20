use std::path::PathBuf;

use anyhow::Result;
use surrealdb_types::ToSql;
use surrealism_runtime::controller::Runtime;
use surrealism_runtime::package::SurrealismPackage;
use surrealism_types::err::PrefixError;

use crate::commands::SurrealismCommand;
use crate::host::DemoHost;

pub struct RunCommand {
	pub file: PathBuf,
	pub fnc: Option<String>,
	pub args: Vec<surrealdb_types::Value>,
}

impl SurrealismCommand for RunCommand {
	fn run(self) -> Result<()> {
		let package = SurrealismPackage::from_file(self.file)?;

		// Load the WASM module
		let runtime = Runtime::new(package)?;
		let mut controller =
			runtime.new_controller().prefix_err(|| "Failed to load WASM module")?;
		let mut host = DemoHost::new();

		controller.with_context(&mut host, |controller| controller.init())?;

		// Invoke the function with the provided arguments
		let result =
			controller.with_context(&mut host, |controller| controller.invoke(self.fnc, self.args));

		match result {
			Ok(result) => {
				println!("✅ {:#}", result.to_sql());
			}
			Err(e) => {
				eprintln!("❌ {}", e);
				return Err(e.into());
			}
		}

		Ok(())
	}
}
