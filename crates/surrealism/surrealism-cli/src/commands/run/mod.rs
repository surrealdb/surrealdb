use crate::{commands::SurrealismCommand, host::DemoHost};
use anyhow::Result;
use surrealdb_types::ToSql;
use std::path::PathBuf;
use surrealism_runtime::package::SurrealismPackage;
use surrealism_types::err::PrefixError;

pub struct RunCommand {
    pub file: PathBuf,
    pub fnc: Option<String>,
    pub args: Vec<surrealdb_types::Value>,
}

impl SurrealismCommand for RunCommand {
    fn run(self) -> Result<()> {
        let package = SurrealismPackage::from_file(self.file)?;

        // Load the WASM module
        let host = DemoHost::new();
        let mut controller = surrealism_runtime::controller::Controller::new(package, host)
            .prefix_err(|| "Failed to load WASM module")?;
        controller.init()?;

        // Invoke the function with the provided arguments
        match controller.invoke(self.fnc, self.args) {
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
