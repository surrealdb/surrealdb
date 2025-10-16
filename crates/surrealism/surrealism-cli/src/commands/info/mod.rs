use crate::{commands::SurrealismCommand, host::DemoHost};
use anyhow::Result;
use std::path::PathBuf;
use surrealdb_types::Kind;
use surrealism_runtime::package::SurrealismPackage;
use surrealism_types::err::PrefixError;

pub struct InfoCommand {
    pub file: PathBuf,
}

impl SurrealismCommand for InfoCommand {
    fn run(self) -> anyhow::Result<()> {
        let package = SurrealismPackage::from_file(self.file)
            .prefix_err(|| "Failed to load Surrealism package")?;
        let meta = package.config.meta.clone();
        let runtime = surrealism_runtime::controller::Runtime::new(package)?;

        // Load the WASM module from memory
        let mut controller = runtime.new_controller(Box::new(DemoHost::new()))
            .prefix_err(|| "Failed to load WASM module")?;

        let exports = controller
            .list()
            .prefix_err(|| "Failed to list functions in the WASM module")?
            .into_iter()
            .map(|name| {
                let args = controller
                    .args(Some(name.clone()))
                    .prefix_err(|| format!("Failed to collect arguments for function '{name}'"))?;
                let returns = controller.returns(Some(name.clone())).prefix_err(|| {
                    format!("Failed to collect return type for function '{name}'")
                })?;

                Ok((name, args, returns))
            })
            .collect::<Result<Vec<(String, Vec<Kind>, Kind)>>>()?;

        let title = format!(
            "Info for @{}/{}@{}",
            meta.organisation, meta.name, meta.version,
        );
        println!("\n{title}");
        println!("{}\n", "=".repeat(title.len() + 2));

        for (name, args, returns) in exports {
            let name = if name.is_empty() {
                "<mod>".to_string()
            } else {
                format!("<mod>::{name}")
            };

            println!(
                "- {name}({}) -> {}",
                args.iter()
                    .map(|arg| format!("{arg}"))
                    .collect::<Vec<_>>()
                    .join(", "),
                returns
            );
        }

        Ok(())
    }
}
