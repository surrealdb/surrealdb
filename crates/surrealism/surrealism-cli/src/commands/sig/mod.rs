use crate::{commands::SurrealismCommand, host::DemoHost};
use std::path::PathBuf;
use surrealism_runtime::package::SurrealismPackage;
use surrealism_types::err::PrefixError;

pub struct SigCommand {
    pub file: PathBuf,
    pub fnc: Option<String>,
}

impl SurrealismCommand for SigCommand {
    fn run(self) -> anyhow::Result<()> {
        let package = SurrealismPackage::from_file(self.file)
            .prefix_err(|| "Failed to load Surrealism package")?;

        // Load the WASM module from memory
        let host = DemoHost::new();
        let mut controller = surrealism_runtime::controller::Controller::new(package, host)
            .prefix_err(|| "Failed to load WASM module")?;

        // Invoke the function with the provided arguments
        let args = controller
            .args(self.fnc.clone())
            .prefix_err(|| "Failed to collect arguments")?;
        let returns = controller
            .returns(self.fnc.clone())
            .prefix_err(|| "Failed to collect return type")?;

        println!(
            "\nSignature:\n - {}({}) -> {}",
            self.fnc.as_deref().unwrap_or("<default>"),
            args.iter()
                .map(|arg| format!("{arg}"))
                .collect::<Vec<_>>()
                .join(", "),
            returns
        );

        Ok(())
    }
}
