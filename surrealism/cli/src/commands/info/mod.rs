use std::path::PathBuf;

use surrealism_runtime::package::SurrealismPackage;
use surrealism_types::err::PrefixError;

use crate::commands::SurrealismCommand;
use crate::host::DemoHost;

pub struct InfoCommand {
	pub file: PathBuf,
}

impl SurrealismCommand for InfoCommand {
	async fn run(self) -> anyhow::Result<()> {
		let package = SurrealismPackage::from_file(self.file)
			.prefix_err(|| "Failed to load Surrealism package")?;
		let meta = package.config.meta.clone();
		let runtime = surrealism_runtime::controller::Runtime::new(package)?;

		// Load the WASM module from memory
		let host = Box::new(DemoHost::new());
		let mut controller =
			runtime.new_controller(host).await.prefix_err(|| "Failed to load WASM module")?;

		let exports =
			controller.list().prefix_err(|| "Failed to list functions in the WASM module")?;

		let mut results = Vec::new();
		for name in exports {
			let args = controller
				.args(Some(name.clone()))
				.await
				.prefix_err(|| format!("Failed to collect arguments for function '{name}'"))?;

			let returns = controller
				.returns(Some(name.clone()))
				.await
				.prefix_err(|| format!("Failed to collect return type for function '{name}'"))?;

			results.push((name, args, returns));
		}

		let exports = results;

		let title = format!("Info for @{}/{}@{}", meta.organisation, meta.name, meta.version,);
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
				args.iter().map(|arg| format!("{arg}")).collect::<Vec<_>>().join(", "),
				returns
			);
		}

		Ok(())
	}
}
