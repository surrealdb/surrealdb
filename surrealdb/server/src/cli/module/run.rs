use std::path::PathBuf;

use anyhow::Result;
use surrealdb_types::ToSql;
use surrealism_runtime::package::SurrealismPackage;
use surrealism_runtime::runtime::Runtime;
use surrealism_runtime::{NetDeny, PrefixErr};

use crate::cli::module::host::DemoHost;

pub async fn init(
	file: PathBuf,
	fnc: Option<String>,
	args: Vec<surrealdb_types::Value>,
) -> Result<()> {
	let package = SurrealismPackage::from_file(file)?;

	// Load the WASM module
	// A local run, so there is no server deny list to apply.
	let runtime = Runtime::new(package, 8, None, None, NetDeny::Nothing)?;
	let host: std::sync::Arc<dyn surrealism_runtime::host::InvocationContext> =
		std::sync::Arc::new(DemoHost::new());
	let mut controller =
		runtime.new_controller(host.as_ref()).await.prefix_err(|| "Failed to load WASM module")?;

	controller.init().await?;

	// `surreal extension run` answers `sql`/`run` from stdin, so it lends a handle
	// where the server would — and withholds it where the server would, which
	// is for a function the manifest says declared no transaction parameter.
	let declares_transaction =
		runtime.exports().get_signature(fnc.as_deref()).is_none_or(|f| f.declares_transaction);
	let lent = declares_transaction.then(|| std::sync::Arc::clone(&host));
	let result = controller.invoke(lent, fnc, args).await;

	match result {
		Ok(result) => {
			println!("✅ {:#}", result.to_sql());
			Ok(())
		}
		Err(e) => {
			eprintln!("❌ {}", e);
			Err(e.into())
		}
	}
}
