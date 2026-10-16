use std::path::PathBuf;

use anyhow::Result;
use surrealdb_types::ToSql;
use surrealism_runtime::epoch::{EngineSettings, engines};
use surrealism_runtime::package::SurrealismPackage;
use surrealism_runtime::runtime::{InstanceSettings, Runtime};
use surrealism_runtime::{NetDeny, NetPolicy, PrefixErr};

use crate::host::DemoHost;

pub async fn init(
	file: PathBuf,
	fnc: Option<String>,
	args: Vec<surrealdb_types::Value>,
) -> Result<()> {
	let package = SurrealismPackage::from_file(file)?;

	// A local run grants the module what its own manifest asks for; there is
	// no server deny list to apply.
	let requested = &package.config.capabilities;
	let settings = InstanceSettings {
		net: NetPolicy::parse(&requested.allow_net, NetDeny::Nothing)?,
		max_memory_bytes: requested.max_memory_bytes,
		max_execution_time: requested.max_execution_time,
		strict_timeout: requested.strict_timeout,
		..InstanceSettings::local()
	};
	let runtime = Runtime::new(
		package,
		engines(EngineSettings {
			max_concurrent_invocations: 1,
			max_memory_bytes: None,
		}),
	)?;
	// `Runtime::new` compiles only the guarded engine.
	runtime.prepare(settings.strict_timeout)?;
	let host: std::sync::Arc<dyn surrealism_runtime::host::InvocationContext> =
		std::sync::Arc::new(DemoHost::new());
	let mut controller = runtime
		.instantiate(&settings, None, None)
		.await
		.prefix_err(|| "Failed to load WASM module")?;

	// `surreal ext run` answers `sql`/`run` from stdin, so it lends a handle
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
