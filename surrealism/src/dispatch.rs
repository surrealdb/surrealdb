//! The WIT exports, answered from one function table: the `#[surrealism]`
//! entries collected by inventory, then the functions passed to
//! [`register`](crate::register) during `init`.
//!
//! [`register`](crate::register) refuses a name either half already holds, so
//! a name resolves to at most one function whichever half is searched first.

use surrealdb_types::Kind;

use crate::bindings::Guest;
use crate::bindings::surrealism::extension::host;
use crate::register::{Registered, with_registry};
use crate::registry::{SurrealismEntry, SurrealismInit};

#[derive(Clone, Copy)]
enum Function {
	Static(&'static SurrealismEntry),
	Registered(&'static Registered),
}

fn find(name: Option<&str>) -> Result<Function, String> {
	if let Some(e) = inventory::iter::<SurrealismEntry>().find(|e| e.name == name) {
		return Ok(Function::Static(e));
	}
	with_registry(|r| r.find(name))
		.map(Function::Registered)
		.ok_or_else(|| format!("unknown function: {}", name.unwrap_or("<default>")))
}

fn encode_args(args: &[(String, Kind)]) -> Result<Vec<u8>, String> {
	let args: Vec<(&str, Kind)> = args.iter().map(|(n, k)| (n.as_str(), k.clone())).collect();
	surrealdb_types::encode_argument_list(&args).map_err(|e| e.to_string())
}

#[cfg(target_os = "wasi")]
unsafe extern "C" {
	/// wasi-libc's hook for snapshotting tools. Drops the cached preopen table
	/// and the descriptor handles behind it, so the next filesystem call asks
	/// the host for its preopens again.
	fn __wasilibc_reset_preopens();
}

struct SurrealismPlugin;

impl Guest for SurrealismPlugin {
	async fn invoke(
		tx: Option<&host::Transaction>,
		name: Option<String>,
		args: Vec<u8>,
	) -> Result<Vec<u8>, String> {
		match find(name.as_deref())? {
			Function::Static(e) => (e.invoke)(tx, &args).await,
			Function::Registered(r) => r.invoke(tx, &args).await,
		}
	}

	fn list_functions() -> Vec<Option<String>> {
		let statics = inventory::iter::<SurrealismEntry>().map(|e| e.name.map(String::from));
		let registered =
			with_registry(|r| r.entries().iter().map(|e| e.name.clone()).collect::<Vec<_>>());
		statics.chain(registered).collect()
	}

	fn function_args(name: Option<String>) -> Result<Vec<u8>, String> {
		match find(name.as_deref())? {
			Function::Static(e) => (e.args)(),
			Function::Registered(r) => encode_args(&r.meta.args),
		}
	}

	fn function_returns(name: Option<String>) -> Result<Vec<u8>, String> {
		match find(name.as_deref())? {
			Function::Static(e) => (e.returns)(),
			Function::Registered(r) => {
				surrealdb_types::encode_kind(&r.meta.returns).map_err(|e| e.to_string())
			}
		}
	}

	fn function_writeable(name: Option<String>) -> Result<bool, String> {
		Ok(match find(name.as_deref())? {
			Function::Static(e) => e.writeable,
			// A function that takes no transaction cannot write, whatever its
			// metadata says, and the manifest should say so.
			Function::Registered(r) => r.meta.declares_transaction && r.meta.writeable,
		})
	}

	fn function_declares_transaction(name: Option<String>) -> Result<bool, String> {
		Ok(match find(name.as_deref())? {
			Function::Static(e) => e.declares_transaction,
			Function::Registered(r) => r.meta.declares_transaction,
		})
	}

	fn function_comment(name: Option<String>) -> Result<Option<String>, String> {
		Ok(match find(name.as_deref())? {
			Function::Static(e) => e.comment.map(String::from),
			Function::Registered(r) => r.meta.comment.clone(),
		})
	}

	/// Runs the `#[surrealism(init)]` function, the only window in which
	/// [`register`](crate::register) succeeds. `surreal ext build` calls
	/// it once and snapshots the result; a second call is refused, since it
	/// would register every function again.
	fn init() -> Result<(), String> {
		let mut inits = inventory::iter::<SurrealismInit>();
		let init = inits.next();
		if inits.next().is_some() {
			return Err("multiple #[surrealism(init)] functions registered; only one is allowed"
				.to_string());
		}
		with_registry(|r| r.begin_init())?;
		let result = init.map_or(Ok(()), |init| (init.0)());
		with_registry(|r| r.end_init());
		// `surreal ext build` snapshots memory as `init` leaves it, and
		// the handle indices wasi-libc caches for the preopened directory name
		// resources in the build's store, not the instance that later runs the
		// snapshot. Releasing them here leaves nothing open, and the first file
		// access in any later instance resolves the preopens afresh.
		#[cfg(target_os = "wasi")]
		// SAFETY: takes no arguments and touches only wasi-libc's own state;
		// `init` is sync-lifted, so no other guest code runs concurrently.
		unsafe {
			__wasilibc_reset_preopens();
		}
		result
	}
}

crate::bindings::export!(SurrealismPlugin with_types_in crate::bindings);
