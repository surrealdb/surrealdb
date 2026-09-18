use crate::bindings::Guest;
use crate::bindings::surrealism::plugin::host;
use crate::registry::{SurrealismEntry, SurrealismInit};

fn find_entry(name: Option<&str>) -> Result<&'static SurrealismEntry, String> {
	inventory::iter::<SurrealismEntry>()
		.find(|e| e.name == name)
		.ok_or_else(|| format!("unknown function: {}", name.unwrap_or("<default>")))
}

struct SurrealismPlugin;

impl Guest for SurrealismPlugin {
	async fn invoke(
		tx: Option<&host::Transaction>,
		name: Option<String>,
		args: Vec<u8>,
	) -> Result<Vec<u8>, String> {
		let entry = find_entry(name.as_deref())?;
		(entry.invoke)(tx, &args).await
	}

	fn list_functions() -> Vec<Option<String>> {
		inventory::iter::<SurrealismEntry>().map(|e| e.name.map(String::from)).collect()
	}

	fn function_args(name: Option<String>) -> Result<Vec<u8>, String> {
		(find_entry(name.as_deref())?.args)()
	}

	fn function_returns(name: Option<String>) -> Result<Vec<u8>, String> {
		(find_entry(name.as_deref())?.returns)()
	}

	fn function_writeable(name: Option<String>) -> Result<bool, String> {
		Ok(find_entry(name.as_deref())?.writeable)
	}

	fn function_declares_transaction(name: Option<String>) -> Result<bool, String> {
		Ok(find_entry(name.as_deref())?.declares_transaction)
	}

	fn function_comment(name: Option<String>) -> Result<Option<String>, String> {
		Ok(find_entry(name.as_deref())?.comment.map(String::from))
	}

	fn init() -> Result<(), String> {
		let mut inits = inventory::iter::<SurrealismInit>();
		let Some(init) = inits.next() else {
			return Ok(());
		};
		if inits.next().is_some() {
			return Err("multiple #[surrealism(init)] functions registered; only one is allowed"
				.to_string());
		}
		(init.0)()
	}
}

crate::bindings::export!(SurrealismPlugin with_types_in crate::bindings);
