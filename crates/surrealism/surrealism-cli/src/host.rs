use std::io::BufRead;

use anyhow::Result;
use async_trait::async_trait;
use surrealdb_types::ToSql;
use surrealism_runtime::config::SurrealismConfig;
use surrealism_runtime::host::InvocationContext;
use surrealism_runtime::kv::{BTreeMapStore, KVStore};

use crate::parse_value;

pub struct DemoHost {
	kv: BTreeMapStore,
}

impl DemoHost {
	pub fn new() -> Self {
		Self {
			kv: BTreeMapStore::new(),
		}
	}
}

#[async_trait]
impl InvocationContext for DemoHost {
	fn kv(&mut self) -> Result<&dyn KVStore> {
		Ok(&self.kv)
	}

	async fn sql(
		&mut self,
		_config: &SurrealismConfig,
		query: String,
		vars: surrealdb_types::Object,
	) -> Result<surrealdb_types::Value> {
		println!("The module is running a SQL query:");
		println!("SQL: {query}");
		println!("Vars: {}", vars.to_sql());
		println!("Please enter the result:");

		let stdin = std::io::stdin();
		loop {
			let line = match stdin.lock().lines().next() {
				Some(Ok(line)) => line,
				Some(Err(e)) => {
					anyhow::bail!("Failed to read from stdin: {e}");
				}
				None => {
					anyhow::bail!("stdin closed unexpectedly");
				}
			};

			match parse_value(&line) {
				Ok(x) => {
					println!(" ");
					return Ok(x);
				}
				Err(e) => {
					println!("Failed to parse value: {e}");
					println!("Please try again");
				}
			}
		}
	}

	async fn run(
		&mut self,
		_config: &SurrealismConfig,
		fnc: String,
		version: Option<String>,
		args: Vec<surrealdb_types::Value>,
	) -> Result<surrealdb_types::Value> {
		let version = version.map(|x| format!("<{x}>")).unwrap_or_default();
		println!("The module is running a function:");
		println!(
			" - {fnc}{version}({})",
			args.iter().map(|x| x.to_sql().clone()).collect::<Vec<String>>().join(", ")
		);
		println!("\nPlease enter the result:");

		let stdin = std::io::stdin();
		loop {
			let line = match stdin.lock().lines().next() {
				Some(Ok(line)) => line,
				Some(Err(e)) => {
					anyhow::bail!("Failed to read from stdin: {e}");
				}
				None => {
					anyhow::bail!("stdin closed unexpectedly");
				}
			};

			match parse_value(&line) {
				Ok(x) => {
					println!(" ");
					return Ok(x);
				}
				Err(e) => {
					println!("Failed to parse value: {e}");
					println!("Please try again");
				}
			}
		}
	}

	fn stdout(&mut self, output: &str) -> Result<()> {
		println!("[surli::out] {}", output);
		Ok(())
	}

	fn stderr(&mut self, output: &str) -> Result<()> {
		eprintln!("[surli::err] {}", output);
		Ok(())
	}
}
