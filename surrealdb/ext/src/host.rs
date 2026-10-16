use std::io::BufRead;

use async_trait::async_trait;
use surrealdb_types::ToSql;
use surrealism_runtime::host::InvocationContext;
use surrealism_runtime::{HostCallError, HostCallResult};

pub struct DemoHost;

impl DemoHost {
	pub fn new() -> Self {
		Self
	}
}

use crate::parse_value;

#[async_trait]
impl InvocationContext for DemoHost {
	async fn sql(
		&self,
		query: String,
		vars: surrealdb_types::Object,
	) -> HostCallResult<surrealdb_types::Value> {
		println!("The module is running a SQL query:");
		println!("SQL: {query}");
		println!("Vars: {}", vars.to_sql());
		println!("Please enter the result:");

		let stdin = std::io::stdin();
		loop {
			let line = match stdin.lock().lines().next() {
				Some(Ok(line)) => line,
				Some(Err(e)) => {
					return Err(HostCallError::Other(format!("failed to read from stdin: {e}")));
				}
				None => {
					return Err(HostCallError::Other("stdin closed unexpectedly".to_string()));
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
		&self,
		fnc: String,
		version: Option<String>,
		args: Vec<surrealdb_types::Value>,
	) -> HostCallResult<surrealdb_types::Value> {
		let version = version.map(|x| format!("<{x}>")).unwrap_or_default();
		println!("The module is running a function:");
		println!(
			" - {fnc}{version}({})",
			args.iter().map(|x| x.to_sql()).collect::<Vec<String>>().join(", ")
		);
		println!("\nPlease enter the result:");

		let stdin = std::io::stdin();
		loop {
			let line = match stdin.lock().lines().next() {
				Some(Ok(line)) => line,
				Some(Err(e)) => {
					return Err(HostCallError::Other(format!("failed to read from stdin: {e}")));
				}
				None => {
					return Err(HostCallError::Other("stdin closed unexpectedly".to_string()));
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

	fn stdout_callback(&self) -> std::sync::Arc<dyn Fn(&str) + Send + Sync> {
		std::sync::Arc::new(|output| println!("[surli::out] {}", output))
	}

	fn stderr_callback(&self) -> std::sync::Arc<dyn Fn(&str) + Send + Sync> {
		std::sync::Arc::new(|output| eprintln!("[surli::err] {}", output))
	}
}
