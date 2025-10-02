// This binary delegates to the library entrypoint so both binary and
// embedded use-cases share the same runtime bootstrap and initialization.
#![allow(deprecated)]
#![deny(clippy::mem_forget)]

use anyhow::Result;
use async_graphql::async_trait;
use std::process::ExitCode;
use std::sync::Arc;
use surreal::ServerComposer;
use surrealdb_core::kvs::{
	DatastoreFlavor, SizedClock, TransactionBuilder, TransactionBuilderFactory,
	TransactionBuilderFactoryRequirements,
};

struct DefaultComposer {}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl TransactionBuilderFactory for DefaultComposer {
	async fn new_transaction_builder(
		path: &str,
		clock: Option<Arc<SizedClock>>,
	) -> Result<(Box<dyn TransactionBuilder>, Arc<SizedClock>)> {
		DatastoreFlavor::new_transaction_builder(path, clock).await
	}

	fn path_valid(v: &str) -> Result<String> {
		DatastoreFlavor::path_valid(v)
	}
}

impl TransactionBuilderFactoryRequirements for DefaultComposer {}

impl surreal::net::RouterFactory for DefaultComposer {}

impl ServerComposer for DefaultComposer {}

fn main() -> ExitCode {
	surreal::init::<DefaultComposer>()
}
