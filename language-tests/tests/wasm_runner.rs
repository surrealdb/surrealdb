#![cfg(target_arch = "wasm32")]

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn run_language_tests_indxdb() {
	surrealql_test::cmd::run::run_wasm(surrealql_test::cli::Backend::IndxDb)
		.await
		.expect("language tests failed on indxdb backend");
}
