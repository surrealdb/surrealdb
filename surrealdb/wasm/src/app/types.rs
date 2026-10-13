//! The hand-written TypeScript for the options object.
//!
//! [`surrealdb_embedded::Options`] is deserialized with serde and so has no
//! TypeScript of its own; this declares the shape wasm-bindgen should emit and
//! names it in [`SurrealWasmEngine::connect`](super::SurrealWasmEngine::connect)
//! so the generated `.d.ts` types the parameter rather than leaving it `any`.

use wasm_bindgen::prelude::*;

// Both are `export`ed: the consumer of this package is the SDK engine in the
// surrealdb.js repository, which imports `ConnectionOptions` to type its own
// public options. Without the keyword the declarations are file-local and that
// import does not resolve.
#[wasm_bindgen(typescript_custom_section)]
const CONNECTION_OPTIONS: &'static str = r#"
export type CapabilitiesAllowDenyList = {
	allow?: boolean | string[];
	deny?: boolean | string[];
};

export type ConnectionOptions = {
	/** Query timeout in whole seconds. */
	query_timeout?: number;
	/** Transaction timeout in whole seconds. */
	transaction_timeout?: number;
	/**
	 * The namespace and database created when the storage is new.
	 * Defaults to `main`/`main`; pass `false` to create neither.
	 */
	defaults?: boolean | {
		namespace?: string;
		database?: string;
	};
	capabilities?: boolean | {
		scripting?: boolean;
		guest_access?: boolean;
		live_query_notifications?: boolean;
		functions?: boolean | string[] | CapabilitiesAllowDenyList;
		network_targets?: boolean | string[] | CapabilitiesAllowDenyList;
		experimental?: boolean | string[] | CapabilitiesAllowDenyList;
		planner_strategy?: "best-effort" | "compute-only" | "all-read-only";
	};
}
"#;

#[wasm_bindgen]
extern "C" {
	#[wasm_bindgen(typescript_type = "ConnectionOptions")]
	pub type TsConnectionOptions;
}
