#![forbid(unsafe_code)]

//! Derive macro for SurrealDB SQL front-end AST nodes.

use proc_macro::TokenStream;

#[proc_macro_derive(Node, attributes(sqlfront, node, dump))]
pub fn derive_node(_input: TokenStream) -> TokenStream {
	TokenStream::new()
}
