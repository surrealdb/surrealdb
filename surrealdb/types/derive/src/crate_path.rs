use proc_macro2::TokenStream;
use quote::quote;
use syn::{Attribute, LitStr, Path};

/// Configuration for the crate path used in generated code.
///
/// This allows users to customize which crate path the derive macro uses,
/// which is useful when re-exporting the types from another crate.
#[derive(Debug, Clone)]
pub struct CratePath {
	path: TokenStream,
}

impl CratePath {
	/// Parse the crate path from attributes.
	///
	/// Looks for `#[surreal(crate = "path::to::crate")]` attribute.
	/// Defaults to `::surrealdb_types` if not specified.
	pub fn parse(attrs: &[Attribute]) -> Self {
		for attr in attrs {
			if attr.path().is_ident("surreal") {
				let mut crate_path = None;

				let _ = attr.parse_nested_meta(|meta| {
					if meta.path.is_ident("crate")
						&& let Ok(value) = meta.value()
						&& let Ok(lit_str) = value.parse::<LitStr>()
						&& let Ok(path) = syn::parse_str::<Path>(&lit_str.value())
					{
						crate_path = Some(quote! { #path });
					}
					Ok(())
				});

				if let Some(path) = crate_path {
					return Self {
						path,
					};
				}
			}
		}

		// Default to ::surrealdb_types
		Self::default()
	}

	/// Get the token stream for Value type
	pub fn value(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::Value }
	}

	/// Get the token stream for Kind type
	pub fn kind(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::Kind }
	}

	/// Get the token stream for KindLiteral type
	pub fn kind_literal(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::KindLiteral }
	}

	/// Get the token stream for Object type
	pub fn object(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::Object }
	}

	/// Get the token stream for Array type
	pub fn array(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::Array }
	}

	/// Get the token stream for ConversionError type
	pub fn conversion_error(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::ConversionError }
	}

	/// Get the token stream for TypeError type
	pub fn type_error(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::TypeError }
	}

	/// Get the token stream for anyhow::Result
	pub fn anyhow_result(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::anyhow::Result }
	}

	/// Get the token stream for anyhow::anyhow!
	pub fn anyhow_macro(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::anyhow::anyhow }
	}

	/// Get the token stream for Value::from_t function
	pub fn value_from_t(&self) -> TokenStream {
		let base = &self.path;
		quote! { #base::Value::from_t }
	}
}

impl Default for CratePath {
	fn default() -> Self {
		Self {
			path: quote! { ::surrealdb_types },
		}
	}
}
