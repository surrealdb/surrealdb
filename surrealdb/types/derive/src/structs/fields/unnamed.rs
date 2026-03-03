use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::Ident;

use crate::CratePath;

#[derive(Debug)]
pub struct UnnamedFields {
	pub fields: Vec<syn::Type>,
	pub field_names: Vec<Ident>,
	/// When true, the type will be wrapped in the Wrapper type to provide interop with serde-only
	/// types
	pub wrap: Vec<bool>,
	pub tuple: bool,
	pub skip_content: Option<crate::SkipContent>,
}

impl UnnamedFields {
	pub fn new(
		fields: Vec<syn::Type>,
		wrap: Vec<bool>,
		tuple: bool,
		skip_content: Option<crate::SkipContent>,
	) -> Self {
		let field_names = fields
			.iter()
			.enumerate()
			.map(|(i, _)| syn::Ident::new(&format!("field_{}", i), Span::call_site()))
			.collect();

		Self {
			fields,
			field_names,
			tuple,
			skip_content,
			wrap,
		}
	}

	pub fn arr_assignments(&self, crate_path: &CratePath) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.enumerate()
			.map(|(i, _)| {
				let field_name = &self.field_names[i];
				let potentially_wrapped = if self.wrap[i] {
					let crate_path = crate_path.wrapper();
					quote! {#crate_path(#field_name)}
				} else {
					quote! {#field_name}
				};
				quote! {
					arr.push(#potentially_wrapped.into_value());
				}
			})
			.collect()
	}

	pub fn arr_retrievals(&self, crate_path: &CratePath) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.enumerate()
			.map(|(i, ty)| {
				let ident = Ident::new(&format!("field_{}", i), Span::call_site());
				let (potentially_wrapped, val_access) = if self.wrap[i] {
					let crate_path = crate_path.wrapper();
					(quote! {#crate_path::<#ty>}, quote! {.0})
				} else {
					(quote! {#ty}, quote! {})
				};
				quote! {
					let #ident = <#potentially_wrapped as SurrealValue>::from_value(arr.remove(0))?#val_access;
				}
			})
			.collect()
	}

	/// Generates `let field_N = Default::default();` for each field.
	/// Used as a fallback when content is missing during deserialization with `skip_content`.
	pub fn default_initializers(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.enumerate()
			.map(|(i, ty)| {
				let ident = Ident::new(&format!("field_{}", i), Span::call_site());
				quote! {
					let #ident = <#ty as Default>::default();
				}
			})
			.collect()
	}

	pub fn field_checks(&self, crate_path: &CratePath) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.enumerate()
			.map(|(i, ty)| {
				let potentially_wrapped = if self.wrap[i] {
					let crate_path = crate_path.wrapper();
					quote! {#crate_path::<#ty>}
				} else {
					quote! {#ty}
				};
				quote! {
					if valid {
						if let Some(v) = arr.get(#i) {
							if !<#potentially_wrapped as SurrealValue>::is_value(v) {
								valid = false;
							}
						} else {
							valid = false;
						}
					}
				}
			})
			.collect()
	}

	pub fn arr_types(&self, crate_path: &CratePath) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.enumerate()
			.map(|(i, ty)| {
				let potentially_wrapped = if self.wrap[i] {
					let crate_path = crate_path.wrapper();
					quote! {#crate_path::<#ty>}
				} else {
					quote! {#ty}
				};
				quote! { arr.push(<#potentially_wrapped as SurrealValue>::kind_of()); }
			})
			.collect()
	}
}
