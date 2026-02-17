use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::Ident;

#[derive(Debug)]
pub struct UnnamedFields {
	pub fields: Vec<syn::Type>,
	pub field_names: Vec<Ident>,
	pub tuple: bool,
}

impl UnnamedFields {
	pub fn new(fields: Vec<syn::Type>, tuple: bool) -> Self {
		let field_names = fields
			.iter()
			.enumerate()
			.map(|(i, _)| syn::Ident::new(&format!("field_{}", i), Span::call_site()))
			.collect();

		Self {
			fields,
			field_names,
			tuple,
		}
	}

	pub fn arr_assignments(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.enumerate()
			.map(|(i, _)| {
				let field_name = &self.field_names[i];
				quote! {
					arr.push(#field_name.into_value());
				}
			})
			.collect()
	}

	pub fn arr_retrievals(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.enumerate()
			.map(|(i, ty)| {
				let ident = Ident::new(&format!("field_{}", i), Span::call_site());
				quote! {
					let #ident = <#ty as SurrealValue>::from_value(arr.remove(0))?;
				}
			})
			.collect()
	}

	/// Generates `let field_N = Default::default();` for each field.
	/// Used as a fallback when content is missing during deserialization with `skip_content_if`.
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

	pub fn field_checks(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.enumerate()
			.map(|(i, ty)| {
				quote! {
					if valid {
						if let Some(v) = arr.get(#i) {
							if !<#ty as SurrealValue>::is_value(v) {
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

	pub fn arr_types(&self) -> Vec<TokenStream2> {
		self.fields
			.iter()
			.map(|ty| {
				quote! { arr.push(<#ty as SurrealValue>::kind_of()); }
			})
			.collect()
	}
}
