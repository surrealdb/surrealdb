//! `#[derive(DynHash)]`: `Hash` through one body over `&mut dyn Hasher`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields};

/// The `Hash` impl and the never-inlined `&mut dyn Hasher` body it forwards to.
pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream> {
	let name = &input.ident;
	let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
	let body = match &input.data {
		Data::Struct(data) => {
			let (pattern, hashes) = destructure(&data.fields, quote!(Self));
			quote! {
				let #pattern = self;
				#(#hashes)*
			}
		}
		Data::Enum(data) => {
			// As `derive(Hash)` does: the discriminant first, when there is more than one variant.
			let discriminant = (data.variants.len() > 1).then(
				|| quote!(::core::hash::Hash::hash(&::core::mem::discriminant(self), state);),
			);
			let arms = data.variants.iter().map(|variant| {
				let ident = &variant.ident;
				let (pattern, hashes) = destructure(&variant.fields, quote!(Self::#ident));
				quote!(#pattern => { #(#hashes)* })
			});
			quote! {
				#discriminant
				match self {
					#(#arms)*
				}
			}
		}
		Data::Union(_) => {
			return Err(syn::Error::new_spanned(name, "DynHash does not derive for a union"));
		}
	};
	Ok(quote! {
		impl #impl_generics ::core::hash::Hash for #name #ty_generics #where_clause {
			#[inline]
			fn hash<__H: ::core::hash::Hasher>(&self, state: &mut __H) {
				let mut state: &mut dyn ::core::hash::Hasher = state;
				self.__dyn_hash(&mut state)
			}
		}

		impl #impl_generics #name #ty_generics #where_clause {
			#[doc(hidden)]
			#[inline(never)]
			fn __dyn_hash(&self, state: &mut &mut dyn ::core::hash::Hasher) {
				#body
			}
		}
	})
}

/// The pattern that binds every field of `fields` under `path`, and the statements hashing each.
fn destructure(fields: &Fields, path: TokenStream) -> (TokenStream, Vec<TokenStream>) {
	match fields {
		Fields::Named(named) => {
			let idents: Vec<_> = named.named.iter().filter_map(|f| f.ident.clone()).collect();
			let bound: Vec<_> = idents.iter().map(|i| format_ident!("__field_{}", i)).collect();
			let hashes =
				bound.iter().map(|b| quote!(::core::hash::Hash::hash(#b, state);)).collect();
			(quote!(#path { #(#idents: #bound),* }), hashes)
		}
		Fields::Unnamed(unnamed) => {
			let idents: Vec<_> =
				(0..unnamed.unnamed.len()).map(|i| format_ident!("__field{}", i)).collect();
			let hashes =
				idents.iter().map(|i| quote!(::core::hash::Hash::hash(#i, state);)).collect();
			(quote!(#path(#(#idents),*)), hashes)
		}
		Fields::Unit => (path, Vec::new()),
	}
}
