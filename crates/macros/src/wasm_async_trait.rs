use proc_macro::TokenStream;
use quote::quote;
use syn::{Error, ItemImpl, ItemTrait};

pub(crate) fn handle_trait(trait_item: ItemTrait) -> TokenStream {
	// Validate that this is actually a trait with async methods
	let has_async_methods = trait_item.items.iter().any(|item| {
		if let syn::TraitItem::Fn(method) = item {
			method.sig.asyncness.is_some()
		} else {
			false
		}
	});

	if !has_async_methods {
		return Error::new_spanned(
			&trait_item,
			"wasm_async_trait should only be used on traits with async methods",
		)
		.to_compile_error()
		.into();
	}

	let expanded = quote! {
		#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
		#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
		#trait_item
	};

	TokenStream::from(expanded)
}

pub(crate) fn handle_impl(impl_item: ItemImpl) -> TokenStream {
	// Check if this is a trait implementation (has a trait path)
	if impl_item.trait_.is_none() {
		return Error::new_spanned(
			&impl_item,
			"wasm_async_trait on impl blocks is only supported for trait implementations",
		)
		.to_compile_error()
		.into();
	}

	// Validate that this impl has async methods
	let has_async_methods = impl_item.items.iter().any(|item| {
		if let syn::ImplItem::Fn(method) = item {
			method.sig.asyncness.is_some()
		} else {
			false
		}
	});

	if !has_async_methods {
		return Error::new_spanned(
			&impl_item,
			"wasm_async_trait should only be used on implementations with async methods",
		)
		.to_compile_error()
		.into();
	}

	let expanded = quote! {
		#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
		#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
		#impl_item
	};

	TokenStream::from(expanded)
}
