use proc_macro::TokenStream;
use quote::quote;
use syn::{Expr, ExprLit, FnArg, Item, ItemFn, ItemMod, Lit, Meta};

use crate::attr::parse_surrealism_attr;
use crate::extract::extract_fn_signature;
use crate::generate::{generate_registration_body, generate_sentinel};

/// Strip `#[name = "..."]` attributes from all function parameters so they
/// don't leak into the emitted function and cause unknown-attribute errors.
fn strip_param_name_attrs(sig: &mut syn::Signature) {
	for arg in &mut sig.inputs {
		if let FnArg::Typed(pat_type) = arg {
			pat_type.attrs.retain(
				|attr| !matches!(&attr.meta, Meta::NameValue(nv) if nv.path.is_ident("name")),
			);
		}
	}
}

/// Collect `#[doc = "..."]` attributes (i.e. `///` doc comments) into a single
/// trimmed string. Returns `None` when no doc comments are present.
fn extract_doc_comment(attrs: &[syn::Attribute]) -> Option<String> {
	let lines: Vec<String> = attrs
		.iter()
		.filter_map(|attr| {
			if !attr.path().is_ident("doc") {
				return None;
			}
			if let Meta::NameValue(nv) = &attr.meta
				&& let Expr::Lit(ExprLit {
					lit: Lit::Str(s),
					..
				}) = &nv.value
			{
				return Some(s.value());
			}
			None
		})
		.collect();
	if lines.is_empty() {
		return None;
	}
	let text = lines
		.iter()
		.map(|l| l.strip_prefix(' ').unwrap_or(l))
		.collect::<Vec<_>>()
		.join("\n")
		.trim()
		.to_string();
	if text.is_empty() {
		None
	} else {
		Some(text)
	}
}

pub(crate) fn handle_function(
	is_default: bool,
	export_name_override: Option<String>,
	is_init: bool,
	is_writeable: bool,
	explicit_comment: Option<String>,
	mut input_fn: ItemFn,
) -> TokenStream {
	let comment = explicit_comment.or_else(|| extract_doc_comment(&input_fn.attrs));

	let parts = match extract_fn_signature(&input_fn.sig) {
		Ok(v) => v,
		Err(e) => return e.to_compile_error().into(),
	};

	strip_param_name_attrs(&mut input_fn.sig);

	let fn_name = &input_fn.sig.ident;
	let fn_vis = &input_fn.vis;
	let fn_sig = &input_fn.sig;
	let fn_block = &input_fn.block;

	let export_name: Option<String> = if is_default {
		None
	} else {
		Some(export_name_override.unwrap_or_else(|| fn_name.to_string()))
	};

	if export_name.as_deref() == Some("default") {
		panic!(
			"`default` is reserved for the default export; use #[surrealism(default)] on the \
			 function that should be the default export instead of naming it \"default\""
		);
	}

	let sentinel = generate_sentinel(export_name.as_deref());
	let registration = generate_registration_body(
		fn_name,
		&parts.arg_patterns,
		&parts.arg_wire_names,
		&parts.tuple_type,
		&parts.tuple_pattern,
		&parts.result_type,
		parts.is_result,
		is_init,
		export_name.as_deref(),
		is_writeable,
		comment.as_deref(),
	);

	let expanded = quote! {
		#fn_vis #fn_sig #fn_block

		#sentinel
		#registration
	};

	TokenStream::from(expanded)
}

pub(crate) fn handle_module(
	is_default: bool,
	export_name_override: Option<String>,
	is_init: bool,
	is_writeable: bool,
	_explicit_comment: Option<String>,
	mut item_mod: ItemMod,
) -> TokenStream {
	if is_default {
		panic!("#[surrealism(default)] cannot be used on a module");
	}
	if is_init {
		panic!("#[surrealism(init)] cannot be used on a module");
	}
	if is_writeable {
		panic!(
			"#[surrealism(writeable)] cannot be used on a module; mark individual functions instead"
		);
	}

	let prefix = export_name_override.unwrap_or_else(|| item_mod.ident.to_string());

	let Some((brace, items)) = item_mod.content.take() else {
		return syn::Error::new_spanned(
			&item_mod.ident,
			"#[surrealism] on mod requires an inline module body (mod foo { ... })",
		)
		.to_compile_error()
		.into();
	};

	let (new_items, sentinels) = process_mod_items(&prefix, items);

	item_mod.content = Some((brace, new_items));

	let expanded = quote! {
		#(#sentinels)*
		#item_mod
	};

	TokenStream::from(expanded)
}

/// Recursively walk items inside a `#[surrealism]` mod, processing annotated
/// functions and nested mods.
fn process_mod_items(prefix: &str, items: Vec<Item>) -> (Vec<Item>, Vec<proc_macro2::TokenStream>) {
	let mut new_items: Vec<Item> = Vec::new();
	let mut sentinels: Vec<proc_macro2::TokenStream> = Vec::new();

	for mut item in items {
		match &mut item {
			Item::Fn(fn_item) => {
				if let Some(idx) =
					fn_item.attrs.iter().position(|a| a.path().is_ident("surrealism"))
				{
					let attr = fn_item.attrs.remove(idx);
					let inner_attrs = match parse_surrealism_attr(&attr) {
						Ok(v) => v,
						Err(e) => {
							new_items.push(Item::Verbatim(e.to_compile_error()));
							continue;
						}
					};

					if inner_attrs.is_init {
						panic!("#[surrealism(init)] cannot be used inside a module");
					}

					let export_name = if inner_attrs.is_default {
						prefix.to_string()
					} else {
						let base = inner_attrs
							.export_name
							.unwrap_or_else(|| fn_item.sig.ident.to_string());
						format!("{prefix}::{base}")
					};

					if export_name == "default" {
						panic!(
							"`default` is reserved for the default export; use \
					 #[surrealism(default)] on the function that should be \
					 the default export instead of naming it \"default\""
						);
					}

					let comment =
						inner_attrs.comment.or_else(|| extract_doc_comment(&fn_item.attrs));

					let parts = match extract_fn_signature(&fn_item.sig) {
						Ok(v) => v,
						Err(e) => {
							new_items.push(Item::Verbatim(e.to_compile_error()));
							continue;
						}
					};

					strip_param_name_attrs(&mut fn_item.sig);
					let fn_name = &fn_item.sig.ident;

					sentinels.push(generate_sentinel(Some(&export_name)));

					let registration = generate_registration_body(
						fn_name,
						&parts.arg_patterns,
						&parts.arg_wire_names,
						&parts.tuple_type,
						&parts.tuple_pattern,
						&parts.result_type,
						parts.is_result,
						false,
						Some(&export_name),
						inner_attrs.is_writeable,
						comment.as_deref(),
					);

					new_items.push(item);
					new_items.push(Item::Verbatim(registration));
					continue;
				}
			}
			Item::Mod(inner_mod) => {
				if let Some(idx) =
					inner_mod.attrs.iter().position(|a| a.path().is_ident("surrealism"))
				{
					let attr = inner_mod.attrs.remove(idx);
					let inner_attrs = match parse_surrealism_attr(&attr) {
						Ok(v) => v,
						Err(e) => {
							new_items.push(Item::Verbatim(e.to_compile_error()));
							continue;
						}
					};

					if inner_attrs.is_default {
						panic!("#[surrealism(default)] cannot be used on a module");
					}
					if inner_attrs.is_init {
						panic!("#[surrealism(init)] cannot be used on a module");
					}
					if inner_attrs.is_writeable {
						panic!(
							"#[surrealism(writeable)] cannot be used on a module; mark individual functions instead"
						);
					}

					let inner_prefix_segment =
						inner_attrs.export_name.unwrap_or_else(|| inner_mod.ident.to_string());
					let inner_prefix = format!("{prefix}::{inner_prefix_segment}");

					let Some((brace, inner_items)) = inner_mod.content.take() else {
						new_items.push(Item::Verbatim(
							syn::Error::new_spanned(
								&inner_mod.ident,
								"#[surrealism] on mod requires an inline module body (mod foo { ... })",
							)
							.to_compile_error(),
						));
						continue;
					};

					let (processed_items, inner_sentinels) =
						process_mod_items(&inner_prefix, inner_items);

					inner_mod.content = Some((brace, processed_items));
					sentinels.extend(inner_sentinels);

					new_items.push(item);
					continue;
				}
			}
			_ => {}
		}
		new_items.push(item);
	}

	(new_items, sentinels)
}
