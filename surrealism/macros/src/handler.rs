use proc_macro::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Expr, ExprLit, FnArg, Item, ItemFn, ItemMod, Lit, Meta};

use crate::attr::parse_surrealism_attr;
use crate::extract::extract_fn_signature;
use crate::generate::{generate_registration_body, generate_sentinel};

/// Strip `#[name = "..."]` attributes from all function parameters so they
/// don't leak into the emitted function and cause unknown-attribute errors.
fn strip_param_name_attrs(sig: &mut syn::Signature) {
	for arg in &mut sig.inputs {
		if let FnArg::Typed(pat_type) = arg {
			// Both are read by `extract_fn_signature` and are not real
			// attributes, so they must not reach the emitted function.
			pat_type.attrs.retain(|attr| match &attr.meta {
				Meta::NameValue(nv) => !nv.path.is_ident("name"),
				Meta::Path(p) => !p.is_ident("tx"),
				Meta::List(_) => true,
			});
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
	explicit_comment: Option<String>,
	mut input_fn: ItemFn,
) -> TokenStream {
	let comment = explicit_comment.or_else(|| extract_doc_comment(&input_fn.attrs));

	let parts = match extract_fn_signature(&input_fn.sig) {
		Ok(v) => v,
		Err(e) => return e.to_compile_error().into(),
	};

	// `init` is sync-lifted by the WIT, and a sync-lifted export cannot block:
	// the canonical ABI has no `task.return` for it, so it must run to
	// completion. An `async fn` here would need a nested executor and could
	// never make progress on a host call, so reject it at the signature.
	if is_init && parts.is_async {
		return syn::Error::new_spanned(
			&input_fn.sig,
			"#[surrealism(init)] cannot be `async`: `init` is lent no transaction and runs \
			 synchronously. Move any async work into a regular #[surrealism] function.",
		)
		.to_compile_error()
		.into();
	}

	// `init` receives no handle, so a transaction parameter can never be filled.
	if is_init && parts.tx.is_some() {
		return syn::Error::new_spanned(
			&input_fn.sig,
			"#[surrealism(init)] cannot take a `Transaction`: `surreal ext build` runs it \
			 once and ships the memory it leaves, so no datastore exists while it runs.",
		)
		.to_compile_error()
		.into();
	}

	strip_param_name_attrs(&mut input_fn.sig);

	let fn_name = input_fn.sig.ident.clone();
	let fn_name = &fn_name;

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

	// Nothing to gate on this path. A `#[cfg]` on a *free* function is resolved
	// before an attribute macro on it runs — false and the macro is never
	// invoked, true and the attribute is inert — and a `#[cfg_attr]` is
	// expanded by then too. So whatever conditional the author wrote has
	// already been decided, and there is nothing left for the generated items
	// to repeat. The `#[surrealism] mod` walker is where that is not true, and
	// where the gates are collected: see `process_mod_items`.
	let gates: &[&syn::Attribute] = &[];

	let sentinel = generate_sentinel(export_name.as_deref(), gates);
	let registration = generate_registration_body(
		fn_name,
		&parts.arg_patterns,
		&parts.arg_wire_names,
		&parts.tuple_type,
		&parts.tuple_pattern,
		&parts.result_type,
		parts.is_result,
		parts.is_async,
		is_init,
		export_name.as_deref(),
		parts.tx.as_ref(),
		comment.as_deref(),
		gates,
	);

	// Emit the whole item rather than vis/sig/block, so the author's attributes
	// reach the emitted function — `#[tokio::main]` above all, which is what
	// enters the runtime a tokio-shaped export needs.
	let expanded = quote! {
		#input_fn

		#sentinel
		#registration
	};

	TokenStream::from(expanded)
}

/// Whether an attribute decides that the item *exists*, rather than decorating it.
///
/// `cfg` does. `cfg_attr` usually does not: it applies an attribute, which is
/// the function's own business, so it counts only when every attribute it
/// applies is itself a `cfg`. Copying the general form would stamp the author's
/// `#[cfg_attr(feature = "x", inline)]` onto a `const` and onto an
/// `inventory::submit!` invocation — a compile error they never wrote, in a
/// place they cannot trace it back from.
///
/// Asked only of items inside a `#[surrealism] mod`, which is where these
/// attributes still arrive unexpanded.
fn is_existence_gate(attr: &syn::Attribute) -> bool {
	if attr.path().is_ident("cfg") {
		return true;
	}
	if !attr.path().is_ident("cfg_attr") {
		return false;
	}
	// `#[cfg_attr(predicate, attr, ...)]`: the predicate first, then whatever it
	// applies. Unparseable means unrecognised, which means not copied.
	attr.parse_args_with(Punctuated::<Meta, syn::Token![,]>::parse_terminated)
		.is_ok_and(|args| args.iter().skip(1).all(|m| m.path().is_ident("cfg")))
}

pub(crate) fn handle_module(
	is_default: bool,
	export_name_override: Option<String>,
	is_init: bool,
	_explicit_comment: Option<String>,
	mut item_mod: ItemMod,
) -> TokenStream {
	if is_default {
		panic!("#[surrealism(default)] cannot be used on a module");
	}
	if is_init {
		panic!("#[surrealism(init)] cannot be used on a module");
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

					// A conditional-compilation gate on the function has to
					// reach everything that names it. This mod's own attribute
					// macro receives the items with their `#[cfg]` still on
					// them, so without repeating it the sentinel and the
					// registration would name a function the later cfg-strip
					// removes.
					let gates: Vec<&syn::Attribute> =
						fn_item.attrs.iter().filter(|a| is_existence_gate(a)).collect();

					sentinels.push(generate_sentinel(Some(&export_name), &gates));

					let registration = generate_registration_body(
						fn_name,
						&parts.arg_patterns,
						&parts.arg_wire_names,
						&parts.tuple_type,
						&parts.tuple_pattern,
						&parts.result_type,
						parts.is_result,
						parts.is_async,
						false,
						Some(&export_name),
						parts.tx.as_ref(),
						comment.as_deref(),
						&gates,
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
