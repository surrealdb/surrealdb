use quote::quote;
use syn::{
	Expr, ExprLit, FnArg, GenericArgument, Lit, Meta, MetaNameValue, Pat, PatType, PathArguments,
	ReturnType, Type, TypePath,
};

use crate::attr::validate_export_name;

/// Extracted components of a function signature used for code generation.
pub(crate) struct FnSignatureParts {
	pub arg_patterns: Vec<syn::Pat>,
	/// Wire names for each argument (derived from pattern or `#[name = "..."]`).
	pub arg_wire_names: Vec<String>,
	pub tuple_type: proc_macro2::TokenStream,
	pub tuple_pattern: proc_macro2::TokenStream,
	pub result_type: proc_macro2::TokenStream,
	pub is_result: bool,
	pub is_async: bool,
	/// The transaction parameter, when the function declared one: its binding
	/// and its written type, verbatim.
	///
	/// Excluded from `arg_patterns`, `arg_wire_names`, `tuple_type` and
	/// `tuple_pattern`, so SurrealQL never sees it as an argument. The mode is
	/// deliberately *not* parsed out of the type — codegen asks the type system
	/// via `TxMode::WRITEABLE` instead, so a type alias works and a bogus mode
	/// is a compile error at the author's signature.
	pub tx: Option<TxParam>,
}

/// The transaction parameter of a `#[surrealism]` function.
pub(crate) struct TxParam {
	pub pattern: syn::Pat,
	pub ty: Box<Type>,
}

/// Whether a type's last path segment is `Transaction`.
///
/// Matched shallowly, consistent with the `Result` detection below, so
/// `Transaction`, `Transaction<ReadWrite>` and `surrealism::Transaction` are all
/// recognised and an alias is not. Two attributes settle the cases the spelling
/// cannot:
///
/// - `#[tx]` on the first parameter forces it to be the handle. That is how an alias for the SDK
///   type is declared, since the spelling no longer says so.
/// - `#[name = "..."]` declares the parameter a SurrealQL argument. That is how a plugin's own
///   first-position type called `Transaction` keeps its meaning.
fn is_transaction_type(ty: &Type) -> bool {
	let Type::Path(TypePath {
		path,
		..
	}) = ty
	else {
		return false;
	};
	path.segments.last().is_some_and(|seg| seg.ident == "Transaction")
}

/// Whether a parameter carries `#[tx]`.
fn has_tx_attr(attrs: &[syn::Attribute]) -> bool {
	attrs.iter().any(|attr| matches!(&attr.meta, Meta::Path(p) if p.is_ident("tx")))
}

/// Derive the wire name from a pattern binding, falling back to positional
/// names like `_0`, `_1` for non-ident patterns.
///
/// `index` counts the SurrealQL arguments, not the Rust parameters, so a
/// transaction handle does not shift the names of everything after it: the same
/// argument is `_0` whether or not the function declared a handle.
fn wire_name_from_pat(pat: &Pat, index: usize) -> String {
	match pat {
		Pat::Ident(ident) => ident.ident.to_string(),
		_ => format!("_{index}"),
	}
}

/// Parse a `#[name = "..."]` attribute on a function parameter.
/// Returns `Some(wire_name)` if present, `None` otherwise.
fn parse_param_name_attr(attrs: &[syn::Attribute]) -> Option<String> {
	for attr in attrs {
		if let Meta::NameValue(MetaNameValue {
			path,
			value,
			..
		}) = &attr.meta
			&& path.is_ident("name")
			&& let Expr::Lit(ExprLit {
				lit: Lit::Str(s),
				..
			}) = value
		{
			return Some(s.value());
		}
	}
	None
}

/// Extract argument patterns, types, wire names, and return type info from a
/// function signature.
///
/// Wire names are derived from the parameter binding name (e.g. `age` from
/// `age: i64`) unless overridden with `#[name = "..."]` on the parameter.
///
/// The `Result` detection is shallow: it only matches the last path segment named
/// `Result` (e.g. `Result<T, E>`, `anyhow::Result<T>`). Aliased or deeply nested
/// Result types are treated as non-Result returns.
pub(crate) fn extract_fn_signature(sig: &syn::Signature) -> syn::Result<FnSignatureParts> {
	let mut arg_patterns = Vec::new();
	let mut arg_wire_names = Vec::new();
	let mut arg_types: Vec<&Box<Type>> = Vec::new();
	let mut tx = None;

	for (index, arg) in sig.inputs.iter().enumerate() {
		match arg {
			FnArg::Typed(PatType {
				pat,
				ty,
				attrs,
				..
			}) => {
				let named = parse_param_name_attr(attrs);
				let forced = has_tx_attr(attrs);
				// Only the first parameter may be the transaction handle. A
				// `Transaction` anywhere else is almost certainly a mistake, and
				// silently treating it as a SurrealQL argument would be worse
				// than saying so. A parameter carrying `#[name = "..."]` is
				// declared an argument by its author, so the spelling of its
				// type is not read.
				if forced || (named.is_none() && is_transaction_type(ty)) {
					if index != 0 {
						return Err(syn::Error::new_spanned(
							ty,
							"the `Transaction` parameter must come first",
						));
					}
					tx = Some(TxParam {
						pattern: *pat.clone(),
						ty: ty.clone(),
					});
					continue;
				}
				let wire_name =
					named.unwrap_or_else(|| wire_name_from_pat(pat, arg_wire_names.len()));
				validate_export_name(&wire_name);
				arg_wire_names.push(wire_name);
				arg_patterns.push(*pat.clone());
				arg_types.push(ty);
			}
			FnArg::Receiver(r) => {
				return Err(syn::Error::new_spanned(
					r,
					"`self` is not supported in #[surrealism] functions",
				));
			}
		}
	}

	let (tuple_type, tuple_pattern) = if arg_types.is_empty() {
		(quote! { () }, quote! { () })
	} else {
		(quote! { ( #(#arg_types),*, ) }, quote! { ( #(#arg_patterns),*, ) })
	};

	let (result_type, is_result) = match &sig.output {
		ReturnType::Default => (quote! { () }, false),
		ReturnType::Type(_, ty) => {
			if let Type::Path(TypePath {
				path,
				..
			}) = &**ty
			{
				if let Some(last_segment) = path.segments.last() {
					if last_segment.ident == "Result" {
						if let PathArguments::AngleBracketed(args) = &last_segment.arguments {
							if let Some(GenericArgument::Type(inner_type)) = args.args.first() {
								(quote! { #inner_type }, true)
							} else {
								(quote! { #ty }, false)
							}
						} else {
							(quote! { #ty }, false)
						}
					} else {
						(quote! { #ty }, false)
					}
				} else {
					(quote! { #ty }, false)
				}
			} else {
				(quote! { #ty }, false)
			}
		}
	};

	Ok(FnSignatureParts {
		arg_patterns,
		arg_wire_names,
		tuple_type,
		tuple_pattern,
		result_type,
		is_result,
		is_async: sig.asyncness.is_some(),
		tx,
	})
}
