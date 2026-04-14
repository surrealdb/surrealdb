use syn::punctuated::Punctuated;
use syn::token::Comma;
use syn::{Attribute, Expr, ExprLit, Lit, Meta, MetaNameValue};

/// Validate that an export name uses only valid segment characters separated by `::`.
pub(crate) fn validate_export_name(val: &str) {
	for segment in val.split("::") {
		if segment.is_empty() || !segment.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
			panic!(
				"#[surrealism(name = \"...\")] segments must use only ASCII \
				 letters, digits, and underscores, separated by `::`"
			);
		}
	}
}

/// Parsed result of `#[surrealism(...)]` attribute arguments.
pub(crate) struct SurrealismAttrs {
	pub is_default: bool,
	pub export_name: Option<String>,
	pub is_init: bool,
	pub is_writeable: bool,
	pub comment: Option<String>,
}

pub(crate) fn parse_surrealism_attrs(args: &Punctuated<Meta, Comma>) -> SurrealismAttrs {
	let mut is_default = false;
	let mut export_name: Option<String> = None;
	let mut is_init = false;
	let mut is_writeable = false;
	let mut comment: Option<String> = None;

	for meta in args.iter() {
		match meta {
			Meta::NameValue(MetaNameValue {
				path,
				value,
				..
			}) if path.is_ident("name") => {
				if let Expr::Lit(ExprLit {
					lit: Lit::Str(s),
					..
				}) = value
				{
					let val = s.value();
					validate_export_name(&val);
					export_name = Some(val);
				}
			}
			Meta::NameValue(MetaNameValue {
				path,
				value,
				..
			}) if path.is_ident("comment") => {
				if let Expr::Lit(ExprLit {
					lit: Lit::Str(s),
					..
				}) = value
				{
					comment = Some(s.value());
				}
			}
			Meta::Path(path) if path.is_ident("default") => {
				is_default = true;
			}
			Meta::Path(path) if path.is_ident("init") => {
				is_init = true;
			}
			Meta::Path(path) if path.is_ident("writeable") => {
				is_writeable = true;
			}
			_ => panic!(
				"Unsupported attribute: expected #[surrealism], #[surrealism(default)], \
				 #[surrealism(init)], #[surrealism(writeable)], #[surrealism(comment = \"...\")], \
				 or #[surrealism(name = \"...\")]"
			),
		}
	}

	SurrealismAttrs {
		is_default,
		export_name,
		is_init,
		is_writeable,
		comment,
	}
}

/// Parse surrealism attribute arguments from a `syn::Attribute` (used when
/// stripping inner attributes inside a mod).
///
/// Returns `Ok(...)` on success or a `syn::Error` that the caller should
/// convert to a compile error via `to_compile_error()`.
pub(crate) fn parse_surrealism_attr(attr: &Attribute) -> syn::Result<SurrealismAttrs> {
	match &attr.meta {
		Meta::Path(_) => Ok(SurrealismAttrs {
			is_default: false,
			export_name: None,
			is_init: false,
			is_writeable: false,
			comment: None,
		}),
		Meta::List(list) => {
			let args: Punctuated<Meta, Comma> =
				list.parse_args_with(Punctuated::parse_terminated)?;
			Ok(parse_surrealism_attrs(&args))
		}
		Meta::NameValue(nv) => Err(syn::Error::new_spanned(
			nv,
			"#[surrealism] does not support top-level name = value syntax",
		)),
	}
}
