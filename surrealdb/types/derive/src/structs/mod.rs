mod fields;
pub use fields::*;

mod r#enum;
pub use r#enum::*;

mod with;
pub use with::*;

mod strategy;
pub use strategy::*;

/// Checks whether `ty` syntactically contains an unqualified reference to
/// `ident` (or `Self`). Used by the derive macro to detect direct self-reference
/// in field types so that `kind_of()` can emit `Kind::Any` instead of recursing.
///
/// Single-segment unqualified paths (`MyType`, `Self`) and two-segment
/// `self::TypeName` paths are considered a direct match. Other module-qualified
/// paths like `other::MyType` are **not** treated as self-referential even if
/// the last segment matches, because they refer to a different type in a
/// different module. Generic arguments of all paths are still recursed into so
/// that `Box<MyType>` or `other::Container<MyType>` are correctly detected.
pub fn type_contains_ident(ty: &syn::Type, ident: &syn::Ident) -> bool {
	match ty {
		syn::Type::Path(type_path) => {
			let segments = &type_path.path.segments;
			// A single-segment path like `MyType` or `Self` is an unqualified
			// reference and could be self-referential. `Self` in a struct/enum
			// definition always refers to the type being defined. A two-segment
			// path starting with `self` (e.g. `self::MyType`) also refers to
			// the current module's type. Other multi-segment paths like
			// `other::MyType` point to a different type.
			if segments.len() == 1
				&& type_path.path.leading_colon.is_none()
				&& (segments[0].ident == *ident || segments[0].ident == "Self")
			{
				return true;
			}
			if segments.len() == 2 && segments[0].ident == "self" && segments[1].ident == *ident {
				return true;
			}
			// Recurse into generic arguments of all segments, since
			// `Box<MyType>` or `some_mod::Container<MyType>` should still be
			// detected via their type parameters.
			segments.iter().any(|seg| match &seg.arguments {
				syn::PathArguments::AngleBracketed(args) => args.args.iter().any(|arg| match arg {
					syn::GenericArgument::Type(inner) => type_contains_ident(inner, ident),
					_ => false,
				}),
				syn::PathArguments::Parenthesized(args) => {
					args.inputs.iter().any(|inner| type_contains_ident(inner, ident))
						|| matches!(&args.output, syn::ReturnType::Type(_, ret) if type_contains_ident(ret, ident))
				}
				syn::PathArguments::None => false,
			})
		}
		syn::Type::Reference(r) => type_contains_ident(&r.elem, ident),
		syn::Type::Slice(s) => type_contains_ident(&s.elem, ident),
		syn::Type::Array(a) => type_contains_ident(&a.elem, ident),
		syn::Type::Tuple(t) => t.elems.iter().any(|el| type_contains_ident(el, ident)),
		syn::Type::Paren(p) => type_contains_ident(&p.elem, ident),
		syn::Type::Group(g) => type_contains_ident(&g.elem, ident),
		_ => false,
	}
}
