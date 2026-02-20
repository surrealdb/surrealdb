mod r#enum;
pub use r#enum::*;

mod unit;
pub use unit::*;

mod field;
pub use field::*;

mod unnamed_fields;
pub use unnamed_fields::*;

mod named_fields;
pub use named_fields::*;

/// Controls whether the content field can be absent for a variant.
#[derive(Clone, Debug)]
pub enum SkipContent {
	/// Always skip the content field (`#[surreal(skip_content)]`).
	Always,
	/// Conditionally skip based on a predicate (`skip_content_if = "fn"`).
	If(syn::Path),
}
