//! `#[derive(Stored)]`: one attribute table per catalog type that generates its encoder, its
//! decoder, its release floor, its VM value conversions and its schema description.
//!
//! A stored type is a keyed map from field number to value (see the stored-value grammar in
//! `surrealdb_expr::val::stored`). The number carries the class: an even number is critical, so a
//! reader that does not know it refuses the entry; an odd number is ignorable, so a reader that
//! does not know it skips the field and a rewrite keeps its bytes. The attribute table is the
//! contract; the derive refuses, at compile time, a table that would break it.
//!
//! # Type attributes: `#[stored(…)]`
//!
//! | attribute | meaning |
//! |---|---|
//! | `since = "4.0"` | **required.** The release the type first shipped in; the default `since` of its fields and variants. Never below 4.0. |
//! | `reserved = [n, …]` | Retired numbers: never reused, skipped on read. On an enum they are variant numbers. |
//! | `legacy = "path"` | An enum's `fn(&Self) -> Self` that maps every legacy variant to a current one. |
//! | `revision_embedded` | The type is also embedded in a `revision` container. Recorded in the schema. |
//! | `floor_fixed` | Every entry's floor is the first release, forever: no critical field or variant is added after the type's `since`. |
//! | `transparent` | A single-field tuple struct, encoded as its field. |
//! | `validate` | The type's invariant check, `fn(&Self) -> Result<(), SchemaError>`, named `Self::validate`. Every constructor the derive emits (decode and VM conversion) runs it on the value it built before returning it, so an invalid value is refused wherever it is decoded. `validate = "path"` names another function. |
//!
//! # Field attributes
//!
//! | attribute | meaning |
//! |---|---|
//! | `n = N` | **required.** The field number: even is critical, odd is ignorable. |
//! | `since = "M.m"` | The release the field arrived in; defaults to the type's. |
//! | `ignorable` | Requires an odd number, a field spelled `Option<…>` or `Unrevisioned<Option<…>>`, and `independent`. |
//! | `independent = "reason"` | Why an earlier release loses nothing by skipping the field. |
//! | `default = "path"` | Absence means `path()`; the field is written only when it differs. Not for `Option` fields. |
//! | `supersedes = M, narrow = "path"` | The field replaces number `M`, which holds a narrower type. `narrow` is `fn(&Field) -> Option<Old>`: the encoder writes `M` whenever it gives `Some` and `N` otherwise, and the decoder accepts either, refusing both. A superseding field is critical, and required or `default`. `M` takes the type's `since`. |
//!
//! A field spelled `Unrevisioned<T>` is one the `revision` encoding leaves out; its absence means
//! `T::default()`.
//!
//! # Variant attributes
//!
//! `n = N` (required unless `legacy`), `since = "M.m"`, `legacy` (never written; needs the type's
//! `legacy` hook) and, on a struct variant, `reserved = [n, …]` for the numbers retired from its
//! fields.
//!
//! # Refused at compile time
//!
//! A field or variant without a number; a number used twice, reserved, zero or above `u32::MAX`;
//! parity that contradicts the class; `ignorable` without an `Option` or without `independent`;
//! `default` on an `Option`; a malformed `since`, or one below 4.0; a field added after its type
//! that is required (an entry written before it existed must still read); a critical field or a
//! variant added after a `floor_fixed` type's first release; a legacy variant without the hook;
//! `supersedes` without `narrow`.
//!
//! # What is generated
//!
//! `impl StoredField` and `impl StoredType` for the type, both under
//! `::surrealdb_expr::val::stored::schema`, so a crate that derives `Stored` depends on
//! `surrealdb-expr` and nothing else.

mod codegen;
mod dyn_hash;
mod model;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Derives `StoredField` and `StoredType`. See the crate documentation for the attributes.
#[proc_macro_derive(Stored, attributes(stored))]
pub fn derive_stored(input: TokenStream) -> TokenStream {
	let input = parse_macro_input!(input as DeriveInput);
	match model::parse(&input) {
		Ok(model) => codegen::expand(&model).into(),
		Err(error) => error.to_compile_error().into(),
	}
}

/// Derives `Hash` as `derive(Hash)` does, feeding the hasher the same values in the same order,
/// through one body that takes `&mut dyn Hasher`. `derive(Hash)` generates a type's hashing code
/// once per hasher type; a large type hashed by several hashers carries that code once instead.
/// Each call goes through the hasher's vtable, so it suits a type hashed for caching and
/// planning, not one hashed per row.
#[proc_macro_derive(DynHash)]
pub fn derive_dyn_hash(input: TokenStream) -> TokenStream {
	let input = parse_macro_input!(input as DeriveInput);
	match dyn_hash::expand(&input) {
		Ok(tokens) => tokens.into(),
		Err(error) => error.to_compile_error().into(),
	}
}
