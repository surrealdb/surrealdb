//! Declarative, statically-checked key layouts for an ordered key-value store.
//!
//! One [`keyspace!`] invocation declares an entire keyspace. From it this crate
//! generates the key types, their encoders and decoders, the scan-bound types, a
//! reverse decoder that turns raw bytes back into typed keys and values, and a
//! human-readable map of the whole layout. Because every key is declared in one
//! place, the macro can check properties no per-key macro could: that no two keys
//! can claim the same bytes, that no tag can be spelled by a user-controlled
//! field, and that every scan bound has an upper bound at all.
//!
//! # What the schema looks like
//!
//! ```ignore
//! keyspace! {
//!     types {
//!         fixed(4)  NamespaceId, DatabaseId;
//!         fixed(16) Uuid;
//!         var       Str = Cow<'k, str>, Table = Cow<'k, TableName>;
//!     }
//!
//!     root = ["/"] {
//!         namespace = ["!ns", @, ns: Str] => NamespaceDefinition;
//!
//!         ns = ["*", ns: NamespaceId] {
//!             database = ["!db", @, db: Str] => DatabaseDefinition;
//!         }
//!     }
//! }
//! ```
//!
//! Segments are written in encode order. A nested block declares a level, whose
//! segments every key beneath it inherits, so a child key cannot be built without
//! the data that identifies its parents. `@` marks a truncation point and
//! generates the scan bound ending there. Names are derived from the schema
//! idents, so the schema never spells a Rust type name.
//!
//! # What the invocation site must provide
//!
//! The expansion refers to these by bare name, so they must be in scope where the
//! macro is invoked:
//!
//! - `KVKey`, `KVKeyDecode`, `KVRange` — the key traits.
//! - `KVValue` — the value trait.
//! - `Key`, `KeyRange` — the byte and byte-range types.
//! - `KeyError` — the key error type, with `Unencodable` and `Corrupted` variants.
//!
//! Everything else is referenced absolutely (`::storekey`, `::anyhow`, `::std`).
//!
//! # What it generates
//!
//! For every entry: the struct, `storekey::Encode`/`BorrowDecode`, `KVKey` with
//! its value binding, `KVKeyDecode`, and a `new` constructor. For every level: a
//! root type naming the whole subtree, plus a builder method per descendant. For
//! every truncation point: a bound type with `range`, `range_subtree`,
//! `range_where` and `skip_extensions`, all built from `Key::next` and
//! `Key::next_neighbour` so no caller ever appends a sentinel byte by hand.
//!
//! Once per invocation: `AnyKey`, `AnyValue`, `KeyKind`, `PATTERNS`, a byte-trie
//! `AnyKey::decode`, an infallible `describe`, and `KEYSPACE_MAP`.

mod check;
mod codegen;
mod model;
mod parse;
mod reverse;

use proc_macro::TokenStream;
use quote::quote;

/// Declares a keyspace. See the crate documentation for the grammar, the
/// requirements on the invocation site, and what gets generated.
#[proc_macro]
pub fn keyspace(input: TokenStream) -> TokenStream {
	match expand(input.into()) {
		Ok(tokens) => tokens.into(),
		Err(err) => err.into_compile_error().into(),
	}
}

fn expand(input: proc_macro2::TokenStream) -> syn::Result<proc_macro2::TokenStream> {
	let ast = parse::parse_keyspace(input)?;
	let (model, mut errors) = model::resolve(&ast);
	check::check(&model, &mut errors);
	errors.into_result()?;

	let types = codegen::generate(&model);
	let kinds = codegen::key_kind(&model);
	let rev = reverse::generate(&model);
	let map = reverse::keyspace_map(&model);
	let selftest = selftest(&model);

	// The map is attached as documentation as well as exposed as a constant, so
	// hovering the constant or reading the rustdoc shows the live layout. It is
	// generated from the same model as the encoders and therefore cannot drift
	// from them.
	let map_doc = format!("The keyspace layout, rendered for humans.\n\n```text\n{map}\n```");

	Ok(quote! {
		#[doc = #map_doc]
		pub(crate) const KEYSPACE_MAP: &str = #map;

		#types
		#kinds
		#rev
		#selftest
	})
}

/// Tests emitted alongside the keyspace.
///
/// These re-derive at run time what the checker proved at compile time. The
/// redundancy is the point: a bug in the checker would let a bad schema through,
/// and only an independent check over the real encoded bytes would catch it. One
/// of them checks something the checker cannot reach at all — that a declared
/// field width is the width `storekey` really produces.
fn selftest(model: &model::Model) -> proc_macro2::TokenStream {
	let widths = width_selftest(model);
	quote! {
		#[cfg(test)]
		mod keyspace_selftest {
			use super::*;

			/// Every declared key kind appears exactly once in the pattern table.
			///
			/// The table's length depends on which keys are compiled in, so the
			/// check is for duplicates rather than for a fixed count.
			#[test]
			fn patterns_cover_every_kind() {
				assert!(!PATTERNS.is_empty(), "the pattern table is empty");
				let mut kinds: ::std::vec::Vec<KeyKind> =
					PATTERNS.iter().map(|p| p.kind).collect();
				kinds.sort();
				let before = kinds.len();
				kinds.dedup();
				assert_eq!(before, kinds.len(), "a key kind appears twice in PATTERNS");
			}

			/// Every pattern's route is the one its kind reports, so the map, the
			/// table and the enum agree.
			#[test]
			fn routes_agree() {
				for pattern in PATTERNS {
					assert_eq!(
						pattern.route,
						pattern.kind.route(),
						"route disagreement for {:?}",
						pattern.kind
					);
					assert!(
						KEYSPACE_MAP.contains(pattern.route),
						"{} is missing from the keyspace map",
						pattern.route
					);
				}
			}

			/// No tag byte may collide with storekey's terminator or escape byte.
			/// Checked at compile time; re-checked here against the emitted table.
			///
			/// Every tag in the layout is checked, not just the leading one: the
			/// leading run is shared by nearly the whole keyspace, so checking it
			/// alone would leave every inner tag — the ones a schema is most likely
			/// to get wrong — unverified.
			#[test]
			fn tags_avoid_reserved_bytes() {
				let mut checked = 0usize;
				for pattern in PATTERNS {
					for tag in pattern.tags {
						for byte in *tag {
							assert!(
								*byte >= 0x02,
								"{} has reserved tag byte {byte:#04x}",
								pattern.route
							);
							checked += 1;
						}
					}
				}
				assert!(checked > 0, "no tag bytes were checked");
			}

			#widths

			/// Unrecognised bytes describe themselves rather than panicking.
			#[test]
			fn describe_never_panics_on_junk() {
				for bytes in [
					&[][..],
					&[0x00][..],
					&[0xff, 0xff, 0xff][..],
					&b"/!zz-not-a-key"[..],
					&b"/"[..],
				] {
					let _ = describe(bytes).to_string();
				}
			}
		}
	}
}

/// A test pinning every `fixed(N)` field type to the width it was declared with.
///
/// This is the one premise of the disjointness argument that nothing else checks:
/// the checker treats a fixed-width field as exactly N bytes of the layout, and a
/// declaration that disagrees with the codec shifts every symbol after it — enough
/// to report two genuinely overlapping keys as disjoint, and to feed the reverse
/// trie the wrong offsets. The types belong to other crates, so only running their
/// codec can settle it.
///
/// It probes the decoder rather than the encoder because a value of every declared
/// type is not constructible from here. A decoder that accepts N bytes and leaves
/// nothing behind has consumed exactly N; one that accepts N + 1 bytes and leaves
/// nothing behind has consumed too many.
fn width_selftest(model: &model::Model) -> proc_macro2::TokenStream {
	use std::collections::BTreeMap;

	// Keyed on the emitted type, so this checks the types the keys actually use
	// rather than the names the prelude happens to spell. A declaration reached
	// through a same-named type in another module shows up here as its own entry.
	let mut widths: BTreeMap<String, (proc_macro2::TokenStream, usize)> = BTreeMap::new();
	for def in &model.types {
		for field in &def.fields {
			if let model::Width::Fixed(n) = field.width {
				widths.insert(field.ty.to_string(), (field.ty.clone(), n));
			}
		}
	}
	if widths.is_empty() {
		return proc_macro2::TokenStream::new();
	}

	let cases = widths.values().map(|(ty, n)| {
		let label = format!("{}", quote!(#ty)).replace(' ', "");
		quote!(check::<#ty>(#n, #label);)
	});

	quote! {
		/// Every `fixed(N)` field type occupies exactly N bytes under `storekey`.
		#[test]
		fn declared_widths_match_the_codec() {
			/// Whether `bytes` decodes as `T` with nothing left over.
			fn exact<'de, T>(bytes: &'de [u8]) -> bool
			where
				T: ::storekey::BorrowDecode<'de, ()>,
			{
				let mut reader = ::storekey::BorrowReader::new(bytes);
				<T as ::storekey::BorrowDecode<'de, ()>>::borrow_decode(&mut reader).is_ok()
					&& reader.is_empty()
			}

			/// Byte strings worth trying: a fixed-width codec is a big-endian
			/// integer or a raw run, so a small value in the last byte is accepted by
			/// every one of them, and an all-`0xff` run covers a codec that rejects
			/// small values.
			fn probes(width: usize) -> ::std::vec::Vec<::std::vec::Vec<u8>> {
				let mut out = ::std::vec::Vec::new();
				for last in [0u8, 1, 2, 3, 4, 5, 6, 0x7f, 0xff] {
					let mut bytes = ::std::vec![0u8; width];
					if let Some(slot) = bytes.last_mut() {
						*slot = last;
					}
					out.push(bytes);
				}
				out.push(::std::vec![0xffu8; width]);
				out
			}

			fn check<T>(width: usize, label: &str)
			where
				T: for<'de> ::storekey::BorrowDecode<'de, ()>,
			{
				let mut accepted = 0usize;
				for bytes in probes(width) {
					if exact::<T>(&bytes) {
						accepted += 1;
					}
					// One byte longer: consuming all of it means the codec is wider
					// than the declaration, which shifts every later field.
					let mut longer = bytes.clone();
					longer.push(0);
					assert!(
						!exact::<T>(&longer),
						"`{label}` is declared fixed({width}) but consumed {} bytes",
						longer.len()
					);
				}
				// Without this the assertion above is vacuous for a codec that is
				// *narrower* than the declaration: it would leave bytes unread and
				// never be `exact`.
				assert!(
					accepted > 0,
					"`{label}` is declared fixed({width}) but decoded no {width}-byte input, so it \
					 does not occupy {width} bytes"
				);
			}

			#(#cases)*
		}
	}
}

#[cfg(test)]
mod tests;
