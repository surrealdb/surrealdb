//! Checker tests.
//!
//! Each rule is exercised by resolving a small schema and asserting on the
//! rendered diagnostics. Testing the checker directly rather than through
//! compile-failure fixtures keeps the assertions about *our* messages, needs no
//! extra dependency, and runs at unit-test speed.

use proc_macro2::TokenStream;
use quote::quote;

use crate::check::{Divergence, Side, diverge};
use crate::model::{Sym, Width};
use crate::{check, model, parse, reverse};

/// Resolves and checks a schema, returning every diagnostic it produced.
fn diagnostics(src: TokenStream) -> Vec<String> {
	let ast = parse::parse_keyspace(src).expect("schema should parse");
	let (resolved, mut errors) = model::resolve(&ast);
	check::check(&resolved, &mut errors);
	errors.messages()
}

fn resolved(src: TokenStream) -> model::Model {
	let ast = parse::parse_keyspace(src).expect("schema should parse");
	let (resolved, mut errors) = model::resolve(&ast);
	check::check(&resolved, &mut errors);
	assert!(errors.is_empty(), "expected a clean schema, got {:?}", errors.messages());
	resolved
}

/// Asserts that some diagnostic mentions `needle`.
fn assert_reports(diags: &[String], needle: &str) {
	assert!(
		diags.iter().any(|d| d.contains(needle)),
		"no diagnostic mentioned {needle:?}; got {diags:?}"
	);
}

fn prelude() -> TokenStream {
	quote! {
		types {
			fixed(4)  NamespaceId, DatabaseId, IndexId;
			fixed(8)  DocId;
			fixed(16) Uuid;
			var       Str = Cow<'k, str>, Table = Cow<'k, Table>;
			var       Value;
		}
	}
}

// ---------------------------------------------------------------------------
// The divergence walk, which every layout rule is built on
// ---------------------------------------------------------------------------

fn lit(bytes: &[u8]) -> Sym {
	Sym::Lit(bytes.to_vec())
}

fn field(name: &str, width: Width) -> Sym {
	Sym::Field {
		name: name.to_owned(),
		width,
	}
}

#[test]
fn differing_tags_are_disjoint() {
	let a = [lit(b"/!ns"), field("ns", Width::Var)];
	let b = [lit(b"/!db"), field("db", Width::Var)];
	assert_eq!(diverge(&a, &b), Divergence::Disjoint);
}

#[test]
fn a_shorter_tag_that_meets_a_field_is_ambiguous() {
	// `!i` is a byte prefix of `!ix`, so after the shared bytes one layout is
	// still reading its tag while the other is already reading a field.
	let a = [lit(b"/!i"), field("x", Width::Var)];
	let b = [lit(b"/!ix"), field("y", Width::Var)];
	assert!(matches!(diverge(&a, &b), Divergence::LiteralMeetsField { .. }));
}

#[test]
fn identical_layouts_are_detected() {
	let a = [lit(b"/!ns"), field("ns", Width::Var)];
	assert_eq!(diverge(&a, &a.clone()), Divergence::Identical);
}

#[test]
fn a_strict_prefix_is_an_extension_and_names_the_shorter_side() {
	let short = [lit(b"/~"), field("id", Width::Var)];
	let long = [lit(b"/~"), field("id", Width::Var), field("tail", Width::Fixed(8))];
	assert_eq!(
		diverge(&short, &long),
		Divergence::Extension {
			shorter: Side::Left
		}
	);
	assert_eq!(
		diverge(&long, &short),
		Divergence::Extension {
			shorter: Side::Right
		}
	);
}

#[test]
fn a_split_literal_run_still_compares_by_bytes() {
	// The schema may spell one tag as several segments. The model merges adjacent
	// literals, so what the checker compares is one run either way — and the
	// comparison has to agree with the joined spelling, because `diverge` walks
	// bytes inside a run rather than whole runs.
	let p = prelude();
	let model = resolved(quote! {
		#p
		root = ["/"] {
			split = ["!n", "sy", f: Str] => Rec;
		}
	});
	let split = model.types.iter().find(|t| t.name == "SplitKey").expect("split key");
	assert_eq!(
		split.syms,
		[lit(b"/!nsy"), field("f", Width::Var)],
		"adjacent literals must merge into one run"
	);

	// A joined tag that shares only a byte prefix with it is still disjoint, and
	// the answer must not depend on how either side was spelled.
	assert_eq!(
		diverge(&split.syms, &[lit(b"/!nsx"), field("f", Width::Var)]),
		Divergence::Disjoint
	);
	assert_eq!(diverge(&[lit(b"/!nsx")], &split.syms), Divergence::Disjoint);
}

#[test]
fn different_fields_at_the_same_position_are_ambiguous() {
	let a = [lit(b"/"), field("ns", Width::Fixed(4))];
	let b = [lit(b"/"), field("db", Width::Fixed(4))];
	assert!(matches!(diverge(&a, &b), Divergence::FieldMeetsField { .. }));
}

// ---------------------------------------------------------------------------
// R1: uniqueness
// ---------------------------------------------------------------------------

#[test]
fn r1_rejects_a_duplicate_entry_ident() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			namespace = ["!ns", ns: Str] => NsDef;
			namespace = ["!nx", ns: Str] => NsDef;
		}
	});
	assert_reports(&diags, "declared twice");
}

#[test]
fn r1_rejects_two_idents_that_derive_the_same_type_name() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			my_key = ["!aa", a: Str] => V;
			my__key = ["!ab", a: Str] => V;
		}
	});
	assert_reports(&diags, "derive the same");
}

#[test]
fn r1_rejects_two_keys_with_identical_layouts() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			first = ["!aa", a: Str] => V;
			second = ["!aa", a: Str] => V;
		}
	});
	assert_reports(&diags, "encode to the same bytes");
}

// ---------------------------------------------------------------------------
// R2 / R3: tag shadowing and tags that collide with field values
// ---------------------------------------------------------------------------

#[test]
fn r2_rejects_a_tag_that_shadows_a_longer_one() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			short = ["!i", a: Str] => V;
			long = ["!ix", b: Str] => V;
		}
	});
	assert_reports(&diags, "overlap");
}

/// The real collision this whole design exists to catch: a subspace tag sitting
/// where a sibling has a user-controlled name field, so a name beginning with
/// those bytes lands inside the wrong subspace.
#[test]
fn r3_rejects_a_tag_that_a_name_field_can_spell() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			sequence = ["*sq", sq: Str] => SeqDef;
			table = ["*", tb: Table, "!x", f: Str] => TbDef;
		}
	});
	assert_reports(&diags, "sits where the other key has a field");
}

#[test]
fn r3_accepts_the_collision_when_it_is_waived() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			#[waive(overlaps = table, at = "sq", tracked = "known collision")]
			sequence = ["*sq", sq: Str] => SeqDef;
			table = ["*", tb: Table, "!x", f: Str] => TbDef;
		}
	});
	assert!(diags.is_empty(), "waived collision should be silent, got {diags:?}");
}

/// A waiver naming a level covers every key beneath it, because a tag that
/// collides with a level's name field collides with all of that level's contents.
#[test]
fn r3_waiver_on_a_level_covers_its_children() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			#[waive(overlaps = tbl, at = "sq", tracked = "known collision")]
			sequence = ["*sq", sq: Str] => SeqDef;

			tbl = ["*", tb: Table] {
				record = ["*", id: Str] => Rec;
				event = ["!ev", ev: Str] => Ev;
			}
		}
	});
	assert!(diags.is_empty(), "level waiver should cover children, got {diags:?}");
}

// ---------------------------------------------------------------------------
// R4: reserved tag bytes
// ---------------------------------------------------------------------------

#[test]
fn r4_rejects_a_tag_byte_that_collides_with_the_terminator() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			bad = ["!aa", raw 0x00, a: Str] => V;
		}
	});
	assert_reports(&diags, "must be `>= 0x02`");
}

#[test]
fn r4_accepts_the_legacy_magic_bytes() {
	// The retired `Option` discriminants are 0x02 and 0x03, which are legal.
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			some_arm = ["!ix", raw 0x03, id: Str] => V;
			none_arm = ["!ix", raw 0x02] => V;
		}
	});
	assert!(diags.is_empty(), "0x02/0x03 tags are legal, got {diags:?}");
}

// ---------------------------------------------------------------------------
// R5: field types
// ---------------------------------------------------------------------------

#[test]
fn r5_rejects_an_undeclared_field_type() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			bad = ["!aa", a: SomethingUndeclared] => V;
		}
	});
	assert_reports(&diags, "is not declared in the `types` prelude");
}

#[test]
fn r5_rejects_float_fields() {
	let diags = diagnostics(quote! {
		types { fixed(8) f64; }
		root = ["/"] {
			bad = ["!aa", a: f64] => V;
		}
	});
	assert_reports(&diags, "floating-point fields are not allowed");
}

/// An alias is how field types are normally written, so the check has to see
/// through one. Checking the written name alone would let this schema compile with
/// a float in a key.
#[test]
fn r5_rejects_a_float_behind_an_alias() {
	let diags = diagnostics(quote! {
		types { fixed(8) Score = f64; }
		root = ["/"] {
			bad = ["!aa", a: Score] => V;
		}
	});
	assert_reports(&diags, "floating-point fields are not allowed");
}

#[test]
fn r5_rejects_a_float_inside_a_composite_alias() {
	let diags = diagnostics(quote! {
		types { fixed(16) Pair = (f32, f32); }
		root = ["/"] {
			bad = ["!aa", a: Pair] => V;
		}
	});
	assert_reports(&diags, "floating-point fields are not allowed");
}

// ---------------------------------------------------------------------------
// R6: a bound shares its entry's format
// ---------------------------------------------------------------------------

#[test]
fn r6_prefix_inherits_the_entry_format() {
	let p = prelude();
	let model = resolved(quote! {
		#p
		root = ["/"] (format_generic) {
			#[format(IndexFormat)]
			entry = ["!ix", @, fd: Value] => V;
		}
	});
	let entry = model.types.iter().find(|t| t.name == "EntryKey").expect("entry");
	let prefix = model.types.iter().find(|t| t.name == "EntryPrefix").expect("prefix");
	assert_eq!(entry.format.describe(), "IndexFormat");
	assert_eq!(prefix.format.describe(), entry.format.describe());
}

#[test]
fn r6_rejects_pinning_a_format_and_asking_for_a_generic_one() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			#[format(IndexFormat)]
			entry = ["!ix", fd: Value] => V (format_generic);
		}
	});
	assert_reports(&diags, "mutually exclusive");
}

// ---------------------------------------------------------------------------
// R7: extensions must be declared
// ---------------------------------------------------------------------------

#[test]
fn r7_rejects_an_undeclared_prefix_relationship() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			base = ["~", id: Str] => V;
			longer = ["~", id: Str, extra: DocId] => V;
		}
	});
	assert_reports(&diags, "strict prefix");
}

#[test]
fn r7_accepts_a_declared_extension() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			base = ["~", id: Str] => V;
			longer = base + [extra: DocId] => V (ignore_trailing);
		}
	});
	assert!(diags.is_empty(), "declared extension should be clean, got {diags:?}");
}

#[test]
fn r7_rejects_extending_an_entry_that_does_not_exist() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			longer = nonexistent + [extra: DocId] => V;
		}
	});
	assert_reports(&diags, "cannot be extended");
}

// ---------------------------------------------------------------------------
// R8: every bound has an upper bound
// ---------------------------------------------------------------------------

#[test]
fn r8_rejects_a_bound_that_starts_with_a_field() {
	let diags = diagnostics(quote! {
		types { fixed(4) NamespaceId; }
		bare = [ns: NamespaceId] => V (also_range);
	});
	assert_reports(&diags, "must start with a literal tag");
}

#[test]
fn r8_rejects_a_bound_starting_with_the_maximum_byte() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		top = [b"\xff!x", a: Str] => V (also_range);
	});
	assert_reports(&diags, "has no successor");
}

/// A key that another key extends gets `skip_extensions` and `range_subtree`,
/// which are built from the same successor arithmetic as a declared bound, so it
/// needs the same guarantee even though it never says `also_range`.
#[test]
fn r8_covers_a_key_that_only_gets_bounds_from_being_extended() {
	let diags = diagnostics(quote! {
		types { fixed(8) DocId; }
		top = [b"\xff\xff"] => V;
		sub = top + [extra: DocId] => V;
	});
	assert_reports(&diags, "has no successor");
}

// ---------------------------------------------------------------------------
// R9: waivers cannot outlive their collision
// ---------------------------------------------------------------------------

#[test]
fn r9_rejects_a_waiver_that_matches_nothing() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			#[waive(overlaps = other, at = "aa", tracked = "stale")]
			one = ["!aa", a: Str] => V;
			other = ["!bb", b: Str] => V;
		}
	});
	assert_reports(&diags, "there is no such overlap; remove the waiver");
}

// ---------------------------------------------------------------------------
// R10: conditional compilation does not hide a conflict
// ---------------------------------------------------------------------------

#[test]
fn r10_compares_conditionally_compiled_keys_against_the_rest() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			always = ["!aa", a: Str] => V;
			#[cfg(feature = "extra")]
			sometimes = ["!aa", a: Str] => V;
		}
	});
	assert_reports(&diags, "encode to the same bytes");
}

/// A waiver reaches every key beneath the level it names, but only at the
/// position it names. A different collision between the same two declarations is
/// a different fact about the layout and has to be reported on its own.
#[test]
fn r9_waiver_covers_only_the_collision_it_documents() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			#[waive(overlaps = names, at = "sq", tracked = "only the sq collision")]
			band = ["*"] {
				sq_state = ["sq", a: Str] => V;
				qq_state = ["qq", b: Str] => V;
			}

			names = ["*", tb: Table, "!x", z: Str] => V;
		}
	});

	assert_reports(&diags, "`qq_state` and `names` overlap");
	assert!(
		!diags.iter().any(|d| d.contains("`sq_state` and `names` overlap")),
		"the documented collision should stay silent, got {diags:?}"
	);
}

// ---------------------------------------------------------------------------
// R11: key-derived value context
// ---------------------------------------------------------------------------

#[test]
fn r11_rejects_a_context_on_a_type_that_stores_nothing() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			level = ["*", ns: NamespaceId] (ctx = |k| k.ns) {
				child = ["!aa", a: Str] => V;
			}
		}
	});
	assert_reports(&diags, "stores no value");
}

// ---------------------------------------------------------------------------
// R12: enum discriminants
// ---------------------------------------------------------------------------

/// Two variants sharing a discriminant is the same failure the layout rules exist
/// to prevent, one level down: two logically distinct keys with the same bytes.
/// `rustc` only warns about the unreachable decode arm.
#[test]
fn r12_rejects_two_variants_that_encode_the_same() {
	let diags = diagnostics(quote! {
		types { var Str = Cow<'k, str>; }
		enums { Dir: u8 { In = 2, Out = 2, Both = 4 } }
		root = ["/"] {
			graph = ["~", d: Dir, x: Str] => V;
		}
	});
	assert_reports(&diags, "both encode as `2`");
}

// ---------------------------------------------------------------------------
// R13: a level root must bound the formats beneath it
// ---------------------------------------------------------------------------

#[test]
fn r13_rejects_a_level_whose_subtree_pins_another_format() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			idx = ["+", ix: IndexId] {
				#[format(IndexFormat)]
				entry = ["*", @, fd: Value] => V;
			}
		}
	});
	assert_reports(&diags, "declare `idx` as `(format_generic)`");
}

#[test]
fn r13_accepts_a_format_generic_level_over_a_pinned_key() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] (format_generic) {
			idx = ["+", ix: IndexId] (format_generic) {
				#[format(IndexFormat)]
				entry = ["*", @, fd: Value] => V;
			}
		}
	});
	assert!(diags.is_empty(), "a format-generic level should be clean, got {diags:?}");
}

// ---------------------------------------------------------------------------
// R15: value types name distinct decoder variants
// ---------------------------------------------------------------------------

/// Two value types with the same leaf name would mint two `AnyValue` variants
/// called the same thing. That is an `E0428` on generated tokens, which points at
/// the invocation rather than at either entry.
#[test]
fn r15_rejects_two_value_types_with_the_same_leaf_name() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			one = ["!aa", a: Str] => crate::cf::Stats;
			two = ["!bb", b: Str] => crate::idx::Stats;
		}
	});
	assert_reports(&diags, "both generate the `AnyValue` variant `Stats`");
}

#[test]
fn r15_allows_one_value_type_under_many_keys() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			one = ["!aa", a: Str] => crate::cf::Stats;
			two = ["!bb", b: Str] => crate::cf::Stats;
		}
	});
	assert!(diags.is_empty(), "a shared value type is one variant, got {diags:?}");
}

// ---------------------------------------------------------------------------
// Structure: flattening, naming and bound derivation
// ---------------------------------------------------------------------------

#[test]
fn a_child_carries_the_fields_of_every_enclosing_level() {
	let p = prelude();
	let model = resolved(quote! {
		#p
		root = ["/"] {
			ns = ["*", ns: NamespaceId] {
				db = ["*", db: DatabaseId] {
					tbl = ["*", tb: Table] {
						record = ["*", @, id: Str] => Rec;
					}
				}
			}
		}
	});

	let record = model.types.iter().find(|t| t.name == "RecordKey").expect("record key");
	let names: Vec<String> = record.fields.iter().map(|f| f.name.to_string()).collect();
	assert_eq!(names, ["ns", "db", "tb", "id"], "fields must be in encode order");
	assert_eq!(record.route, "/*{ns}*{db}*{tb}*{id}");
	assert!(record.lifetime, "a key with borrowed fields needs a lifetime");
}

#[test]
fn truncation_points_are_named_after_the_last_field_they_include() {
	let p = prelude();
	let model = resolved(quote! {
		#p
		root = ["/"] {
			db = ["*", db: DatabaseId] {
				change_feed = ["#", @, ts: Str, @, "*", tb: Table] => Muts;
			}
		}
	});
	let names: Vec<String> = model
		.types
		.iter()
		.filter(|t| t.role == model::Role::Prefix)
		.map(|t| t.name.to_string())
		.collect();
	assert!(names.contains(&"ChangeFeedPrefix".to_owned()), "got {names:?}");
	assert!(names.contains(&"ChangeFeedTsPrefix".to_owned()), "got {names:?}");
}

/// A bound whose entry continues past it must clear every longer key when the
/// caller asks for an inclusive upper bound, which is what
/// `extended_by_keys` drives in the generated code.
#[test]
fn a_bound_knows_whether_stored_keys_continue_past_it() {
	let p = prelude();
	let model = resolved(quote! {
		#p
		root = ["/"] {
			graph = ["~", @, id: Str, @, ft: Table, fk: Str] => V;
		}
	});
	let first = model.types.iter().find(|t| t.name == "GraphPrefix").expect("graph prefix");
	let second = model.types.iter().find(|t| t.name == "GraphIdPrefix").expect("id prefix");
	assert!(first.extended_by_keys);
	assert!(second.extended_by_keys);
}

#[test]
fn open_cuts_must_follow_a_list() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			bad = ["!ix", a: Str, @open] => V;
		}
	});
	assert_reports(&diags, "`@open` must follow a list field");
}

#[test]
fn a_list_field_generates_both_a_closed_and_an_open_bound() {
	let p = prelude();
	let model = resolved(quote! {
		#p
		root = ["/"] (format_generic) {
			#[format(IndexFormat)]
			entry = ["!ix", @, fd: [Value..], @open, @, raw 0x03, id: Str] => V;
		}
	});
	let names: Vec<String> = model
		.types
		.iter()
		.filter(|t| t.role == model::Role::Prefix)
		.map(|t| t.name.to_string())
		.collect();
	assert!(names.contains(&"EntryPrefix".to_owned()), "got {names:?}");
	assert!(names.contains(&"EntryFdOpenPrefix".to_owned()), "got {names:?}");
	assert!(names.contains(&"EntryFdPrefix".to_owned()), "got {names:?}");

	// The two bounds cut at the same segment and differ only in whether the list
	// terminator is written, so their routes have to say which is which: the map is
	// checked in so that a change to a bound shows up as a readable diff.
	let route = |name: &str| {
		model
			.types
			.iter()
			.find(|t| t.name == name)
			.map(|t| t.route.clone())
			.unwrap_or_else(|| panic!("{name} missing"))
	};
	assert_ne!(
		route("EntryFdPrefix"),
		route("EntryFdOpenPrefix"),
		"the closed and open bounds must be distinguishable in the map"
	);
	assert!(route("EntryFdPrefix").ends_with("{fd..}"), "got {}", route("EntryFdPrefix"));
	assert!(route("EntryFdOpenPrefix").ends_with("{fd.."), "got {}", route("EntryFdOpenPrefix"));
}

/// `extended_by_keys` decides `next` versus `next_neighbour` for an inclusive
/// bound, so it has to agree with what the divergence walk calls an extension. An
/// extension that adds a literal continues the base's literal run rather than
/// adding a symbol, which a symbol-sequence comparison misses.
#[test]
fn an_extension_that_adds_a_literal_still_marks_its_base() {
	let p = prelude();
	let model = resolved(quote! {
		#p
		root = ["/"] {
			base = ["!aa", @, id: Str] => V;
			longer = base + ["!bb", extra: DocId] => V;
		}
	});

	let base = model.types.iter().find(|t| t.name == "BaseKey").expect("base");
	assert!(
		base.extended_by_keys,
		"a key whose run another key continues needs a bound past the whole run"
	);
}

/// A family three deep: each extension must carry the whole chain, not just its
/// immediate base's own segments.
#[test]
fn an_extension_chain_carries_every_ancestor_segment() {
	let p = prelude();
	let model = resolved(quote! {
		#p
		root = ["/"] {
			terms = ["!tt"] => V (also_range);
			term = terms + [t: Str] => V (also_range);
			posting = term + [doc: DocId] => V;
		}
	});

	let posting = model.types.iter().find(|t| t.name == "PostingKey").expect("posting");
	assert_eq!(posting.route, "/!tt{t}{doc}", "the chain's tag and every field must survive");
	let fields: Vec<String> = posting.fields.iter().map(|f| f.name.to_string()).collect();
	assert_eq!(fields, ["t", "doc"]);
}

/// A childless level is a bound-only subspace. Nothing inside it can establish
/// that it does not overlap a sibling, so the region itself is compared.
#[test]
fn a_childless_level_is_checked_against_its_siblings() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			pending = ["!hp"] {}
			other = ["!hp", a: Str] => V;
		}
	});
	assert_reports(&diags, "strict prefix");
}

/// Two levels claiming the same bytes is not caught by comparing keys: the keys
/// under them can be perfectly disjoint, and a range over either subtree still
/// covers the other's data.
#[test]
fn two_levels_cannot_claim_the_same_bytes() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			users = ["!u"] { by_name = ["!n", n: Str] => V; }
			usage = ["!u"] { by_day  = ["!d", d: Str] => V; }
		}
	});
	assert_reports(&diags, "subtrees overlap");
}

/// A subtree whose tag is a byte prefix of another's is the same hazard: the
/// shorter one's range runs over the longer one's keys.
#[test]
fn a_level_tag_may_not_be_a_prefix_of_another_levels() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			users = ["!u"]  { by_name = ["!n", n: Str] => V; }
			usage = ["!ug"] { by_day  = ["!d", d: Str] => V; }
		}
	});
	assert_reports(&diags, "subtrees overlap");
}

#[test]
fn nested_levels_are_not_reported_as_overlapping() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			ns = ["*", ns: NamespaceId] {
				db = ["*", db: DatabaseId] {
					table = ["!tb", @, tb: Table] => V;
				}
			}
		}
	});
	assert!(diags.is_empty(), "nesting is not an overlap, got {diags:?}");
}

#[test]
fn a_childless_level_beside_a_distinct_tag_is_accepted() {
	let p = prelude();
	let diags = diagnostics(quote! {
		#p
		root = ["/"] {
			pending = ["!hp"] {}
			other = ["!hr", a: Str] => V;
		}
	});
	assert!(diags.is_empty(), "distinct tags must be accepted, got {diags:?}");
}

// ---------------------------------------------------------------------------
// The generated map
// ---------------------------------------------------------------------------

#[test]
fn the_map_lists_every_route_with_its_value_type() {
	let p = prelude();
	let model = resolved(quote! {
		#p
		root = ["/"] {
			namespace = ["!ns", @, ns: Str] => NamespaceDefinition;
		}
	});
	let map = reverse::keyspace_map(&model);
	assert!(map.contains("/!ns{ns}"), "map missing the route:\n{map}");
	assert!(map.contains("NamespaceDefinition"), "map missing the value type:\n{map}");
	assert!(map.contains("NamespaceKey"), "map missing the generated type:\n{map}");
	assert!(map.contains("(range)"), "map should mark bound-only types:\n{map}");
}

// ---------------------------------------------------------------------------
// End to end: a schema shaped like the real one expands cleanly
// ---------------------------------------------------------------------------

#[test]
fn a_representative_schema_passes_every_rule() {
	let p = prelude();
	let src = quote! {
		#p
		enums {
			Dir: u8 { In = 2, Out = 3, Both = 4 }
		}

		version = ["!v"] => MajorVersion (also_range);

		root = ["/"] (format_generic) {
			namespace = ["!ns", @, ns: Str] => NamespaceDefinition;
			node = ["!nd", @, nd: Uuid] => Node;

			ns = ["*", ns: NamespaceId] (format_generic) {
				database = ["!db", @, db: Str] => DatabaseDefinition;

				db = ["*", db: DatabaseId] (format_generic) {
					table = ["!tb", @, tb: Table] => StoredTableDefinition;
					change_feed = ["#", @, ts: Str, @, "*", tb: Table] => TableMutations;

					tbl = ["*", tb: Table] (format_generic) {
						event = ["!ev", @, ev: Str] => StoredEventDefinition;
						record = ["*", @, id: Str] => Record
							(ctx = |k| RecordId::new(k.tb.clone(), k.id.clone()));

						graph = ["~", @, id: Str, @, dir: Dir, @, ft: Table, fk: Str] => Unit;
						graph_target = graph + [tt: Table, tk: Str] => Unit (ignore_trailing);

						idx = ["+", ix: IndexId] (format_generic) {
							#[format(IndexFormat)]
							entry = ["*", @, fd: [Value..], @open, @, raw 0x03, id: Str]
								=> IndexEntryValue;
							#[format(IndexFormat)]
							unique = ["*", fd: [Value..], raw 0x02] => IndexEntryValue;
						}
					}
				}
			}
		}
	};

	let model = resolved(src);
	// Levels, entries and every derived bound are present.
	assert!(model.types.len() > 20, "expected a populated model, got {}", model.types.len());

	// An extension carries its base's layout, so its route continues the base's.
	let graph = model.types.iter().find(|t| t.name == "GraphKey").expect("graph");
	let target = model.types.iter().find(|t| t.name == "GraphTargetKey").expect("target");
	assert!(
		target.route.starts_with(&graph.route),
		"extension route {} should continue {}",
		target.route,
		graph.route
	);
	let target_fields: Vec<String> = target.fields.iter().map(|f| f.name.to_string()).collect();
	assert_eq!(target_fields, ["ns", "db", "tb", "id", "dir", "ft", "fk", "tt", "tk"]);
}

#[test]
fn an_empty_schema_is_rejected_rather_than_silently_expanding() {
	let err = crate::expand(quote! {}).expect_err("empty schema should not expand");
	assert!(!err.to_string().is_empty());
}

/// Writes the expansion of a schema shaped like the real one to a scratch file so
/// generated code can be inspected. Ignored by default.
#[test]
#[ignore = "developer aid: prints the expansion"]
fn dump_expansion() {
	let src = quote! {
		types {
			fixed(4)  NamespaceId, DatabaseId;
			fixed(16) Uuid;
			var       Str = Cow<'k, str>;
			var       Table = Cow<'k, TableName>;
			var       Id = Cow<'k, RecordIdKey>;
			var       Bytes = Cow<'k, [u8]>;
		}
		version = ["!v"] => MajorVersion (also_range);
		root = ["/"] {
			namespace = ["!ns", @, ns: Str] => NamespaceDefinition;
			node = ["!nd", @, nd: Uuid] => Node;
			ns = ["*", ns: NamespaceId] {
				database = ["!db", @, db: Str] => DatabaseDefinition;
				db = ["*", db: DatabaseId] {
					table = ["!tb", @, tb: Table] => StoredTableDefinition;
					change_feed = ["#", @, ts: Bytes, @, "*", tb: Table] => TableMutations;
					tbl = ["*", tb: Table] {
						record = ["*", @, id: Id] => Record (ctx = |k: &RecordKey| todo!());
					}
				}
			}
		}
	};
	let tokens = crate::expand(src).expect("schema should expand");
	let path = ::std::env::var("KEYSPACE_DUMP").unwrap_or_else(|_| "/tmp/keyspace.rs".to_owned());
	::std::fs::write(&path, tokens.to_string()).expect("write dump");
	eprintln!("wrote {path}");
}
