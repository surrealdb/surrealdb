//! The table's field schema, applied to one record.
//!
//! The field kernel: types, defaults, `VALUE`, `ASSERT`, the cleanup of what the schema leaves
//! empty, and the `COMPUTED` derivation the record's observers read.
//!
//! Definer's-rights narrowing applies here (ADR 0001): a field clause runs
//! with the definer's authority, not the caller's, and so does the decision of
//! whether that field's `PERMISSIONS` are enforced at all. The narrowing is
//! per field, because the fields of one table need not share a definer.
//!
//! # A stored clause needs a compilation route
//!
//! The kernel evaluates clauses the catalog stores as text, and the
//! engine reaches one through [`crate::exec::permission`], which compiles the
//! logical body the definition carries and, for a clause carrying none,
//! carries it as an [`UncompilableClause`](crate::exec::permission::UncompilableClause)
//! that fails where it is evaluated. A field carries a logical body for its
//! `DEFAULT`, its `VALUE`, its `ASSERT`, its `COMPUTED` and its `PERMISSIONS
//! FOR select`; its `PERMISSIONS FOR create` / `update` predicate carries none
//! and is therefore always the carried form. [`clause_frame`] binds the
//! parameters every clause reads, so a clause means the same thing whichever
//! form it takes. A clause that fails closed fails the write that reaches it
//! rather than letting the record through unjudged.

use std::collections::HashSet;

use crate::catalog::FieldDefinition;
use crate::doc::Error as DocError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::expr::ControlFlow;
use crate::expr::idiom::{Idiom, IdiomTrie, IdiomTrieContains};
use crate::expr::kind::Kind;
use crate::expr::part::Part;
use crate::expr::statements::define::kind_contains_object;
use crate::val::Value;
use crate::val::value::every::ArrayBehaviour;

/// Whether a place holds nothing under a kind that admits nothing.
///
/// Two rules read it. An absent optional parent makes its children skippable,
/// because there is no object for their clauses to be about; and an `ASSERT`
/// is not judged against an absent optional value, because the field declared
/// that absence acceptable.
pub(crate) fn optional_and_absent(fd: &FieldDefinition, val: &Value) -> bool {
	val.is_none() && fd.field_kind.as_ref().is_some_and(Kind::can_be_none)
}

/// Restore the uniqueness of a set whose elements were rewritten.
///
/// A clause on `field[*]` is applied to each element, and coercion can
/// normalise two distinct stored elements to the same value, which leaves a
/// `TYPE set` parent holding duplicates.
pub(crate) fn normalise_set_elements(row: &mut WriteRow, fd: &FieldDefinition) {
	let parts = fd.name.0.as_slice();
	if parts.len() < 2 || !matches!(parts.last(), Some(Part::All)) {
		return;
	}
	let parent = Idiom(parts[..parts.len() - 1].to_vec());
	if let Value::Set(set) = row.current.pick(&parent) {
		row.current.put(&parent, Value::Set(set.normalize()));
	}
}

/// Raise on the first path of the post-image that the table's field
/// definitions do not reach.
///
/// The table is `SCHEMAFULL`, so an undefined field is refused rather than
/// dropped: a record that silently lost a field would be stored as something
/// other than what the caller wrote.
pub(crate) fn refuse_undefined_fields(
	current: &Value,
	fields: &[FieldDefinition],
	table: &crate::val::TableName,
) -> Result<(), ControlFlow> {
	// Which paths a definition reaches, and whether paths *below* each one are
	// admitted without a definition of their own.
	let mut defined: IdiomTrie<bool> = IdiomTrie::new();
	// A path that carries its own definition is never treated as the
	// schemaless interior of an ancestor, however permissive that ancestor is.
	let explicit: HashSet<&[Part]> = fields.iter().map(|fd| fd.name.0.as_slice()).collect();

	for fd in fields.iter() {
		let field_kind = fd.field_kind.as_ref();
		// `TYPE any` and a literal type admit anything beneath them — a
		// literal's interior is caught by coercion instead — and an object
		// type does so only when the field is `FLEXIBLE`.
		let allows_nested = field_kind.is_some_and(Kind::is_any)
			|| field_kind.is_some_and(Kind::contains_literal)
			|| (field_kind.is_some_and(kind_contains_object) && fd.flexible);

		for k in current.each(&fd.name) {
			defined.insert(&k, allows_nested);
			// The ancestors of an expanded path are reachable too. One with no
			// definition of its own is a schemaless object on the way to a
			// defined field, so its interior is admitted.
			for i in 1..k.len() {
				if !explicit.contains(&&k[..i]) {
					defined.insert(&k[..i], true);
				}
			}
		}
	}

	for path in current.every(None, true, ArrayBehaviour::Full).iter() {
		// `id`, `in` and `out` are the record's own, not the schema's.
		if path.is_special() {
			continue;
		}
		match defined.contains(path) {
			// Defined, or below something that admits an undefined interior.
			IdiomTrieContains::Exact(_) | IdiomTrieContains::Ancestor(true) => continue,
			// Below a definition that admits no undefined interior. An array
			// index is addressed by the definition of the array itself, so it
			// needs none of its own; anything else is undefined.
			IdiomTrieContains::Ancestor(false) if path.last().is_some_and(Part::is_index) => {
				continue;
			}
			// Below nothing the schema defines at all. The index exemption does
			// not extend here: an index reached from no definition is not
			// addressed by one, and the path that carries it is refused on its
			// own account only when the schema reaches that path.
			IdiomTrieContains::Ancestor(false) | IdiomTrieContains::None => {
				return Err(ControlFlow::Err(anyhow::anyhow!(DocError::FieldUndefined {
					table: table.as_str().to_string(),
					field: path.clone()
				})));
			}
		}
	}
	Ok(())
}

/// Remove `NONE` recursively from objects, leaving array elements alone.
///
/// Answers whether `v` survives: an object or array always does, a `NONE`
/// never does, and anything else does. An array is walked so that a `NONE`
/// nested inside one of its objects is still removed, but a `NONE` that *is* an
/// element stays — dropping it would shift every element after it.
// The rule is about the two container shapes; every other value is carried
// through unchanged, so the wildcard is the rule rather than an omission.
#[allow(clippy::wildcard_enum_match_arm)]
pub(crate) fn clean_none(v: &mut Value) -> bool {
	match v {
		Value::None => false,
		Value::Object(o) => {
			o.retain(|_, v| clean_none(v));
			true
		}
		Value::Array(x) => {
			x.iter_mut().for_each(|x| {
				clean_none(x);
			});
			true
		}
		_ => true,
	}
}

/// Whether anything other than the statement's own projection reads this record
/// after it is written: a table event, a live-query subscription, or a
/// changefeed on the table or on its database.
///
/// Under `OPTION IMPORT` nothing does. An import runs no observer, so deriving
/// the computed values one would read evaluates bodies whose result is
/// discarded — and a body reading a table the dump defines later raises, which
/// would fail a restore of a database's own export.
pub(crate) fn has_record_observers(ctx: &WriteCtx) -> bool {
	if ctx.exec().options().is_some_and(|options| options.import) {
		return false;
	}
	!ctx.events.is_empty()
		|| !ctx.lives.is_empty()
		|| ctx.table.changefeed.is_some()
		|| ctx.db.changefeed.is_some()
}
