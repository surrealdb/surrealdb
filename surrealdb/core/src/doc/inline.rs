//! Inline edge properties: keeping the vertex-side adjacency payloads in
//! step with the edge record.
//!
//! An edge table can mark top-level fields `INLINE`, and every write of an
//! edge then embeds those fields' values into both of its vertex-side
//! pointer values (the [`ADJACENCY_FLAG_PROPS`] section), so a filtered
//! traversal can evaluate an edge-table predicate without fetching the
//! edge record.
//!
//! # The invariant readers rely on
//!
//! Within any committed transaction, every pointer key of a non-lightweight
//! edge on a generation-G inline table carries generation-G values equal to
//! the committed record's projection, or is stale-marked/absent — readers
//! never evaluate silently-stale data. It holds because:
//!
//! * the record write and both pointer-value writes commit in one transaction, so readers see
//!   both-old or both-new;
//! * every write path that can change an edge's fields rewrites both pointer values unconditionally
//!   — `RELATE` when it stores the edge, and every update-shaped path in the write stages (the
//!   blind rewrite is also what lazily repairs a stale generation);
//! * a payload the writer could not represent — the encoded values outgrew the cap — is written
//!   *spilled*, which readers treat as fallback, and the generation stamp catches everything else:
//!   any schema mutation that changes the inline field set bumps the table's generation, so a
//!   payload written under an older set no longer matches and falls back;
//! * the fold copies section bodies into block entries verbatim and never regenerates them, so a
//!   folded payload is exactly the one some transaction committed.

use anyhow::{Result, ensure};
use surrealdb_datastore::values::graph::{ADJACENCY_FLAG_PROPS, AdjacencySection, InlineProps};

use crate::catalog::FieldDefinition;
use crate::expr::Part;
use crate::val::Value;

/// The table's inline fields, in the canonical payload order: sorted by
/// the raw field-name bytes. An inline field is a top-level single-part
/// idiom (DEFINE and ALTER enforce it), so the raw name is the whole
/// identity — no SQL rendering or escaping takes part. The order is part
/// of what a generation identifies — writers and readers derive it the
/// same way from the same field set.
pub(crate) fn inline_fields(fields: &[FieldDefinition]) -> Vec<&FieldDefinition> {
	fn root(f: &FieldDefinition) -> Option<&str> {
		match f.name.0.as_slice() {
			[Part::Field(name)] => Some(name.as_str()),
			_ => None,
		}
	}
	let mut inline: Vec<&FieldDefinition> = fields.iter().filter(|f| f.inline).collect();
	inline.sort_by(|a, b| match (root(a), root(b)) {
		(Some(a), Some(b)) => a.as_bytes().cmp(b.as_bytes()),
		// A non-simple idiom cannot be inline. This arm only keeps the
		// order total for that shape. A single-part field's raw string is
		// its root name, so both arms compare consistently.
		_ => a.name.to_raw_string().cmp(&b.name.to_raw_string()),
	});
	inline
}

/// Encodes the inline payload section for one edge document: the inline
/// fields' values read from `doc` in canonical order, stamped with the
/// table's generation — or a spilled marker when the encoded values
/// outgrow `cap` bytes.
pub(crate) fn encode_inline_section(
	doc: &Value,
	fields: &[&FieldDefinition],
	generation: u32,
	cap: usize,
) -> Result<AdjacencySection> {
	ensure!(
		fields.len() <= u8::MAX as usize,
		"an inline props payload cannot carry more than 255 values"
	);
	let none = Value::None;
	// An inline field is a top-level single-part idiom, so its value is
	// borrowed from the document's object map. Any other shape, which
	// DEFINE and ALTER reject, falls back to an owned pick.
	let values: Vec<std::borrow::Cow<'_, Value>> = fields
		.iter()
		.map(|f| match (f.name.0.as_slice(), doc) {
			([Part::Field(name)], Value::Object(obj)) => {
				std::borrow::Cow::Borrowed(obj.get(name.as_str()).unwrap_or(&none))
			}
			_ => std::borrow::Cow::Owned(doc.pick(&f.name)),
		})
		.collect();
	let body = match InlineProps::encode_values_capped(
		generation,
		values.iter().map(|v| v.as_ref()),
		fields.len() as u8,
		cap,
	)? {
		Some(body) => body,
		// The encoded values outgrew the cap: record the spill so readers
		// fall back to the record.
		None => InlineProps::spilled(generation).encode()?,
	};
	Ok(AdjacencySection {
		flag_bit: ADJACENCY_FLAG_PROPS.trailing_zeros() as u8,
		body,
	})
}
