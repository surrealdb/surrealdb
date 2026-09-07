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
//!   — `RELATE` in `store_edges_data`, and every update-shaped path through
//!   [`Document::store_inline_adjacency_data`] (the blind rewrite is also what lazily repairs a
//!   stale generation);
//! * a payload the writer could not represent — the encoded values outgrew the cap — is written
//!   *spilled*, which readers treat as fallback, and the generation stamp catches everything else:
//!   any schema mutation that changes the inline field set bumps the table's generation, so a
//!   payload written under an older set no longer matches and falls back;
//! * the fold copies section bodies into block entries verbatim and never regenerates them, so a
//!   folded payload is exactly the one some transaction committed.

use anyhow::{Result, bail, ensure};
use surrealdb_datastore::values::graph::{
	ADJACENCY_FLAG_PROPS, AdjacencySection, AdjacencyValue, InlineProps,
};
use surrealdb_types::ToSql;

use crate::catalog::{FieldDefinition, LATEST_EDGE_VARIANT};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{Document, Error as DocError, Extras};
use crate::expr::Part;
use crate::expr::paths::{IN, OUT};
use crate::key::schema::GraphPointerKey;
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
		// A non-simple idiom cannot be inline; keep the order total for
		// that unreachable shape anyway. A single-part field's raw string
		// is its root name, so mixing the two arms stays consistent.
		_ => a.name.to_raw_string().cmp(&b.name.to_raw_string()),
	});
	inline
}

/// The payload name of an inline field: the raw root field name, exactly
/// as the field keys a record's object map. Readers match predicate
/// dependencies against it and synthesize candidate objects with it, so
/// payload evaluation sees the same keys record evaluation does.
pub(crate) fn inline_field_name(f: &FieldDefinition) -> String {
	match f.name.0.as_slice() {
		[Part::Field(name)] => name.as_str().to_owned(),
		// A non-simple idiom cannot be inline; render the unreachable
		// shape deterministically anyway.
		_ => f.name.to_raw_string(),
	}
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
	// borrowed straight off the document's object map; any other shape —
	// unreachable through DEFINE/ALTER — falls back to an owned pick.
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

impl Document {
	/// Rewrites both vertex-side pointer values of an already-stored edge
	/// with the current inline payload. Runs between the record write and
	/// the index write on every update-shaped path (UPDATE, UPSERT of an
	/// existing record, INSERT-on-duplicate); `RELATE` embeds the payload
	/// in `store_edges_data` instead.
	///
	/// The rewrite is unconditional — even when the projection did not
	/// change — because it is also the lazy repair path for a payload
	/// written under an older generation.
	///
	/// The pointer keys are derived from the committed edge's endpoints,
	/// and a write that moves `in` or `out` on the record is rejected: an
	/// edge's endpoints are fixed at `RELATE` time, and the record must
	/// keep naming the endpoints its adjacency was written under.
	pub(super) async fn store_inline_adjacency_data(
		&mut self,
		ctx: &FrozenContext,
		_opt: &Options,
	) -> Result<()> {
		// RELATE writes the payload with the pointer keys themselves.
		if matches!(self.extras, Extras::Relate(_, _, _)) {
			return Ok(());
		}
		// Only stored edges in the current key layout carry pointer values;
		// legacy-variant edges have no pointer keys to stamp, and a
		// lightweight edge is immutable and payload-less by definition.
		if !self.initial.doc.is_edge()
			|| self.initial.doc.edge_variant() != Some(LATEST_EDGE_VARIANT)
		{
			return Ok(());
		}
		let tb = self.doc_ctx.tb()?;
		if tb.drop
			|| matches!(&tb.table_type, crate::catalog::TableType::Relation(rel) if rel.lightweight)
		{
			return Ok(());
		}
		let fields = self.doc_ctx.fd()?;
		let inline = inline_fields(fields);
		if inline.is_empty() {
			return Ok(());
		}
		// The endpoints come from the committed edge: they are what the
		// existing pointer keys were derived from, so the rewrite below can
		// only restamp those keys — never mint adjacency elsewhere.
		let initial = self.initial.doc.as_ref();
		let Value::RecordId(ref l) = initial.pick(&IN) else {
			fail!("Expected a record id for the `in` field of an edge");
		};
		let Value::RecordId(ref r) = initial.pick(&OUT) else {
			fail!("Expected a record id for the `out` field of an edge");
		};
		// A write that lands different `in`/`out` values on the record — a
		// user payload, or a schema clause — is rejected outright: the
		// record and the adjacency about to be restamped must keep naming
		// the same endpoints, or a later rewrite would derive pointer keys
		// from a record that no longer matches them.
		let current = self.current.doc.as_ref();
		match current.pick(&IN) {
			Value::RecordId(ref id) if id == l => {}
			v => bail!(DocError::InOverride {
				value: v.to_sql(),
			}),
		}
		match current.pick(&OUT) {
			Value::RecordId(ref id) if id == r => {}
			v => bail!(DocError::OutOverride {
				value: v.to_sql(),
			}),
		}
		let rid = self.id()?;
		let ns = self.doc_ctx.ns().namespace_id;
		let db = self.doc_ctx.db().database_id;
		let cap = ctx.config.idx.graph_inline_props_cap;
		let section = encode_inline_section(current, &inline, tb.graph_inline_gen, cap)?;
		let value = AdjacencyValue {
			tombstone: false,
			sections: vec![section],
		};
		let txn = ctx.tx();
		let ltr = GraphPointerKey {
			ns,
			db,
			tb: std::borrow::Cow::Borrowed(&l.table),
			id: std::borrow::Cow::Borrowed(&l.key),
			dir: crate::expr::Dir::Out,
			foreign_table: std::borrow::Cow::Borrowed(&rid.table),
			foreign_key: std::borrow::Cow::Borrowed(&rid.key),
			target_table: std::borrow::Cow::Borrowed(&r.table),
			target_key: std::borrow::Cow::Borrowed(&r.key),
		};
		let rtl = GraphPointerKey {
			ns,
			db,
			tb: std::borrow::Cow::Borrowed(&r.table),
			id: std::borrow::Cow::Borrowed(&r.key),
			dir: crate::expr::Dir::In,
			foreign_table: std::borrow::Cow::Borrowed(&rid.table),
			foreign_key: std::borrow::Cow::Borrowed(&rid.key),
			target_table: std::borrow::Cow::Borrowed(&l.table),
			target_key: std::borrow::Cow::Borrowed(&l.key),
		};
		futures::try_join!(txn.set_key(&ltr, &value), txn.set_key(&rtl, &value))?;
		Ok(())
	}
}
