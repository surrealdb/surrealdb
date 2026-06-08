use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::FieldDefinition;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::expr::FlowResultExt as _;
use crate::val::RecordId;

/// Identifies which of the four `CursorDoc` views on a [`Document`] to
/// evaluate computed fields against.
///
/// Reduction and computed-field evaluation are orthogonal: a SELECT against
/// a permission-restricted table needs reduction applied first, then
/// computed fields populated on the reduced view (so a permission predicate
/// `WHERE flag` sees `flag` after both filtering and computation).
#[derive(Clone, Copy, Debug)]
pub(super) enum DocKind {
	Initial,
	Current,
	InitialReduced,
	CurrentReduced,
}

impl Document {
	/// Returns true when this document's table has at least one field
	/// with a `COMPUTED` clause. Used to short-circuit the computed-field
	/// pipeline on tables that don't have any.
	pub(super) fn has_computed_fields(&self) -> bool {
		match self.doc_ctx.fd() {
			Ok(fields) => fields.iter().any(|fd| fd.computed.is_some()),
			Err(_) => false,
		}
	}

	/// Evaluate the closure of computed fields required to satisfy
	/// `needed_roots` against the chosen [`DocKind`] view of this
	/// document, populating the results into that view's `CursorDoc`.
	///
	/// - `needed_roots = None` means "every computed field is potentially referenced" — evaluate
	///   them all. This is the conservative choice used by the write paths that return the full new
	///   record (CREATE / UPSERT / UPDATE / RELATE / INSERT default output) and by `Output::After`
	///   / `Output::Before` / `Output::Diff`.
	/// - `needed_roots = Some(roots)` restricts evaluation to the transitive closure of those root
	///   field names. SELECT extracts `roots` from its projection / WHERE / ORDER / GROUP / SPLIT.
	///
	/// When any field's dependency set is `is_complete = false`
	/// (opaque sub-expressions, parameters, graph traversals), we fall
	/// back to evaluating every computed field. This is the same
	/// safety net main carries.
	pub(super) async fn compute_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc_kind: DocKind,
		needed_roots: Option<&HashSet<String>>,
	) -> anyhow::Result<()> {
		// Skip when the table has no computed fields at all.
		if !self.has_computed_fields() {
			return Ok(());
		}
		// Computed field evaluation needs a record id — temporary
		// documents (no id) have no schema to project against.
		let Ok(rid) = self.id() else {
			return Ok(());
		};
		let fields = Arc::clone(self.doc_ctx.fd()?);

		// Resolve the doc reference up front so the &mut self borrow
		// is dropped before the async call below.
		let doc: &mut CursorDoc = match doc_kind {
			DocKind::Initial => &mut self.initial,
			DocKind::Current => &mut self.current,
			DocKind::InitialReduced => match self.initial_reduced.as_mut() {
				Some(d) => d,
				None => return Ok(()),
			},
			DocKind::CurrentReduced => match self.current_reduced.as_mut() {
				Some(d) => d,
				None => return Ok(()),
			},
		};

		let Some(needed_roots) = needed_roots else {
			return Document::computed_fields_inner(
				stk,
				ctx,
				opt,
				rid.as_ref(),
				&fields,
				doc,
				None,
			)
			.await;
		};

		// Build dependency metadata for computed fields only.
		let mut dep_map: HashMap<String, crate::expr::computed_deps::ComputedDeps> = HashMap::new();
		for fd in fields.iter() {
			if fd.computed.is_none() {
				continue;
			}
			let field_name = fd.name.to_raw_string();
			let deps = if let Some(cd) = &fd.computed_deps {
				crate::expr::computed_deps::ComputedDeps {
					fields: cd.fields.clone(),
					is_complete: cd.is_complete,
				}
			} else if let Some(expr) = &fd.computed {
				crate::expr::computed_deps::extract_computed_deps(expr)
			} else {
				crate::expr::computed_deps::ComputedDeps::default()
			};
			dep_map.insert(field_name, deps);
		}

		// Resolve transitive computed-field requirements from the selected
		// roots. Opaque dependencies trigger a safe full-compute fallback.
		let required = match crate::expr::computed_deps::resolve_required_computed_fields(
			needed_roots,
			&dep_map,
		) {
			Some(required) => required,
			None => {
				return Document::computed_fields_inner(
					stk,
					ctx,
					opt,
					rid.as_ref(),
					&fields,
					doc,
					None,
				)
				.await;
			}
		};

		// If the projection doesn't reach any computed field, leave the
		// cursor untouched. Keep `fields_computed = false` so a later
		// full evaluation (e.g. live-query notification) can still run.
		let has_required_computed = required.iter().any(|name| dep_map.contains_key(name));
		if !has_required_computed {
			return Ok(());
		}

		Document::computed_fields_inner(stk, ctx, opt, rid.as_ref(), &fields, doc, Some(&required))
			.await
	}

	pub(super) async fn computed_fields_inner(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
		fields: &[FieldDefinition],
		doc: &mut CursorDoc,
		required: Option<&HashSet<String>>,
	) -> anyhow::Result<()> {
		// Skip when the full set has already been materialized — the
		// flag only reflects "every computed field has run", so we
		// must not honour it for partial (selective) computations.
		if required.is_none() && doc.fields_computed {
			return Ok(());
		}

		// Compute the fields
		for fd in fields.iter() {
			let Some(computed) = &fd.computed else {
				continue;
			};
			// Restrict to the resolved closure when in selective mode.
			if let Some(required) = required {
				let field_name = fd.name.to_raw_string();
				if !required.contains(&field_name) {
					continue;
				}
			}

			let mut val = computed.compute(stk, ctx, opt, Some(doc)).await.catch_return()?;
			if let Some(kind) = fd.field_kind.as_ref() {
				val = val.coerce_to_kind(kind).map_err(|e| Error::FieldCoerce {
					record: rid.to_sql(),
					field_name: fd.name.to_sql(),
					error: Box::new(e),
				})?;
			}

			doc.doc.to_mut().put(&fd.name, val);
		}

		// Only flag as fully computed for full evaluations. Selective
		// runs leave the flag alone so a later non-selective pass can
		// still fill in the missing fields.
		if required.is_none() {
			doc.fields_computed = true;
		}

		Ok(())
	}
}
