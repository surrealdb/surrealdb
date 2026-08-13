use std::collections::HashSet;
use std::sync::Arc;

use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::FieldDefinition;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document, Error};
use crate::exe::FlowResultExt as _;
use crate::iam::AuthLimit;
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
	/// Materialise the pre-mutation snapshot that a statement's `WHERE`
	/// condition and its data clause both read, and return the view they
	/// must be evaluated against.
	///
	/// The snapshot is the field-level-permission-reduced view of
	/// `self.current` with every `COMPUTED` field populated, and then the
	/// computed fields filtered by their own `PERMISSIONS FOR select` — a
	/// field the caller may not read is absent from the snapshot whether it is
	/// stored or computed. Both readers share one image, so a `WHERE`
	/// predicate and a data-clause expression in the same statement always
	/// agree about what a computed field holds:
	///
	/// ```surql
	/// DEFINE FIELD can_drive ON person COMPUTED age >= 18;
	/// CREATE person:kid SET age = 17;
	/// -- `can_drive` is false in both clauses, and `age` is still 17 in
	/// -- both, because neither reads the value the statement is writing.
	/// UPDATE person:kid SET age = 18, adult = can_drive WHERE can_drive = false;
	/// ```
	///
	/// Reads never observe the statement's own writes: the data clause is
	/// evaluated in full against this snapshot before `process_record_data`
	/// applies any assignment, which is what makes
	/// `SET a = a + 1, b = a + 1` assign `b` from the old `a`.
	///
	/// `COMPUTED` fields are populated only for a record that already
	/// exists. A creating shape has no pre-image to derive them from, so
	/// they read as `NONE` there.
	///
	/// Both halves are memoised — the reduce on `self.current_reduced`, the
	/// computed fields on the view's `fields_computed` flag — so repeated
	/// calls cost at most one reduce and one computed-field pass per row,
	/// in any order.
	pub(super) async fn materialise_current_snapshot(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> anyhow::Result<&CursorDoc> {
		// Apply field-level select permissions to the pre-mutation record.
		// This populates `current_reduced` exactly when a reduce was needed.
		self.reduce_current(stk, ctx, opt).await?;
		// Populate the computed fields on whichever view that produced
		if !self.is_new() {
			let kind = match self.current_reduced.is_some() {
				true => DocKind::CurrentReduced,
				false => DocKind::Current,
			};
			self.compute_fields(stk, ctx, opt, kind, None).await?;
			// SECURITY: the reduce ran before those fields existed, so it could
			// not apply their own `PERMISSIONS FOR select`. Apply them now: this
			// image is what the `WHERE` condition and the data clause read, and
			// a data clause can copy a value it reads into a field the caller is
			// allowed to select.
			self.filter_reduced_computed_fields(stk, ctx, opt).await?;
		}
		// Hand back the view the caller must evaluate against
		Ok(self.current_reduced.as_ref().unwrap_or(&self.current))
	}

	/// Materialise the post-write image that this record's observers read:
	/// events, live-query notifications, changefeed entries, and the
	/// statement's own output.
	///
	/// `COMPUTED` fields are stripped before storage, so an observer reading
	/// `self.current` or `self.initial` directly would otherwise see a record
	/// without them. Both views are populated because an observer can read
	/// either one — an event body reads `$after` and `$before`, a changefeed
	/// entry stores a diff between them.
	///
	/// A view holding no record is left alone. There is no earlier version of a
	/// record this statement created and no later version of one it deleted,
	/// and deriving a computed field from an absent field raises rather than
	/// yielding an empty answer.
	///
	/// Runs only when the table actually has an observer, because this is the
	/// one place that has to evaluate every computed field rather than the ones
	/// a projection asked for. An event body, a live-query predicate and a
	/// changefeed entry all read the record as a whole, and an event body reads
	/// its views through `$before` / `$after` where no static analysis can tell
	/// which fields it wants. A table with no observer keeps the read side's
	/// rule that a computed field a query did not request is never evaluated
	/// (issue #7094) — including one whose body would throw.
	///
	/// Runs after the record is stored, so the values it populates are never
	/// written; `output_write` then finds them already in place.
	pub(super) async fn materialise_observed_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> anyhow::Result<()> {
		// Nothing to materialise when the table has no computed fields
		if !self.has_computed_fields() {
			return Ok(());
		}
		// Nothing observes this write, so leave the computed fields to the read
		// side, which evaluates only what a projection consumes
		if !self.has_record_observers() {
			return Ok(());
		}
		// The post-write record, absent on a delete, and the pre-write record,
		// absent on a create
		self.compute_fields_if_present(stk, ctx, opt, DocKind::Current, None).await?;
		self.compute_fields_if_present(stk, ctx, opt, DocKind::Initial, None).await?;
		Ok(())
	}

	/// Returns true when something other than the statement's own projection
	/// reads this record after it is written: a table event, a live-query
	/// subscription, or a changefeed on the table or its database.
	///
	/// Each reads the record as a whole rather than a resolvable set of fields, so
	/// any of them forces every computed field to be evaluated. Narrowing this to
	/// what the observer actually reads is not possible with the dependency
	/// analysis available: an event's `WHEN` almost always names `$event` and its
	/// `THEN` is almost always a nested statement, and both analyse as opaque.
	///
	/// The consequence is that a computed field which is not derivable for a given
	/// record — `COMPUTED a * 2` where that record has no `a` — fails a write to a
	/// table carrying any observer, where the same write to an observer-free table
	/// succeeds whenever the projection does not ask for the field.
	///
	/// The projection is not an observer: it resolves the fields it needs and
	/// evaluates only those.
	fn has_record_observers(&self) -> bool {
		if self.doc_ctx.ev().is_ok_and(|events| !events.is_empty()) {
			return true;
		}
		if self.doc_ctx.lv().is_ok_and(|lives| !lives.is_empty()) {
			return true;
		}
		match self.doc_ctx.tb() {
			Ok(tb) => tb.changefeed.is_some() || self.doc_ctx.db().changefeed.is_some(),
			Err(_) => false,
		}
	}

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

		// Derived once when the table context was built, not per document:
		// extracting them walks every computed field's expression tree, and the
		// result depends only on the field set.
		let dep_map = self.doc_ctx.computed_deps()?;

		// Resolve transitive computed-field requirements from the selected
		// roots. Opaque dependencies trigger a safe full-compute fallback.
		let required = match crate::expr::computed_deps::resolve_required_computed_fields(
			needed_roots,
			dep_map,
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

	/// Populate the computed fields on one view, unless that view holds no
	/// record.
	///
	/// A creating statement has no pre-mutation record and a deleting one has no
	/// post-mutation record. A computed field cannot be derived from an absent
	/// one: `COMPUTED a * 2` against an empty view raises rather than yielding
	/// `NONE`, because the multiplication itself fails. Leaving an absent view
	/// untouched is also what a table with no computed fields returns for it, so
	/// `CREATE … RETURN BEFORE` answers the same either way.
	pub(super) async fn compute_fields_if_present(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc_kind: DocKind,
		needed_roots: Option<&HashSet<String>>,
	) -> anyhow::Result<()> {
		let doc = match doc_kind {
			DocKind::Initial => &self.initial,
			DocKind::Current => &self.current,
			DocKind::InitialReduced => match self.initial_reduced.as_ref() {
				Some(doc) => doc,
				None => return Ok(()),
			},
			DocKind::CurrentReduced => match self.current_reduced.as_ref() {
				Some(doc) => doc,
				None => return Ok(()),
			},
		};
		if doc.doc.as_ref().is_none() {
			return Ok(());
		}
		self.compute_fields(stk, ctx, opt, doc_kind, needed_roots).await
	}

	/// Evaluate one `COMPUTED` field's body against `doc` and write the result
	/// back into it.
	///
	/// The single place a computed body is evaluated. Every caller goes through
	/// here — the read side's [`Self::computed_fields_inner`] and the write
	/// side's `process_table_fields`, which materialises a computed field that
	/// another field's clause reads — so that the rules governing how a body may
	/// run are stated once and cannot be missed by a new call site.
	pub(super) async fn compute_one_field(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
		fd: &FieldDefinition,
		computed: &crate::expr::Expr,
		doc: &mut CursorDoc,
	) -> anyhow::Result<()> {
		// SECURITY: apply the field's AUTH LIMIT so the body runs under the
		// definer's auth, not the reader's. Without it a low-privileged
		// definer can plant a body that a high-privileged reader then
		// executes with their own privilege. Mirrors the streaming
		// `compute_fields_for_value`.
		//
		// SECURITY: and refuse writes from the body, whichever auth it runs
		// under. `Expr::contains_mutation` rejects a mutation written into the
		// body at definition time but cannot see through a call to a
		// user-defined function, so without this a read of the field would
		// mutate the database as the definer.
		let opt = &opt.limited_by(&AuthLimit::try_from(&fd.auth_limit)?).new_for_computed_field();
		// Evaluate the body against the view being populated
		let mut val =
			crate::legacy::expr_compute(computed, stk, ctx, opt, Some(doc)).await.catch_return()?;
		// A computed value still has to satisfy the field's declared type
		if let Some(kind) = fd.field_kind.as_ref() {
			val = val.coerce_to_kind(kind).map_err(|e| Error::FieldCoerce {
				record: rid.to_sql(),
				field_name: fd.name.to_sql(),
				error: Box::new(e),
			})?;
		}
		doc.doc.to_mut().put(&fd.name, val);
		Ok(())
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

			Document::compute_one_field(stk, ctx, opt, rid, fd, computed, doc).await?;
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
