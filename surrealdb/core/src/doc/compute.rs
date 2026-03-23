use std::collections::{HashMap, HashSet};

use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::FieldDefinition;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::expr::FlowResultExt as _;
use crate::iam::AuthLimit;
use crate::val::RecordId;

impl Document {
	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::computed_fields", skip_all)
	)]
	pub(super) async fn computed_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc_kind: DocKind,
		needed_roots: Option<&HashSet<String>>,
	) -> anyhow::Result<()> {
		// Get the record id for the document
		// If the document has no id, it means there
		// is no schema with computed fields for it either
		let Ok(rid) = self.id() else {
			return Ok(());
		};

		let table_fields = self.fd(ctx, opt).await?;

		// Get the document to compute the fields for
		let doc = match doc_kind {
			DocKind::Initial => &mut self.initial,
			DocKind::Current => &mut self.current,
			DocKind::InitialReduced => &mut self.initial_reduced,
			DocKind::CurrentReduced => &mut self.current_reduced,
		};

		let Some(needed_roots) = needed_roots else {
			return Document::computed_fields_inner(
				stk,
				ctx,
				opt,
				rid.as_ref(),
				&table_fields,
				doc,
				None,
			)
			.await;
		};

		// Build dependency metadata for computed fields only.
		let mut dep_map: HashMap<String, crate::expr::computed_deps::ComputedDeps> = HashMap::new();
		for fd in table_fields.iter() {
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

		// Resolve transitive computed-field requirements from the selected roots.
		// Opaque dependencies trigger a safe full-compute fallback.
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
					&table_fields,
					doc,
					None,
				)
				.await;
			}
		};

		// If no computed fields are required, skip evaluation and keep
		// `fields_computed = false` so later full-materialization can still run.
		let has_required_computed = required.iter().any(|name| dep_map.contains_key(name));
		if !has_required_computed {
			return Ok(());
		}

		Document::computed_fields_inner(
			stk,
			ctx,
			opt,
			rid.as_ref(),
			&table_fields,
			doc,
			Some(&required),
		)
		.await?;

		Ok(())
	}

	#[cfg_attr(
		feature = "trace-doc-ops",
		instrument(level = "trace", name = "Document::computed_fields_inner", skip_all)
	)]
	pub(super) async fn computed_fields_inner(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
		fields: &[FieldDefinition],
		doc: &mut CursorDoc,
		required: Option<&HashSet<String>>,
	) -> anyhow::Result<()> {
		// Check if the fields have already been computed
		if doc.fields_computed {
			return Ok(());
		}

		// Compute the fields
		for fd in fields.iter() {
			let Some(computed) = &fd.computed else {
				continue;
			};

			if let Some(required) = required {
				let field_name = fd.name.to_raw_string();
				if !required.contains(&field_name) {
					continue;
				}
			}

			// Limit auth
			let opt = AuthLimit::try_from(&fd.auth_limit)?.limit_opt(opt);
			let mut val = computed.compute(stk, ctx, &opt, Some(doc)).await.catch_return()?;
			if let Some(kind) = fd.field_kind.as_ref() {
				val = val.coerce_to_kind(kind).map_err(|e| Error::FieldCoerce {
					record: rid.to_sql(),
					field_name: fd.name.to_sql(),
					error: Box::new(e),
				})?;
			}

			doc.doc.to_mut().put(&fd.name, val);
		}

		// Mark as fully computed only for full evaluation (not selective mode).
		if required.is_none() {
			doc.fields_computed = true;
		}

		Ok(())
	}
}

pub(super) enum DocKind {
	Initial,
	Current,
	InitialReduced,
	CurrentReduced,
}
