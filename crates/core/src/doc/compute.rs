use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::FieldDefinition;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::expr::FlowResultExt as _;
use crate::val::RecordId;

impl Document {
	#[instrument(level = "trace", name = "Document::computed_fields", skip_all)]
	pub(super) async fn computed_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc_kind: DocKind,
	) -> anyhow::Result<()> {
		// Get the record id for the document
		// If the document has no id, it means there
		// is no schema with computed fields for it either
		let Ok(rid) = self.id() else {
			return Ok(());
		};

		// Get the document to compute the fields for
		let doc = match doc_kind {
			DocKind::Initial => &mut self.initial,
			DocKind::Current => &mut self.current,
			DocKind::InitialReduced => &mut self.initial_reduced,
			DocKind::CurrentReduced => &mut self.current_reduced,
		};

		let table_fields = self.doc_ctx.fd()?;

		Document::computed_fields_inner(stk, ctx, opt, rid.as_ref(), table_fields, doc).await?;

		Ok(())
	}

	#[instrument(level = "trace", name = "Document::computed_fields_inner", skip_all)]
	pub(super) async fn computed_fields_inner(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
		fields: &[FieldDefinition],
		doc: &mut CursorDoc,
	) -> anyhow::Result<()> {
		// Check if the fields have already been computed
		if doc.fields_computed {
			return Ok(());
		}

		// Compute the fields
		for fd in fields.iter() {
			if let Some(computed) = &fd.computed {
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
		}

		// Mark the fields as computed
		doc.fields_computed = true;

		Ok(())
	}
}

pub(super) enum DocKind {
	Initial,
	Current,
	InitialReduced,
	CurrentReduced,
}
