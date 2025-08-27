use reblessive::tree::Stk;

use crate::catalog::FieldDefinition;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::expr::FlowResultExt as _;
use crate::val::RecordId;

impl Document {
	pub(super) async fn computed_fields(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc_kind: DocKind,
	) -> anyhow::Result<()> {
		// Get the record id for the document
		// If the document has no id, it means there
		// is no schema with computed fields for it either
		if let Ok(rid) = self.id() {
			// Get the fields to compute
			let fields = self.fd(ctx, opt).await?;

			// Get the document to compute the fields for
			let doc = match doc_kind {
				DocKind::Initial => &mut self.initial,
				DocKind::Current => &mut self.current,
				DocKind::InitialReduced => &mut self.initial_reduced,
				DocKind::CurrentReduced => &mut self.current_reduced,
			};

			Document::computed_fields_inner(stk, ctx, opt, rid.as_ref(), fields.as_ref(), doc)
				.await?;
		}

		Ok(())
	}

	pub(super) async fn computed_fields_inner(
		stk: &mut Stk,
		ctx: &Context,
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
						thing: rid.to_string(),
						field_name: fd.name.to_string(),
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
