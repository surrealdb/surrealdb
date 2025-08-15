use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::Document;
use reblessive::tree::Stk;

use super::IgnoreError;

impl Document {
	pub(super) async fn computed_fields(
        &mut self, 
        stk: &mut Stk, 
        ctx: &Context, 
        opt: &Options, 
        doc_kind: DocKind,
    ) -> Result<(), IgnoreError> {
        // Get the fields to compute
        let fields = self.fd(ctx, opt).await?;

        // Get the document to compute the fields for
		let doc = match doc_kind {
			DocKind::Initial => &mut self.initial,
			DocKind::Current => &mut self.current,
			DocKind::InitialReduced => &mut self.initial_reduced,
			DocKind::CurrentReduced => &mut self.current_reduced,
		};

        // Check if the fields have already been computed
        if doc.fields_computed {
            return Ok(());
        }

        // Compute the fields
		for fd in fields.iter() {
			if let Some(computed) = &fd.computed {
				let val = computed.compute(stk, ctx, opt, Some(&doc)).await.unwrap();
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