use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::lookup::{ComputedLookupSubject, LookupSubject};

#[instrument(level = "trace", name = "LookupSubject::compute", skip_all)]
pub(crate) async fn lookup_subject_compute(
	this: &LookupSubject,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<ComputedLookupSubject> {
	match this {
		LookupSubject::Table {
			table,
			referencing_field,
		} => Ok(ComputedLookupSubject::Table {
			table: table.clone(),
			referencing_field: referencing_field.clone(),
		}),
		LookupSubject::Range {
			table,
			range,
			referencing_field,
		} => Ok(ComputedLookupSubject::Range {
			table: table.clone(),
			range: crate::legacy::record_id_key_range_lit_compute(range, stk, ctx, opt, doc)
				.await?,
			referencing_field: referencing_field.clone(),
		}),
	}
}
