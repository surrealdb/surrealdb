use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, Field, Fields, Literal, SelectStatement};
use crate::val::Object;
use crate::val::record_id::RecordId;

pub(crate) async fn record_id_select_document(
	this: RecordId,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> anyhow::Result<Option<Object>> {
	// Fetch the record id's contents
	let stm = SelectStatement {
		fields: Fields::Select(vec![Field::All]),
		what: vec![Expr::Literal(Literal::RecordId(this.clone().into_literal()))],
		omit: vec![],
		only: false,
		with: None,
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		version: Expr::Literal(Literal::None),
		timeout: Expr::Literal(Literal::None),
		explain: None,
		tempfiles: false,
	};

	Ok(stk
		.run(|stk| crate::legacy::select_statement_compute(&stm, stk, ctx, opt, doc))
		.await?
		.first()
		.into_object())
}
