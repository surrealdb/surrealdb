use anyhow::Result;
use reblessive::tree::Stk;

use crate::api::middleware::common::BodyStrategy;
use crate::api::middleware::req::BodyParser;
use crate::api::request::ApiRequest;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::fnc::args::{FromPublic, Optional};
use crate::val::{Closure, Value};

pub async fn body(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(FromPublic(mut req), next, Optional(strategy)): (
		FromPublic<ApiRequest>,
		Box<Closure>,
		Optional<FromPublic<BodyStrategy>>,
	),
) -> Result<Value> {
	let strategy = strategy.map(|x| x.0).unwrap_or_default();
	let mut parser = BodyParser::from((&mut req, strategy));
	parser.process().await?;

	next.invoke(stk, ctx, opt, doc, vec![req.into()]).await
}
