use anyhow::Result;
use reblessive::tree::Stk;

use crate::{api::{middleware::api_x::{common::BodyStrategy, req::BodyParser}, request::ApiRequest}, ctx::FrozenContext, dbs::Options, doc::CursorDoc, fnc::args::{FromPublic, Optional}, val::{Closure, Value}};

pub async fn body(
    (stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>), 
    (FromPublic(mut req), next, Optional(strategy)): (FromPublic<ApiRequest>, Box<Closure>, Optional<FromPublic<BodyStrategy>>)
) -> Result<Value> {
    let strategy = strategy.map(|x| x.0).unwrap_or_default();
    let mut parser = BodyParser::from((&mut req, strategy));
    parser.process().await?;

    next.invoke(stk, ctx, opt, doc, vec![req.into()]).await
}