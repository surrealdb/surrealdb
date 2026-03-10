use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::expr::paths::{AC, DB, ID, IP, NS, OR, RD, TK};
use crate::val::Value;

pub fn ac(ctx: &FrozenContext, _: ()) -> Result<Value> {
	Ok(ctx.value("session").unwrap_or(&Value::None).pick(AC.as_ref()))
}

pub fn db(ctx: &FrozenContext, _: ()) -> Result<Value> {
	Ok(ctx.value("session").unwrap_or(&Value::None).pick(DB.as_ref()))
}

pub fn id(ctx: &FrozenContext, _: ()) -> Result<Value> {
	Ok(ctx.value("session").unwrap_or(&Value::None).pick(ID.as_ref()))
}

pub fn ip(ctx: &FrozenContext, _: ()) -> Result<Value> {
	Ok(ctx.value("session").unwrap_or(&Value::None).pick(IP.as_ref()))
}

pub fn ns(ctx: &FrozenContext, _: ()) -> Result<Value> {
	Ok(ctx.value("session").unwrap_or(&Value::None).pick(NS.as_ref()))
}

pub fn origin(ctx: &FrozenContext, _: ()) -> Result<Value> {
	Ok(ctx.value("session").unwrap_or(&Value::None).pick(OR.as_ref()))
}

pub fn rd(ctx: &FrozenContext, _: ()) -> Result<Value> {
	Ok(ctx.value("session").unwrap_or(&Value::None).pick(RD.as_ref()))
}

pub fn token(ctx: &FrozenContext, _: ()) -> Result<Value> {
	Ok(ctx.value("session").unwrap_or(&Value::None).pick(TK.as_ref()))
}
