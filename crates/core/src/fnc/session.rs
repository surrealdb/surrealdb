use crate::ctx::Context;
use crate::err::Error;
use crate::expr::paths::AC;
use crate::expr::paths::DB;
use crate::expr::paths::ID;
use crate::expr::paths::IP;
use crate::expr::paths::NS;
use crate::expr::paths::OR;
use crate::expr::paths::RD;
use crate::expr::paths::TK;
use crate::expr::value::Value;

pub fn ac(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(AC.as_ref()).ok()
}

pub fn db(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(DB.as_ref()).ok()
}

pub fn id(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(ID.as_ref()).ok()
}

pub fn ip(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(IP.as_ref()).ok()
}

pub fn ns(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(NS.as_ref()).ok()
}

pub fn origin(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(OR.as_ref()).ok()
}

pub fn rd(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(RD.as_ref()).ok()
}

pub fn token(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(TK.as_ref()).ok()
}
