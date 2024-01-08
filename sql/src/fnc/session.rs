use crate::ctx::Context;
use crate::err::Error;
use crate::paths::DB;
use crate::paths::ID;
use crate::paths::IP;
use crate::paths::NS;
use crate::paths::OR;
use crate::paths::SC;
use crate::paths::SD;
use crate::paths::TK;
use crate::value::Value;

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

pub fn sc(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(SC.as_ref()).ok()
}

pub fn sd(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(SD.as_ref()).ok()
}

pub fn token(ctx: &Context, _: ()) -> Result<Value, Error> {
	ctx.value("session").unwrap_or(&Value::None).pick(TK.as_ref()).ok()
}
