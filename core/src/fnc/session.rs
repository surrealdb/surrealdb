use crate::ctx::Context;
use crate::err::Error;
use crate::sql::paths::AC;
use crate::sql::paths::DB;
use crate::sql::paths::ID;
use crate::sql::paths::IP;
use crate::sql::paths::NS;
use crate::sql::paths::OR;
use crate::sql::paths::RD;
use crate::sql::paths::TK;
use crate::sql::value::Value;

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
