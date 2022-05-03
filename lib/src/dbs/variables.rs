use crate::ctx::Context;
use crate::sql::value::Value;
use std::collections::HashMap;

pub type Variables = Option<HashMap<String, Value>>;

pub(crate) trait Attach {
	fn attach(self, ctx: Context) -> Context;
}

impl Attach for Variables {
	fn attach(self, mut ctx: Context) -> Context {
		match self {
			Some(m) => {
				for (key, val) in m {
					ctx.add_value(key, val);
				}
				ctx
			}
			None => ctx,
		}
	}
}
