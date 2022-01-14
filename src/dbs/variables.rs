use crate::ctx::Context;
use crate::sql::value::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub type Variables = Option<HashMap<String, Value>>;

pub(crate) trait Attach {
	fn attach(self, ctx: Arc<Context>) -> Arc<Context>;
}

impl Attach for Variables {
	fn attach(self, ctx: Arc<Context>) -> Arc<Context> {
		match self {
			Some(m) => {
				let mut ctx = Context::new(&ctx);
				for (key, val) in m {
					ctx.add_value(key, val);
				}
				ctx.freeze()
			}
			None => ctx,
		}
	}
}
