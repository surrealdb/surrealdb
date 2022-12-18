use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;
use std::collections::BTreeMap;

pub type Variables = Option<BTreeMap<String, Value>>;

pub(crate) trait Attach {
	fn attach(self, ctx: Context) -> Result<Context, Error>;
}

impl Attach for Variables {
	fn attach(self, mut ctx: Context) -> Result<Context, Error> {
		match self {
			Some(m) => {
				for (key, val) in m {
					// Check if the variable is a protected variable
					match PROTECTED_PARAM_NAMES.contains(&key.as_str()) {
						// The variable isn't protected and can be stored
						false => ctx.add_value(key, val),
						// The user tried to set a protected variable
						true => {
							return Err(Error::InvalidParam {
								name: key,
							})
						}
					}
				}
				Ok(ctx)
			}
			None => Ok(ctx),
		}
	}
}
