use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::MutableContext;
use crate::err::Error;
use crate::expr::value::Value;
use crate::protocol::FromFlatbuffers;
use anyhow::Result;
use std::collections::BTreeMap;

pub(crate) type Variables = Option<BTreeMap<String, Value>>;

pub(crate) trait Attach {
	fn attach(self, ctx: &mut MutableContext) -> Result<(), Error>;
}

impl Attach for Variables {
	fn attach(self, ctx: &mut MutableContext) -> Result<(), Error> {
		match self {
			Some(m) => {
				for (key, val) in m {
					// Check if the variable is a protected variable
					if PROTECTED_PARAM_NAMES.contains(&key.as_str()) {
						// The user tried to set a protected variable
						return Err(Error::InvalidParam {
							name: key,
						});
					}

					// The variable isn't protected and can be stored
					ctx.add_value(key, val.into());
				}
				Ok(())
			}
			None => Ok(()),
		}
	}
}

// impl FromFlatbuffers for Variables {

// }
