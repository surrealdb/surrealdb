use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::val::Value;

/// Return the next value for a given sequence.
pub async fn nextval((ctx, opt): (&Context, &Options), (seq,): (Value,)) -> Result<Value> {
	if let Value::Strand(s) = seq {
		let next = ctx.try_get_sequences()?.next_val_user(ctx, opt, &s).await?;
		Ok(next.into())
	} else {
		Err(anyhow::Error::new(Error::InvalidArguments {
			name: "sequence::nextval()".to_string(),
			message: "Expect a sequence name".to_string(),
		}))
	}
}
