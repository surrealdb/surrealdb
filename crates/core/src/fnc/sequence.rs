use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::Value;

/// Return the next value for a given sequence.
pub async fn nextval((ctx, opt): (&Context, &Options), (seq,): (Value,)) -> Result<Value, Error> {
	if let Some(sqs) = ctx.get_sequences() {
		if let Value::Strand(s) = seq {
			let next = sqs.next_val(ctx, opt, &s).await?;
			Ok(next.into())
		} else {
			Err(Error::InvalidArguments {
				name: "sequence::nextval()".to_string(),
				message: "Expect a sequence name".to_string(),
			})
		}
	} else {
		Err(Error::Internal("Sequences are not supported in this context.".to_string()))?
	}
}
