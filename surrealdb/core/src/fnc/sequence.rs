use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Error;
use crate::val::Value;

/// Return the next value for a given sequence.
pub async fn nextval((ctx, opt): (&FrozenContext, &Options), (seq,): (Value,)) -> Result<Value> {
	if let Value::String(s) = seq {
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let sequences = ctx.try_get_sequences()?;
		let txn = ctx.tx();
		let next = crate::exec::vm::descriptor::object(sequences.next_user_sequence_id(
			Some(ctx),
			&txn,
			ns,
			db,
			&s,
		))
		.await?;
		Ok(next.into())
	} else {
		Err(anyhow::Error::new(Error::InvalidFunctionArguments {
			name: "sequence::nextval()".to_string(),
			message: "Expect a sequence name".to_string(),
		}))
	}
}
