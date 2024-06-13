use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::key::key_req::KeyRequirements;

impl<'a> Document<'a> {
	pub async fn store(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Check if the table is a view
		if self.tb(ctx, opt).await?.drop {
			return Ok(());
		}
		// Claim transaction
		let mut run = ctx.tx_lock().await;
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Store the record data
		let key = crate::key::thing::new(opt.ns()?, opt.db()?, &rid.tb, &rid.id);
		//
		match stm {
			// This is a CREATE statement so try to insert the key
			Statement::Create(_) => match run.put(key.key_category(), key, self).await {
				// The key already exists, so return an error
				Err(Error::TxKeyAlreadyExistsCategory(_)) => Err(Error::RecordExists {
					thing: rid.to_string(),
				}),
				// Return any other received error
				Err(e) => Err(e),
				// Record creation worked fine
				Ok(v) => Ok(v),
			},
			// This is not a CREATE statement, so update the key
			_ => run.set(key, self).await,
		}?;
		// Carry on
		Ok(())
	}
}
