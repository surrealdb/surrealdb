use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn exist(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if this record exists
		if let Some(id) = &self.id {
			// If there is a current value
			if self.current.is_some() {
				// The record already exists
				return Err(Error::RecordExists {
					thing: id.to_string(),
				});
			}
		}
		// Carry on
		Ok(())
	}
}
