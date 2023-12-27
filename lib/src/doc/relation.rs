use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn relation(
		&mut self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		let tb = self.tb(opt, txn).await?;
		let rid = self.id.as_ref().unwrap();
		match stm {
			Statement::Create(_) | Statement::Insert(_) => {
				if tb.relation {
					return Err(Error::TableCheck {
						thing: rid.to_string(),
						relation: false,
					});
				}
			}
			Statement::Relate(_) => {
				if !tb.relation {
					return Err(Error::TableCheck {
						thing: rid.to_string(),
						relation: true,
					});
				}
			}
			_ => {}
		}
		// Carry on
		Ok(())
	}
}
