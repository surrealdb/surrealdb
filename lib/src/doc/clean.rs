use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::idiom::Idiom;

impl<'a> Document<'a> {
	pub async fn clean(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the table
		let tb = self.tb(opt, txn).await?;
		// This table is schemafull
		if tb.full {
			// Create a vector to store the keys
			let mut keys: Vec<Idiom> = vec![];
			// Loop through all field statements
			for fd in self.fd(opt, txn).await?.iter() {
				// Is this a schemaless field?
				match fd.flex {
					false => {
						// Loop over this field in the document
						for k in self.current.each(&fd.name).into_iter() {
							keys.push(k);
						}
					}
					true => {
						// Loop over every field under this field in the document
						for k in self.current.every(Some(&fd.name), true, true).into_iter() {
							keys.push(k);
						}
					}
				}
			}
			// Loop over every field in the document
			for fd in self.current.every(None, true, true).iter() {
				if !keys.contains(fd) {
					match fd {
						fd if fd.is_id() => continue,
						fd if fd.is_in() => continue,
						fd if fd.is_out() => continue,
						fd if fd.is_meta() => continue,
						fd => self.current.to_mut().del(ctx, opt, txn, fd).await?,
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}
