use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::idiom::Idiom;
use reblessive::tree::Stk;

impl Document {
	pub async fn clean(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the table
		let tb = self.tb(ctx, opt).await?;
		// This table is schemafull
		if tb.full {
			// Create a vector to store the keys
			let mut keys: Vec<Idiom> = vec![];
			// Loop through all field statements
			for fd in self.fd(ctx, opt).await?.iter() {
				// Is this a schemaless field?
				match fd.flex {
					false => {
						// Loop over this field in the document
						for k in self.current.doc.as_ref().each(&fd.name).into_iter() {
							keys.push(k);
						}
					}
					true => {
						// Loop over every field under this field in the document
						for k in
							self.current.doc.as_ref().every(Some(&fd.name), true, true).into_iter()
						{
							keys.push(k);
						}
					}
				}
			}
			// Loop over every field in the document
			for fd in self.current.doc.as_ref().every(None, true, true).iter() {
				if !keys.contains(fd) {
					match fd {
						fd if fd.is_id() => continue,
						fd if fd.is_in() => continue,
						fd if fd.is_out() => continue,
						fd if fd.is_meta() => continue,
						fd => self.current.doc.to_mut().del(stk, ctx, opt, fd).await?,
					}
				}
			}
		}
		// Carry on
		Ok(())
	}
}
