use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn admit(
		&self,
		_ctx: &Runtime,
		_opt: &Options,
		_txn: &Transaction,
		stm: &Statement,
	) -> Result<(), Error> {
		// Check that we are altering a record
		if self.id.is_none() {
			return match stm {
				Statement::Create(_) => Err(Error::CreateStatement {
					value: self.initial.to_string(),
				}),
				Statement::Update(_) => Err(Error::UpdateStatement {
					value: self.initial.to_string(),
				}),
				Statement::Relate(_) => Err(Error::RelateStatement {
					value: self.initial.to_string(),
				}),
				Statement::Delete(_) => Err(Error::DeleteStatement {
					value: self.initial.to_string(),
				}),
				Statement::Insert(_) => Err(Error::InsertStatement {
					value: self.initial.to_string(),
				}),
				_ => unreachable!(),
			};
		}
		// Carry on
		Ok(())
	}
}
