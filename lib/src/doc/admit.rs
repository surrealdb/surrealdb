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
		match self.id {
			Some(_) => Ok(()),
			None => match stm {
				Statement::Create(_) => Err(Error::CreateStatement {
					value: (*self.initial).clone(),
				}),
				Statement::Update(_) => Err(Error::UpdateStatement {
					value: (*self.initial).clone(),
				}),
				Statement::Relate(_) => Err(Error::RelateStatement {
					value: (*self.initial).clone(),
				}),
				Statement::Delete(_) => Err(Error::DeleteStatement {
					value: (*self.initial).clone(),
				}),
				Statement::Insert(_) => Err(Error::InsertStatement {
					value: (*self.initial).clone(),
				}),
				_ => unreachable!(),
			},
		}
	}
}
