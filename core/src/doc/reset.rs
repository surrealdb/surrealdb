use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn reset(
		&mut self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Set default field values
		self.current.doc.to_mut().def(rid);
		// This is a RELATE statement, so reset fields
		if let Workable::Relate(l, r) = &self.extras {
			self.current.doc.to_mut().put(&*EDGE, Value::Bool(true));
			self.current.doc.to_mut().put(&*IN, l.clone().into());
			self.current.doc.to_mut().put(&*OUT, r.clone().into());
		}
		// This is an UPDATE of a graph edge, so reset fields
		if self.initial.doc.pick(&*EDGE).is_true() {
			self.current.doc.to_mut().put(&*EDGE, Value::Bool(true));
			self.current.doc.to_mut().put(&*IN, self.initial.doc.pick(&*IN));
			self.current.doc.to_mut().put(&*OUT, self.initial.doc.pick(&*OUT));
		}
		// Carry on
		Ok(())
	}
}
