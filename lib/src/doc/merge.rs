use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::data::Data;
use crate::sql::operator::Operator;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn merge(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement,
	) -> Result<(), Error> {
		// Get the ID reference
		let id = self.id.as_ref();
		// Set default field values
		self.current.to_mut().def(ctx, opt, txn, id).await?;
		// The statement has a data clause
		if let Some(v) = stm.data() {
			match v {
				Data::SetExpression(x) => {
					for x in x.iter() {
						let v = x.2.compute(ctx, opt, txn, Some(&self.current)).await?;
						match x.1 {
							Operator::Equal => match v {
								Value::Void => {
									self.current.to_mut().del(ctx, opt, txn, &x.0).await?
								}
								_ => self.current.to_mut().set(ctx, opt, txn, &x.0, v).await?,
							},
							Operator::Inc => {
								self.current.to_mut().increment(ctx, opt, txn, &x.0, v).await?
							}
							Operator::Dec => {
								self.current.to_mut().decrement(ctx, opt, txn, &x.0, v).await?
							}
							_ => unreachable!(),
						}
					}
				}
				Data::PatchExpression(v) => self.current.to_mut().patch(ctx, opt, txn, v).await?,
				Data::MergeExpression(v) => self.current.to_mut().merge(ctx, opt, txn, v).await?,
				Data::ReplaceExpression(v) => {
					self.current.to_mut().replace(ctx, opt, txn, v).await?
				}
				Data::ContentExpression(v) => {
					self.current.to_mut().replace(ctx, opt, txn, v).await?
				}
				_ => unreachable!(),
			};
		};
		// Set default field values
		self.current.to_mut().def(ctx, opt, txn, id).await?;
		// Carry on
		Ok(())
	}
}
