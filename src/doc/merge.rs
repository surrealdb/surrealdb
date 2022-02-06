use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::data::Data;
use crate::sql::operator::Operator;
use crate::sql::value::Value;

impl Document {
	pub async fn merge(
		&mut self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &Executor<'_>,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the ID reference
		let id = self.id.as_ref();
		// Extract statement clause
		let data = match stm {
			Statement::Create(stm) => stm.data.as_ref(),
			Statement::Update(stm) => stm.data.as_ref(),
			_ => unreachable!(),
		};
		// Set default field values
		self.current.def(ctx, opt, exe, id).await?;
		// Check for a data clause
		match data {
			// The statement has a data clause
			Some(v) => match v {
				Data::SetExpression(x) => {
					for x in x.iter() {
						let v = x.2.compute(ctx, opt, exe, Some(&self.current)).await?;
						match x.1 {
							Operator::Equal => match v {
								Value::Void => self.current.del(ctx, opt, exe, &x.0).await?,
								_ => self.current.set(ctx, opt, exe, &x.0, v).await?,
							},
							Operator::Inc => self.current.increment(ctx, opt, exe, &x.0, v).await?,
							Operator::Dec => self.current.decrement(ctx, opt, exe, &x.0, v).await?,
							_ => unreachable!(),
						}
					}
				}
				Data::PatchExpression(v) => self.current.patch(ctx, opt, exe, v).await?,
				Data::MergeExpression(v) => self.current.merge(ctx, opt, exe, v).await?,
				Data::ReplaceExpression(v) => self.current.replace(ctx, opt, exe, v).await?,
				Data::ContentExpression(v) => self.current.replace(ctx, opt, exe, v).await?,
				_ => unreachable!(),
			},
			// No data clause has been set
			None => (),
		};
		// Set default field values
		self.current.def(ctx, opt, exe, id).await?;
		// Set ASSERT and VALUE clauses
		// todo!();
		// Delete non-defined FIELDs
		// todo!();
		// Carry on
		Ok(())
	}
}
