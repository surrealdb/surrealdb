use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::field::Field;
use crate::sql::idiom::Idiom;
use crate::sql::output::Output;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn pluck(
		&self,
		ctx: &Runtime,
		opt: &Options,
		txn: &Transaction,
		stm: &Statement,
	) -> Result<Value, Error> {
		// Extract statement clause
		let expr = match stm {
			Statement::Select(_) => None,
			Statement::Create(stm) => stm.output.as_ref().or(Some(&Output::After)),
			Statement::Update(stm) => stm.output.as_ref().or(Some(&Output::After)),
			Statement::Relate(stm) => stm.output.as_ref().or(Some(&Output::After)),
			Statement::Delete(stm) => stm.output.as_ref().or(Some(&Output::None)),
			Statement::Insert(stm) => stm.output.as_ref().or(Some(&Output::After)),
			_ => unreachable!(),
		};
		// Ensure futures are run
		let opt = &opt.futures(true);
		// Match clause
		match expr {
			Some(v) => match v {
				Output::None => Err(Error::Ignore),
				Output::Null => Ok(Value::Null),
				Output::Diff => Ok(self.initial.diff(&self.current, Idiom::default()).into()),
				Output::After => self.current.compute(ctx, opt, txn, Some(&self.current)).await,
				Output::Before => self.initial.compute(ctx, opt, txn, Some(&self.initial)).await,
				Output::Fields(v) => {
					let mut out = match v.all() {
						true => self.current.compute(ctx, opt, txn, Some(&self.current)).await?,
						false => Value::base(),
					};
					for v in v.other() {
						match v {
							Field::All => (),
							Field::Alone(v) => {
								let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
								out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
							}
							Field::Alias(v, i) => {
								let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
								out.set(ctx, opt, txn, i, x).await?;
							}
						}
					}
					Ok(out)
				}
			},
			None => match stm {
				Statement::Select(stm) => {
					let mut out = match stm.expr.all() {
						true => self.current.compute(ctx, opt, txn, Some(&self.current)).await?,
						false => Value::base(),
					};
					for v in stm.expr.other() {
						match v {
							Field::All => (),
							Field::Alone(v) => match v {
								Value::Function(f) if stm.group.is_some() && f.is_aggregate() => {
									let x = match f.args().len() {
										0 => f.compute(ctx, opt, txn, Some(&self.current)).await?,
										_ => {
											f.args()[0]
												.compute(ctx, opt, txn, Some(&self.current))
												.await?
										}
									};
									out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
								}
								_ => {
									let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
									out.set(ctx, opt, txn, v.to_idiom().as_ref(), x).await?;
								}
							},
							Field::Alias(v, i) => match v {
								Value::Function(f) if stm.group.is_some() && f.is_aggregate() => {
									let x = match f.args().len() {
										0 => f.compute(ctx, opt, txn, Some(&self.current)).await?,
										_ => {
											f.args()[0]
												.compute(ctx, opt, txn, Some(&self.current))
												.await?
										}
									};
									out.set(ctx, opt, txn, i, x).await?;
								}
								_ => {
									let x = v.compute(ctx, opt, txn, Some(&self.current)).await?;
									out.set(ctx, opt, txn, i, x).await?;
								}
							},
						}
					}
					Ok(out)
				}
				_ => unreachable!(),
			},
		}
	}
}
