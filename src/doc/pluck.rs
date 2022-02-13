use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
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
		exe: &Executor<'_>,
		stm: &Statement<'_>,
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
				Output::None => Err(Error::IgnoreError),
				Output::Null => Ok(Value::Null),
				Output::Diff => Ok(self.initial.diff(&self.current, Idiom::default()).into()),
				Output::After => self.current.compute(ctx, opt, exe, Some(&self.current)).await,
				Output::Before => self.initial.compute(ctx, opt, exe, Some(&self.initial)).await,
				Output::Fields(v) => {
					let mut out = match v.all() {
						true => self.current.compute(ctx, opt, exe, Some(&self.current)).await?,
						false => Value::base(),
					};
					for v in v.iter() {
						match v {
							Field::All => (),
							Field::Alone(v) => {
								let x = v.compute(ctx, opt, exe, Some(&self.current)).await?;
								out.set(ctx, opt, exe, &v.to_idiom(), x).await?;
							}
							Field::Alias(v, i) => {
								let x = v.compute(ctx, opt, exe, Some(&self.current)).await?;
								out.set(ctx, opt, exe, &i, x).await?;
							}
						}
					}
					Ok(out)
				}
			},
			None => match stm {
				Statement::Select(stm) => {
					let mut out = match stm.expr.all() {
						true => self.current.compute(ctx, opt, exe, Some(&self.current)).await?,
						false => Value::base(),
					};
					for v in stm.expr.iter() {
						match v {
							Field::All => (),
							Field::Alone(v) => {
								let x = v.compute(ctx, opt, exe, Some(&self.current)).await?;
								out.set(ctx, opt, exe, &v.to_idiom(), x).await?;
							}
							Field::Alias(v, i) => {
								let x = v.compute(ctx, opt, exe, Some(&self.current)).await?;
								out.set(ctx, opt, exe, &i, x).await?;
							}
						}
					}
					Ok(out)
				}
				_ => unreachable!(),
			},
		}
	}
}
