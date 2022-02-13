use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn compute(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		stm: &Statement<'_>,
	) -> Result<Value, Error> {
		match stm {
			Statement::Select(_) => self.select(ctx, opt, exe, stm).await,
			Statement::Create(_) => self.create(ctx, opt, exe, stm).await,
			Statement::Update(_) => self.update(ctx, opt, exe, stm).await,
			Statement::Relate(_) => self.relate(ctx, opt, exe, stm).await,
			Statement::Delete(_) => self.delete(ctx, opt, exe, stm).await,
			Statement::Insert(_) => self.insert(ctx, opt, exe, stm).await,
			_ => unreachable!(),
		}
	}
}
