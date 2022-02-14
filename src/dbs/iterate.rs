use crate::cnf::ID_CHARS;
use crate::dbs::Executor;
use crate::dbs::Iterator;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::array::Array;
use crate::sql::model::Model;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use async_recursion::async_recursion;
use nanoid::nanoid;

impl Value {
	#[async_recursion]
	pub async fn iterate(
		self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		ite: &mut Iterator<'_>,
	) -> Result<(), Error> {
		match self {
			Value::Array(v) => v.iterate(ctx, opt, exe, ite).await?,
			Value::Model(v) => v.iterate(ctx, opt, exe, ite).await?,
			Value::Thing(v) => v.iterate(ctx, opt, exe, ite).await?,
			Value::Table(v) => v.iterate(ctx, opt, exe, ite).await?,
			v => ite.process(ctx, opt, exe, None, v).await,
		}
		Ok(())
	}
}

impl Array {
	#[async_recursion]
	pub async fn iterate(
		self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		ite: &mut Iterator<'_>,
	) -> Result<(), Error> {
		for v in self.value.into_iter() {
			match v {
				Value::Array(v) => v.iterate(ctx, opt, exe, ite).await?,
				Value::Model(v) => v.iterate(ctx, opt, exe, ite).await?,
				Value::Thing(v) => v.iterate(ctx, opt, exe, ite).await?,
				Value::Table(v) => v.iterate(ctx, opt, exe, ite).await?,
				v => ite.process(ctx, opt, exe, None, v).await,
			}
		}
		Ok(())
	}
}

impl Model {
	pub async fn iterate(
		self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		ite: &mut Iterator<'_>,
	) -> Result<(), Error> {
		if ctx.is_ok() {
			if let Some(c) = self.count {
				for _ in 0..c {
					Thing {
						tb: self.table.to_string(),
						id: nanoid!(20, &ID_CHARS),
					}
					.iterate(ctx, opt, exe, ite)
					.await?;
				}
			}
			if let Some(r) = self.range {
				for x in r.0..r.1 {
					Thing {
						tb: self.table.to_string(),
						id: x.to_string(),
					}
					.iterate(ctx, opt, exe, ite)
					.await?;
				}
			}
		}
		Ok(())
	}
}

impl Thing {
	pub async fn iterate(
		self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		ite: &mut Iterator<'_>,
	) -> Result<(), Error> {
		todo!()
	}
}

impl Table {
	pub async fn iterate(
		self,
		ctx: &Runtime,
		opt: &Options,
		exe: &Executor<'_>,
		ite: &mut Iterator<'_>,
	) -> Result<(), Error> {
		todo!()
	}
}
