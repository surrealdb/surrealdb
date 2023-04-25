use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::value::Value;

impl Value {
	pub(crate) async fn merge(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		val: Value,
	) -> Result<(), Error> {
		match val {
			v if v.is_object() => {
				for k in v.every(None, false, false).iter() {
					match v.get(ctx, opt, txn, &k.0).await? {
						Value::None => self.del(ctx, opt, txn, &k.0).await?,
						v => self.set(ctx, opt, txn, &k.0, v).await?,
					}
				}
				Ok(())
			}
			_ => Ok(()),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn merge_none() {
		let (ctx, opt, txn) = mock().await;
		let mut res = Value::parse(
			"{
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		);
		let mrg = Value::None;
		let val = Value::parse(
			"{
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		);
		res.merge(&ctx, &opt, &txn, mrg).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn merge_basic() {
		let (ctx, opt, txn) = mock().await;
		let mut res = Value::parse(
			"{
				name: {
					first: 'Tobie',
					last: 'Morgan Hitchcock',
					initials: 'TMH',
				},
			}",
		);
		let mrg = Value::parse(
			"{
				name: {
					title: 'Mr',
					initials: NONE,
				},
				tags: ['Rust', 'Golang', 'JavaScript'],
			}",
		);
		let val = Value::parse(
			"{
				name: {
					title: 'Mr',
					first: 'Tobie',
					last: 'Morgan Hitchcock',
				},
				tags: ['Rust', 'Golang', 'JavaScript'],
			}",
		);
		res.merge(&ctx, &opt, &txn, mrg).await.unwrap();
		assert_eq!(res, val);
	}
}
