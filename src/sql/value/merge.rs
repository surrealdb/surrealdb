use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::object::Object;
use crate::sql::value::Value;

impl Value {
	pub async fn merge(
		&mut self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &Executor<'_>,
		val: &Object,
	) -> Result<(), Error> {
		match val.compute(ctx, opt, exe, Some(self)).await? {
			Value::Object(v) => {
				for (k, v) in v.value.into_iter() {
					self.set(ctx, opt, exe, &k.into(), v).await?;
				}
				Ok(())
			}
			_ => unreachable!(),
		}
	}
}
