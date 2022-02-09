use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::value::Value;

impl Value {
	pub async fn clear(
		&mut self,
		_ctx: &Runtime,
		_opt: &Options,
		_exe: &Executor<'_>,
	) -> Result<(), Error> {
		*self = Value::base();
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn clear_none() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{}");
		val.clear(&ctx, &opt, &exe).await.unwrap();
		assert_eq!(res, val);
	}

	#[tokio::test]
	async fn clear_path() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{}");
		val.clear(&ctx, &opt, &exe).await.unwrap();
		assert_eq!(res, val);
	}
}
