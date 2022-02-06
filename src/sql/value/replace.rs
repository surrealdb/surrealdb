use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::object::Object;
use crate::sql::value::Value;

impl Value {
	pub async fn replace(
		&mut self,
		_ctx: &Runtime,
		_opt: &Options,
		_exe: &Executor<'_>,
		val: &Object,
	) -> Result<(), Error> {
		// Clear all entries
		*self = Value::from(val.clone());
		Ok(())
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;

	#[tokio::test]
	async fn replace() {
		let (ctx, opt, exe) = mock();
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ other: true }");
		let obj = Object::from(map! {String::from("other") => Value::from(true) });
		val.replace(&ctx, &opt, &exe, &obj).await.unwrap();
		assert_eq!(res, val);
	}
}
