use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::sql::idiom::Idiom;
use crate::sql::number::Number;
use crate::sql::value::Value;

impl Value {
	pub fn decrement(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		exe: &mut Executor,
		path: &Idiom,
		val: Value,
	) {
		match self.get(ctx, opt, exe, path) {
			Value::Number(v) => match val {
				Value::Number(x) => self.set(ctx, opt, exe, path, Value::from(v - x)),
				_ => (),
			},
			Value::Array(v) => match val {
				Value::Array(x) => self.set(ctx, opt, exe, path, Value::from(v - x)),
				x => self.set(ctx, opt, exe, path, Value::from(v - x)),
			},
			Value::None => match val {
				Value::Number(x) => self.set(ctx, opt, exe, path, Value::from(Number::from(0) - x)),
				_ => (),
			},
			_ => (),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;

	#[test]
	fn dec_none() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("other");
		let mut val = Value::parse("{ test: 100 }");
		let res = Value::parse("{ test: 100, other: -10 }");
		val.decrement(&ctx, &opt, &mut exe, &idi, Value::from(10));
		assert_eq!(res, val);
	}

	#[test]
	fn dec_number() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: 100 }");
		let res = Value::parse("{ test: 90 }");
		val.decrement(&ctx, &opt, &mut exe, &idi, Value::from(10));
		assert_eq!(res, val);
	}

	#[test]
	fn dec_array_number() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test[1]");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 190, 300] }");
		val.decrement(&ctx, &opt, &mut exe, &idi, Value::from(10));
		assert_eq!(res, val);
	}

	#[test]
	fn dec_array_value() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [100, 300] }");
		val.decrement(&ctx, &opt, &mut exe, &idi, Value::from(200));
		assert_eq!(res, val);
	}

	#[test]
	fn dec_array_array() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: [100, 200, 300] }");
		let res = Value::parse("{ test: [200] }");
		val.decrement(&ctx, &opt, &mut exe, &idi, Value::parse("[100, 300]"));
		assert_eq!(res, val);
	}
}
