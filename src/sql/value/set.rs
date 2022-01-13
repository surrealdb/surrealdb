use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Process;
use crate::dbs::Runtime;
use crate::sql::idiom::Idiom;
use crate::sql::object::Object;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub fn set(
		&mut self,
		ctx: &Runtime,
		opt: &Options,
		exe: &mut Executor,
		path: &Idiom,
		val: Value,
	) {
		match path.parts.first() {
			// Get the current path part
			Some(p) => match self {
				// Current path part is an object
				Value::Object(v) => match p {
					Part::Field(p) => match v.value.get_mut(&p.name) {
						Some(v) if v.is_some() => v.set(ctx, opt, exe, &path.next(), val),
						_ => {
							let mut obj = Value::from(Object::default());
							obj.set(ctx, opt, exe, &path.next(), val);
							v.insert(&p.name, obj)
						}
					},
					_ => (),
				},
				// Current path part is an array
				Value::Array(v) => match p {
					Part::All => v
						.value
						.iter_mut()
						.for_each(|v| v.set(ctx, opt, exe, &path.next(), val.clone())),
					Part::First => match v.value.first_mut() {
						Some(v) => v.set(ctx, opt, exe, &path.next(), val),
						None => (),
					},
					Part::Last => match v.value.last_mut() {
						Some(v) => v.set(ctx, opt, exe, &path.next(), val),
						None => (),
					},
					Part::Index(i) => match v.value.get_mut(i.to_usize()) {
						Some(v) => v.set(ctx, opt, exe, &path.next(), val),
						None => (),
					},
					Part::Where(w) => {
						v.value.iter_mut().for_each(|v| match w.process(ctx, opt, exe, Some(v)) {
							Ok(mut v) if v.is_truthy() => {
								v.set(ctx, opt, exe, &path.next(), val.clone())
							}
							_ => (),
						})
					}
					_ => (),
				},
				// Ignore everything else
				_ => (),
			},
			// No more parts so set the value
			None => *self = val.clone(),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::sql::test::Parse;

	#[test]
	fn set_none() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom {
			parts: vec![],
		};
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("999");
		val.set(&ctx, &opt, &mut exe, &idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[test]
	fn set_reset() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: 999 }");
		val.set(&ctx, &opt, &mut exe, &idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[test]
	fn set_basic() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 999 } }");
		val.set(&ctx, &opt, &mut exe, &idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[test]
	fn set_wrong() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something.wrong");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: null, something: 123 } }");
		val.set(&ctx, &opt, &mut exe, &idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[test]
	fn set_other() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.other.something");
		let mut val = Value::parse("{ test: { other: null, something: 123 } }");
		let res = Value::parse("{ test: { other: { something: 999 }, something: 123 } }");
		val.set(&ctx, &opt, &mut exe, &idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[test]
	fn set_array() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something[1]");
		let mut val = Value::parse("{ test: { something: [123, 456, 789] } }");
		let res = Value::parse("{ test: { something: [123, 999, 789] } }");
		val.set(&ctx, &opt, &mut exe, &idi, Value::from(999));
		assert_eq!(res, val);
	}

	#[test]
	fn set_array_field() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something[1].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 34 }, { age: 21 }] } }");
		val.set(&ctx, &opt, &mut exe, &idi, Value::from(21));
		assert_eq!(res, val);
	}

	#[test]
	fn set_array_fields() {
		let (ctx, opt, mut exe) = mock();
		let idi = Idiom::parse("test.something[*].age");
		let mut val = Value::parse("{ test: { something: [{ age: 34 }, { age: 36 }] } }");
		let res = Value::parse("{ test: { something: [{ age: 21 }, { age: 21 }] } }");
		val.set(&ctx, &opt, &mut exe, &idi, Value::from(21));
		assert_eq!(res, val);
	}
}
