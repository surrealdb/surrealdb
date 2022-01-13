use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::sql::array::Array;
use crate::sql::idiom::Idiom;
use crate::sql::value::Value;

impl Value {
	pub fn array(&mut self, ctx: &Runtime, opt: &Options, exe: &mut Executor, path: &Idiom) {
		let val = Value::from(Array::default());
		self.set(ctx, opt, exe, path, Value::from(val))
	}
}
