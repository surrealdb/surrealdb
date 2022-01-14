use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::sql::idiom::Idiom;
use crate::sql::object::Object;
use crate::sql::value::Value;

impl Value {
	pub async fn object(
		&mut self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &mut Executor,
		path: &Idiom,
	) {
		let val = Value::from(Object::default());
		self.set(ctx, opt, exe, path, val).await
	}
}
