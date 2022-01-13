use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::sql::idiom::Idiom;
use crate::sql::part::Part;
use crate::sql::value::Value;

impl Value {
	pub fn last(&self, ctx: &Runtime, opt: &Options, exe: &mut Executor) -> Self {
		self.get(ctx, opt, exe, &Idiom::from(vec![Part::Last]))
	}
}
