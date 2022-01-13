use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::sql::idiom::Idiom;
use crate::sql::value::Value;

impl Value {
	pub fn fetch(self, _ctx: &Runtime, _opt: &Options, _exe: &mut Executor, _path: &Idiom) -> Self {
		self
	}
}
