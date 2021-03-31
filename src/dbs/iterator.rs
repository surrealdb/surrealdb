use crate::dbs::Executor;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::literal::Literal;

pub struct Iterator {}

impl Iterator {
	pub fn new() -> Iterator {
		Iterator {}
	}
	pub fn process_query(&self, ctx: &Runtime, exe: &Executor) {}
	pub fn process_table(&self, ctx: &Runtime, exe: &Executor) {}
	pub fn process_thing(&self, ctx: &Runtime, exe: &Executor) {}
	pub fn process_model(&self, ctx: &Runtime, exe: &Executor) {}
	pub fn process_array(&self, ctx: &Runtime, exe: &Executor) {}
	pub fn process_object(&self, ctx: &Runtime, exe: &Executor) {}
	pub fn output(&self, ctx: &Runtime, exe: &Executor) -> Result<Literal, Error> {
		Ok(Literal::Null)
	}
}
