use crate::ctx::Parent;
use crate::dbs::Executor;
use crate::err::Error;
use crate::sql::literal::Literal;

pub struct Iterator {}

impl Iterator {
	pub fn new() -> Iterator {
		Iterator {}
	}
	pub fn process_query(&self, ctx: &Parent, exe: &Executor) {}
	pub fn process_table(&self, ctx: &Parent, exe: &Executor) {}
	pub fn process_thing(&self, ctx: &Parent, exe: &Executor) {}
	pub fn process_model(&self, ctx: &Parent, exe: &Executor) {}
	pub fn process_array(&self, ctx: &Parent, exe: &Executor) {}
	pub fn process_object(&self, ctx: &Parent, exe: &Executor) {}
	pub fn output(&self, ctx: &Parent, exe: &Executor) -> Result<Literal, Error> {
		Ok(Literal::Null)
	}
}
