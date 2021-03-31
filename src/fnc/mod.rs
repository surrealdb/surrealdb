use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::literal::Literal;

pub mod cast;
pub mod future;
pub mod operate;

pub fn run(ctx: &Runtime, name: &String, args: Vec<Literal>) -> Result<Literal, Error> {
	todo!()
}
