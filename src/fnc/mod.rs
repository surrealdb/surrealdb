use crate::ctx::Parent;
use crate::err::Error;
use crate::sql::literal::Literal;

pub mod cast;
pub mod future;
pub mod operate;

pub fn run(ctx: &Parent, name: &String, args: Vec<Literal>) -> Result<Literal, Error> {
	todo!()
}
