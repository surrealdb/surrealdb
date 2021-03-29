use crate::ctx::Parent;
use crate::dbs::executor::Executor;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::literal::Literal;

pub trait Process {
	fn process(
		&self,
		ctx: &Parent,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error>;
}
