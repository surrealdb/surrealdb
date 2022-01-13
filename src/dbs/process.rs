use crate::dbs::executor::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::value::Value;

pub trait Process {
	fn process(
		&self,
		ctx: &Runtime,
		opt: &Options,
		exe: &mut Executor,
		doc: Option<&Value>,
	) -> Result<Value, Error>;
}
