use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::operation::Operations;
use crate::sql::value::Value;

impl Value {
	pub async fn patch(
		&self,
		_ctx: &Runtime,
		_opt: &Options<'_>,
		_exe: &mut Executor,
		ops: Operations,
	) -> Result<(), Error> {
		Ok(())
	}
}
