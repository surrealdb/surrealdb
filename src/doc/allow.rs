use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;

impl Document {
	pub async fn allow(
		&self,
		_ctx: &Runtime,
		_opt: &Options<'_>,
		_exe: &Executor<'_>,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		Ok(())
	}
}
