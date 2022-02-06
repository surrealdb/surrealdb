use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;

impl Document {
	pub async fn erase(
		&mut self,
		ctx: &Runtime,
		opt: &Options<'_>,
		exe: &Executor<'_>,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		self.current.clear(ctx, opt, exe).await
	}
}
