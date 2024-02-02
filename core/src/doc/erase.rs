use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn erase(
		&mut self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		self.current.doc.to_mut().clear()
	}
}
