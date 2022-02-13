use crate::dbs::Executor;
use crate::dbs::Options;
use crate::dbs::Runtime;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn empty(
		&self,
		_ctx: &Runtime,
		_opt: &Options,
		_exe: &Executor<'_>,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		match self.id.is_some() && self.current.is_none() {
			true => Err(Error::IgnoreError),
			false => Ok(()),
		}
	}
}
