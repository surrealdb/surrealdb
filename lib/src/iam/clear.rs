use crate::dbs::Auth;
use crate::dbs::Session;
use crate::err::Error;
use std::sync::Arc;

pub fn clear(session: &mut Session) -> Result<(), Error> {
	session.au = Arc::new(Auth::No);
	session.tk = None;
	session.sc = None;
	session.sd = None;
	Ok(())
}
