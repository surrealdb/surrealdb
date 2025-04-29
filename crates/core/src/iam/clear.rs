use std::sync::Arc;

use super::Auth;
use crate::dbs::Session;
use crate::err::Error;

pub fn clear(session: &mut Session) -> Result<(), Error> {
	session.au = Arc::new(Auth::default());
	session.tk = None;
	session.ac = None;
	session.rd = None;
	Ok(())
}
