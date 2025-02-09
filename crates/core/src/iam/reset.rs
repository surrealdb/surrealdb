use super::Auth;
use crate::dbs::Session;
use crate::err::Error;
use std::collections::BTreeMap;
use std::sync::Arc;

pub fn reset(session: &mut Session) -> Result<(), Error> {
	session.au = Arc::new(Auth::default());
	session.tk = None;
	session.ac = None;
	session.rd = None;
	session.ns = None;
	session.db = None;
	session.parameters = BTreeMap::new();
	Ok(())
}
