use super::Auth;
use crate::dbs::Session;
use anyhow::Result;
use std::sync::Arc;

pub fn clear(session: &mut Session) -> Result<()> {
	session.au = Arc::new(Auth::default());
	session.tk = None;
	session.ac = None;
	session.rd = None;
	Ok(())
}
