use std::sync::Arc;

use anyhow::Result;

use super::Auth;
use crate::dbs::Session;

pub fn clear(session: &mut Session) -> Result<()> {
	session.au = Arc::new(Auth::default());
	session.tk = None;
	session.ac = None;
	session.rd = None;
	Ok(())
}
