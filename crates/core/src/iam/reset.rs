use std::sync::Arc;

use super::Auth;
use crate::dbs::{Session, Variables};

pub fn reset(session: &mut Session) {
	session.au = Arc::new(Auth::default());
	session.tk = None;
	session.ac = None;
	session.rd = None;
	session.ns = None;
	session.db = None;
	session.variables = Variables::default();
}
