use std::sync::Arc;

use super::Auth;
use crate::dbs::Session;
use crate::types::PublicVariables;

pub fn reset(session: &mut Session) {
	session.au = Arc::new(Auth::default());
	session.tk = None;
	session.ac = None;
	session.rd = None;
	// SECURITY: the CONTEXT payload is an authorisation grant. A reset session
	// must be in the most restrictive state, so it cannot survive one.
	session.data = None;
	session.ns = None;
	session.db = None;
	session.variables = PublicVariables::default();
}
