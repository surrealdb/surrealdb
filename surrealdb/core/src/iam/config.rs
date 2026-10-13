//! How much of an authentication failure the client is told about.
//!
//! Authentication normally answers with one opaque error whatever went wrong, so
//! a caller cannot tell a wrong password from a missing record, a failing
//! `SIGNIN`/`SIGNUP` clause from a rejecting `AUTHENTICATE` clause. This layer
//! owns the knob that widens that answer because it is where every such failure
//! is turned into the error the client receives.

use surrealdb_cnf as cnf;

/// Error-reporting behaviour of the authentication paths.
#[derive(Clone, Debug, Default)]
pub(crate) struct IamConfig {
	/// Forward all authentication errors to the client. Do not use in production
	/// (default: false)
	pub insecure_forward_access_errors: bool,
}

impl cnf::Config for IamConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("insecure_forward_access_errors", &mut self.insecure_forward_access_errors);
	}
}
