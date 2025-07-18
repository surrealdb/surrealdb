use http::HeaderMap;

use crate::expr::Bytesize;
use crate::val::Duration;

#[derive(Default, Debug)]
pub struct InvocationContext {
	pub request_body_max: Option<Bytesize>,
	pub request_body_raw: bool,
	pub response_body_raw: bool,
	pub response_headers: Option<HeaderMap>,
	pub timeout: Option<Duration>,
}
