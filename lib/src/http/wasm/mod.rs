pub type Error = js_sys::Error;

pub struct Client {}

impl Client {
	pub fn new() -> Result<Self, Error> {
		Ok(Self {})
	}

	pub fn build(builder: ClientBuilder) -> Result<Self, Error> {}
}
