use anyhow::Result;

pub struct HttpClient {}

impl HttpClient {
	pub fn new(capabilities: Arc<Capabilities>) -> Result<Self> {
		Ok(HttpClient {})
	}
}
