use std::string::ToString;

#[derive(Debug, Copy, Clone)]
pub enum Format {
	Json,
}

impl ToString for Format {
	fn to_string(&self) -> String {
		match self {
			Self::Json => "json".to_owned(),
		}
	}
}
