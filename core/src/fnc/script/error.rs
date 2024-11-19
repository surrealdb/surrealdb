use crate::err::Error;

impl From<js::CaughtError<'_>> for Error {
	fn from(e: js::CaughtError) -> Error {
		match e {
			js::CaughtError::Exception(e) => Error::InvalidScript {
				message: format!(
					"An exception occurred: {}{}",
					e.message().unwrap_or_default(),
					match e.stack() {
						Some(stack) => format!("\n{stack}"),
						None => String::default(),
					}
				),
			},
			js::CaughtError::Error(js::Error::Unknown) => Error::InvalidScript {
				message: "An unknown error occurred".to_string(),
			},
			_ => Error::InvalidScript {
				message: e.to_string(),
			},
		}
	}
}
