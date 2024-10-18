use crate::err::Error;

impl From<js::CaughtError<'_>> for Error {
	fn from(e: js::CaughtError) -> Error {
		match e {
			js::CaughtError::Exception(e) => {
				let line = e.line().unwrap_or(-1);
				Error::InvalidScript {
					message: format!(
						"An exception occurred{}: {}{}",
						match e.file() {
							Some(file) => format!(" at {file}:{line}"),
							None => String::default(),
						},
						e.message().unwrap_or_default(),
						match e.stack() {
							Some(stack) => format!("\n{stack}"),
							None => String::default(),
						}
					),
				}
			}
			js::CaughtError::Error(js::Error::Unknown) => Error::InvalidScript {
				message: "An unknown error occurred".to_string(),
			},
			_ => Error::InvalidScript {
				message: e.to_string(),
			},
		}
	}
}
