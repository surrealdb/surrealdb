use crate::err::Error;

impl From<js::CaughtError<'_>> for Error {
	fn from(e: js::CaughtError) -> Error {
		match e {
			js::CaughtError::Exception(e) => {
				let line = e.line().unwrap_or(-1);
				Error::InvalidScript {
					message: format!(
						"An exception occurred{}: {}{}",
						e.file().map(|file| format!(" at {file}:{line}")).unwrap_or_default(),
						e.message().unwrap_or_default(),
						e.stack().map(|stack| format!("\n{stack}")).unwrap_or_default()
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
