use crate::err::Error;
use crate::exec::Error as ExecError;

impl From<js::CaughtError<'_>> for Error {
	fn from(e: js::CaughtError) -> Error {
		match e {
			js::CaughtError::Exception(e) => ExecError::InvalidScript {
				message: format!(
					"An exception occurred: {}{}",
					e.message().unwrap_or_default(),
					match e.stack() {
						Some(stack) => format!("\n{stack}"),
						None => String::default(),
					}
				),
			},
			js::CaughtError::Error(js::Error::Unknown) => ExecError::InvalidScript {
				message: "An unknown error occurred".to_string(),
			},
			_ => ExecError::InvalidScript {
				message: e.to_string(),
			},
		}
		.into()
	}
}
