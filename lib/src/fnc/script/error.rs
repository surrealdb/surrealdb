use crate::err::Error;

impl From<js::Error> for Error {
	fn from(e: js::Error) -> Error {
		match e {
			js::Error::Exception {
				message,
				stack,
				file,
				line,
			} => Error::InvalidScript {
				message: format!(
					"An exception occurred{}: {}{}",
					match file.is_empty() {
						false => format!(" at {}:{}", file, line),
						true => String::default(),
					},
					match message.is_empty() {
						false => message,
						true => String::default(),
					},
					match stack.is_empty() {
						false => format!("\n{}", stack),
						true => String::default(),
					}
				),
			},
			js::Error::Unknown => Error::InvalidScript {
				message: "An unknown error occurred".to_string(),
			},
			_ => Error::InvalidScript {
				message: e.to_string(),
			},
		}
	}
}

#[macro_export]
macro_rules! throw_js_exception {
	($e:ident) => {
		js::Error::Exception {
			line: line!() as i32,
			message: $e.to_string(),
			file: file!().to_owned(),
			stack: "".to_owned(),
		}
	};
	($str:expr) => {
		js::Error::Exception {
			line: line!() as i32,
			message: $str.to_owned(),
			file: file!().to_owned(),
			stack: "".to_owned(),
		}
	};
}
