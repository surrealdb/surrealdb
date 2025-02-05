use crate::{api::context::RequestContext, err::Error, fnc::args, sql::Value};

use super::api;

pub trait InvokeMiddleware<'a> {
	fn invoke(self, context: &'a mut RequestContext) -> Result<(), Error>;
}

macro_rules! dispatch {
	($name: ident, $args: expr, $context: expr, $($function_name: literal => $(($wrapper: tt))* $($function_path: ident)::+,)+) => {
		{
			match $name {
				$($function_name => {
					let args = args::FromArgs::from_args($name, $args)?;
					#[allow(clippy::redundant_closure_call)]
					$($wrapper)*(|| $($function_path)::+($context, args))()
				},)+
				_ => {
					Err($crate::err::Error::InvalidFunction{
						name: String::from($name),
						message: "unknown middleware".to_string()
					})
				}
			}
		}
	};
}

impl<'a> InvokeMiddleware<'a> for (&'a String, &'a Vec<Value>) {
	fn invoke(self, context: &'a mut RequestContext) -> Result<(), Error> {
		let name = self.0.as_str();

		dispatch!(
			name,
			self.1.to_owned(),
			context,
			//
			"api::body::max_size" => api::body::max_size,
			"api::header" => api::header,
			"api::headers" => api::headers,
			"api::timeout" => api::timeout,
		)
	}
}
