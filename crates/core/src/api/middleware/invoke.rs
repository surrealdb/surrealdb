use crate::{api::context::InvocationContext, err::Error, fnc::args, sql::Value};

use super::api;

pub trait InvokeMiddleware<'a> {
	fn invoke(self, context: &'a mut InvocationContext) -> Result<(), Error>;
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
	fn invoke(self, context: &'a mut InvocationContext) -> Result<(), Error> {
		let name = self.0.as_str();

		dispatch!(
			name,
			self.1.to_owned(),
			context,
			//
			"api::req::max_body" => api::req::max_body,
			"api::req::raw_body" => api::req::raw_body,
			//
			"api::res::raw_body" => api::res::raw_body,
			"api::res::headers" => api::res::headers,
			"api::res::header" => api::res::header,
			//
			"api::timeout" => api::timeout,
		)
	}
}
