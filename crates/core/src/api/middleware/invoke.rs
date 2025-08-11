use anyhow::Result;

use super::api;
use crate::api::context::InvocationContext;
use crate::fnc::args;
use crate::val::Value;

macro_rules! dispatch {
	($name: ident, $args: expr_2021, $context: expr_2021, $($function_name: literal => $(($wrapper: tt))* $($function_path: ident)::+,)+) => {
		{
			match $name {
				$($function_name => {
					let args = args::FromArgs::from_args($name, $args)?;
					#[expect(clippy::redundant_closure_call)]
					$($wrapper)*(|| $($function_path)::+($context, args))()
				},)+
				_ => {
					Err(::anyhow::Error::new($crate::err::Error::InvalidFunction{
						name: String::from($name),
						message: "unknown middleware".to_string()
					}))
				}
			}
		}
	};
}

pub fn invoke(context: &mut InvocationContext, name: &str, args: Vec<Value>) -> Result<()> {
	dispatch!(
		name,
		args,
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
