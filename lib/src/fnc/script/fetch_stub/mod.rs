//! stub implementations for the fetch API when `http` is not enabled.

use js::{class::Trace, Class, Ctx, Exception, Function, Result};

#[cfg(test)]
mod test;

/// Register the fetch types in the context.
pub fn register(ctx: &Ctx<'_>) -> Result<()> {
	let globals = ctx.globals();
	Class::<response::Response>::define(&globals)?;
	Class::<request::Request>::define(&globals)?;
	Class::<blob::Blob>::define(&globals)?;
	Class::<form_data::FormData>::define(&globals)?;
	Class::<headers::Headers>::define(&globals)?;
	globals.set("fetch", Function::new(ctx.clone(), js_fetch)?.with_name("fetch")?)
}

#[js::function]
fn fetch<'js>(ctx: Ctx<'js>) -> Result<()> {
	Err(Exception::throw_internal(&ctx,"The 'fetch' function is not available in this build of SurrealDB. In order to use 'fetch', enable the 'http' feature."))
}

macro_rules! impl_stub_class {
	($($module:ident::$name:ident),*) => {

		$(

			mod $module{
				use super::*;

				#[js::class]
				#[derive(Trace)]
				pub struct $name;

				#[js::methods]
				impl $name {
					#[qjs(constructor)]
					pub fn new(ctx: Ctx<'_>) -> Result<Self> {
						Err(Exception::throw_internal(
							&ctx,
							concat!(
								"The '",
								stringify!($name),
								"' class is not available in this build of SurrealDB. In order to use '",
								stringify!($name),
								"', enable the 'http' feature."
							),
						))
					}
				}
			}
		)*
	};
}

impl_stub_class!(
	response::Response,
	request::Request,
	headers::Headers,
	blob::Blob,
	form_data::FormData
);
