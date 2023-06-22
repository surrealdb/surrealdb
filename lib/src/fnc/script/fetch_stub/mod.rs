/// The stub implementations for the fetch API when `http` is not enabled.
use js::{bind, prelude::*, Ctx, Exception, Result};

#[cfg(test)]
mod test;

/// Register the fetch types in the context.
pub fn register(ctx: Ctx<'_>) -> Result<()> {
	let globals = ctx.globals();
	globals.init_def::<Fetch>()?;

	globals.init_def::<Response>()?;
	globals.init_def::<Request>()?;
	globals.init_def::<Blob>()?;
	globals.init_def::<FormData>()?;
	globals.init_def::<Headers>()?;

	Ok(())
}

#[bind(object, public)]
#[allow(unused_variables)]
fn fetch<'js>(ctx: Ctx<'js>, args: Rest<()>) -> Result<()> {
	Err(Exception::throw_internal(ctx,"The 'fetch' function is not available in this build of SurrealDB. In order to use 'fetch', enable the 'http' feature."))
}

macro_rules! impl_stub_class {
	($($module:ident::$name:ident),*) => {

		$(

			#[js::bind(object, public)]
			#[quickjs(bare)]
			#[allow(non_snake_case)]
			#[allow(unused_variables)]
			#[allow(clippy::module_inception)]
			pub mod $module{
				use js::{function::Rest, Ctx, Exception, Result};

				pub struct $name;
				impl $name {
					#[quickjs(constructor)]
					pub fn new(ctx: Ctx, _arg: Rest<()>) -> Result<Self> {
						Err(Exception::throw_internal(
								ctx,
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
