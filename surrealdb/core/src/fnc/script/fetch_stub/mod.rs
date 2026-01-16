//! stub implementations for the fetch API when `http` is not enabled.

use js::class::{JsClass, Trace, Tracer};
use js::function::Constructor;
use js::{Class, Ctx, Exception, Function, Object, Result};

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
	Err(Exception::throw_internal(
		&ctx,
		"The 'fetch' function is not available in this build of SurrealDB. In order to use 'fetch', enable the 'http' feature.",
	))
}

macro_rules! impl_stub_class {
	($($module:ident::$name:ident),*) => {

		$(

			mod $module{
				use super::*;

				pub struct $name;

				unsafe impl<'js> js::JsLifetime<'js> for $name{
					type Changed<'to> = $name;
				}

				impl<'js> Trace<'js> for $name{
					fn trace<'a>(&self, _tracer: Tracer<'a, 'js>){}
				}

				impl<'js> JsClass<'js> for $name {
					const NAME: &'static str = stringify!($name);

					type Mutable = js::class::Readable;

					/// Returns the class prototype,
					fn prototype(ctx: &Ctx<'js>) -> Result<Option<Object<'js>>>{
						Object::new(ctx.clone()).map(Some)
					}

					/// Returns a predefined constructor for this specific class type if there is one.
					fn constructor(ctx: &Ctx<'js>) -> Result<Option<Constructor<'js>>>{
						fn new(ctx: Ctx<'_>) -> Result<()> {
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

						Constructor::new_class::<$name,_,_>(ctx.clone(), new).map(Some)
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
