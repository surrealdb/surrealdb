use std::error::Error;
use std::fmt;
use std::sync::Arc;

use js::{Class, Ctx, Result};

mod body;
mod classes;
mod func;
mod stream;
mod util;

use classes::{Blob, FormData, Headers, Request, Response};
use func::js_fetch;

// Anoyingly errors aren't clone,
// But with how we implement streams RequestError must be clone.
/// Error returned by the request.
#[derive(Debug, Clone)]
pub enum RequestError {
	Reqwest(Arc<reqwest::Error>),
}

impl fmt::Display for RequestError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match *self {
			RequestError::Reqwest(ref e) => writeln!(f, "request error: {e}"),
		}
	}
}

impl Error for RequestError {}

/// Register the fetch types in the context.
pub fn register(ctx: &Ctx<'_>) -> Result<()> {
	let globals = ctx.globals();
	globals.set("fetch", js_fetch)?;
	Class::<Response>::define(&globals)?;
	Class::<Request>::define(&globals)?;
	Class::<Blob>::define(&globals)?;
	Class::<FormData>::define(&globals)?;
	Class::<Headers>::define(&globals)?;

	Ok(())
}

#[cfg(test)]
mod test {
	use js::Function;

	macro_rules! create_test_context{
		($ctx:ident => { $($t:tt)* }) => {
			async {

				let rt = js::AsyncRuntime::new().unwrap();
				let ctx = js::AsyncContext::full(&rt).await.unwrap();

				js::async_with!(ctx => |$ctx|{
					crate::fnc::script::fetch::register(&$ctx).unwrap();

					$ctx.eval::<(),_>(r"
					globalThis.assert = (arg, text) => {
						if (!arg) {
							throw new Error('assertion failed ' + (text ? text : ''))
						}
					};
					assert.eq = (a,b, text) => {
						if(a != b){
							throw new Error(`assertion failed, '${a}' != '${b}'` + (text ? text : ''))
						}
					};
					assert.seq = (a,b, text) => {
						if(!(a === b)){
							throw new Error(`assertion failed, '${a}' !== '${b}'` +( text ? text : ''))
						}
					};
					assert.mustThrow = (cb) => {
						try{
							cb()
						}catch(e){
							return e
						}
						throw new Error(`Code which should throw, didnt: \n${cb}`)
					}
					assert.mustThrowAsync = async (cb) => {
						try{
							await cb()
						}catch(e){
							return e
						}
						throw new Error(`Code which should throw, didnt: \n${cb}`)
					}
					").unwrap();

					$($t)*
				}).await;
			}
		};
	}
	pub(crate) use create_test_context;

	#[tokio::test]
	async fn exists() {
		create_test_context!(ctx => {
			let globals = ctx.globals();
			globals.get::<_,Function>("fetch").unwrap();
			let response = globals.get::<_,Function>("Response").unwrap();
			assert!(response.is_constructor());
			let request = globals.get::<_,Function>("Request").unwrap();
			assert!(request .is_constructor());
			let blob = globals.get::<_,Function>("Blob").unwrap();
			assert!(blob.is_constructor());
			let form_data = globals.get::<_,Function>("FormData").unwrap();
			assert!(form_data.is_constructor());
			let headers = globals.get::<_,Function>("Headers").unwrap();
			assert!(headers.is_constructor());
		})
		.await
	}

	#[tokio::test]
	async fn test_tests() {
		create_test_context!(ctx => {
			assert!(ctx.eval::<(),_>("assert(false)").is_err());
			ctx.eval::<(),_>("assert(true)").unwrap();
			assert!(ctx.eval::<(),_>("assert.eq(1,2)").is_err());
			ctx.eval::<(),_>("assert.eq(1,1)").unwrap();
			ctx.eval::<(),_>("assert.eq(1,'1')").unwrap();
			ctx.eval::<(),_>("assert.seq(1,1)").unwrap();
			assert!(ctx.eval::<(),_>("assert.seq(1,2)").is_err());
			assert!(ctx.eval::<(),_>("assert.seq(1,'1')").is_err());
			assert!(ctx.eval::<(),_>("assert.mustThrow(() => {
				// don't throw
			})").is_err());
			assert!(ctx.eval::<(),_>("assert.mustThrow(() => {
				throw new Error('an error')
			})").is_ok());
		})
		.await;
	}
}
