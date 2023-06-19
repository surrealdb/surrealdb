macro_rules! impl_stub_class {
	($($name:ident),*) => {
		use js::{function::Rest, Ctx, Exception, Result};

		pub struct $name;
		impl $name {
			#[quickjs(constructor)]
			pub fn new(ctx: Ctx, _arg: Rest<()>) -> Result<Self> {
				Exception::throw_internal(
					ctx,
					concat!(
						"The '",
						stringify!($name),
						"' class is not available in this build of SurrealDB. In order to use '",
						stringify!($name),
						"', enable the 'http' feature."
					),
				)
			}
		}
	};
}

#[bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod stub {
	impl_stub_class!(Response, Request, Headers, Blob, FormData);
}

pub use stub::{
	Blob as BlobClass, FormData as FormDataClass, Headers as HeadersClass, Request as RequestClass,
	Response as ResponseClass,
};
