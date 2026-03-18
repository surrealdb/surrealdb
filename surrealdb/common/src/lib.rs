#[macro_use]
pub mod ids;

mod error;
use std::fmt;

pub use error::{Error, ErrorCode, ErrorTrait, TypedError, source as source_error};

pub mod non_max;
pub mod span;

/// implementation of `std::fmt::from_fn` which is still unstable on our MSRV.
/// TODO: Remove once we update our MSRV.
pub fn fmt_from_fn<F>(f: F) -> impl fmt::Display
where
	F: Fn(&mut fmt::Formatter) -> fmt::Result,
{
	struct Fmt<F>(F);
	impl<F> fmt::Display for Fmt<F>
	where
		F: Fn(&mut fmt::Formatter) -> fmt::Result,
	{
		fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
			self.0(fmt)
		}
	}
	Fmt(f)
}
