//! Debug-only extensions for `async_graphql` dynamic types.
//!
//! The `async_graphql::dynamic::Scalar` builder consumes `self` when adding a
//! validator, which makes it awkward to attach validators to an already-created
//! scalar via `&mut self`.  [`ValidatorExt`] works around this by temporarily
//! swapping the scalar out, calling the consuming `.validator()` method, and
//! swapping the result back in.
//!
//! This trait and its implementation are only compiled in debug builds
//! (`#[cfg(debug_assertions)]`).  In release builds the validators are omitted
//! entirely, so there is no runtime cost.

/// Extension trait for adding validators to `async_graphql::dynamic::Scalar`
/// through a `&mut self` reference rather than a consuming builder call.
#[cfg(debug_assertions)]
pub trait ValidatorExt {
	fn add_validator(
		&mut self,
		validator: impl Fn(&async_graphql::Value) -> bool + Send + Sync + 'static,
	) -> &mut Self;
}

#[cfg(debug_assertions)]
impl ValidatorExt for async_graphql::dynamic::Scalar {
	fn add_validator(
		&mut self,
		validator: impl Fn(&async_graphql::Value) -> bool + Send + Sync + 'static,
	) -> &mut Self {
		// Swap the real scalar out, call the consuming `.validator()` method,
		// and put the result back.  The temporary empty scalar is never used.
		let mut tmp = async_graphql::dynamic::Scalar::new("");
		std::mem::swap(self, &mut tmp);
		*self = tmp.validator(validator);
		self
	}
}
