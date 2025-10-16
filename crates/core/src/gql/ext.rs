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
		let mut tmp = async_graphql::dynamic::Scalar::new("");
		std::mem::swap(self, &mut tmp);
		*self = tmp.validator(validator);
		self
	}
}
