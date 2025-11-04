use proc_macro2::TokenStream as TokenStream2;

#[derive(Debug)]
pub enum With {
	Value(TokenStream2),
	String(TokenStream2),
	Map(TokenStream2),
	Arr(TokenStream2),
}

#[derive(Default, Debug)]
pub struct WithMap {
	pub value: Vec<TokenStream2>,
	pub string: Vec<TokenStream2>,
	pub map: Vec<TokenStream2>,
	pub arr: Vec<TokenStream2>,
}

impl WithMap {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn push(&mut self, with: With) {
		match with {
			With::Value(v) => self.value.push(v),
			With::String(v) => self.string.push(v),
			With::Map(v) => self.map.push(v),
			With::Arr(v) => self.arr.push(v),
		}
	}

	pub fn wants_map(&self) -> Option<&Vec<TokenStream2>> {
		if self.map.is_empty() {
			None
		} else {
			Some(&self.map)
		}
	}

	pub fn wants_arr(&self) -> Option<&Vec<TokenStream2>> {
		if self.arr.is_empty() {
			None
		} else {
			Some(&self.arr)
		}
	}

	pub fn wants_value(&self) -> Option<&Vec<TokenStream2>> {
		if self.value.is_empty() {
			None
		} else {
			Some(&self.value)
		}
	}

	pub fn wants_string(&self) -> Option<&Vec<TokenStream2>> {
		if self.string.is_empty() {
			None
		} else {
			Some(&self.string)
		}
	}
}
