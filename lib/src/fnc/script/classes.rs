#[js::bind(object, public)]
#[quickjs(bare)]
pub mod duration {

	#[derive(Clone)]
	#[quickjs(class)]
	pub struct Duration {
		#[quickjs(hide)]
		pub(crate) value: String,
	}

	impl Duration {
		#[quickjs(constructor)]
		pub fn new(value: String) -> Self {
			Self {
				value,
			}
		}
		#[quickjs(rename = "toString")]
		pub fn output(&self) -> String {
			self.value.to_owned()
		}
		#[quickjs(get)]
		pub fn value(&self) -> &str {
			&self.value
		}
	}
}

#[js::bind(object, public)]
#[quickjs(bare)]
pub mod uuid {

	#[derive(Clone)]
	#[quickjs(class)]
	pub struct Uuid {
		#[quickjs(hide)]
		pub(crate) value: String,
	}

	impl Uuid {
		#[quickjs(constructor)]
		pub fn new(value: String) -> Self {
			Self {
				value,
			}
		}
		#[quickjs(rename = "toString")]
		pub fn output(&self) -> String {
			self.value.to_owned()
		}
		#[quickjs(get)]
		pub fn value(&self) -> &str {
			&self.value
		}
	}
}

#[js::bind(object, public)]
#[quickjs(bare)]
pub mod record {

	#[derive(Clone)]
	#[quickjs(class)]
	pub struct Record {
		#[quickjs(hide)]
		pub(crate) tb: String,
		#[quickjs(hide)]
		pub(crate) id: String,
	}

	impl Record {
		#[quickjs(constructor)]
		pub fn new(tb: String, id: String) -> Self {
			Self {
				tb,
				id,
			}
		}
		#[quickjs(rename = "toString")]
		pub fn output(&self) -> String {
			format!("{}:{}", self.tb, self.id)
		}
		#[quickjs(get)]
		pub fn tb(&self) -> &str {
			&self.tb
		}
		#[quickjs(get)]
		pub fn id(&self) -> &str {
			&self.id
		}
	}
}
