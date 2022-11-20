#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(clippy::module_inception)]
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
		#[quickjs(get)]
		pub fn tb(&self) -> &str {
			&self.tb
		}
		#[quickjs(get)]
		pub fn id(&self) -> &str {
			&self.id
		}
		/// Convert the object to a string
		pub fn toString(&self) -> String {
			format!("{}:{}", self.tb, self.id)
		}
	}
}
