#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod record {

	use crate::sql::value::Value;
	use js::Rest;

	#[derive(Clone)]
	#[quickjs(class)]
	#[quickjs(cloneable)]
	pub struct Record {
		#[quickjs(hide)]
		pub(crate) tb: String,
		#[quickjs(hide)]
		pub(crate) id: String,
	}

	impl Record {
		#[quickjs(constructor)]
		pub fn new(tb: String, id: String, args: Rest<Value>) -> Self {
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
		// Compare two Record instances
		pub fn is(a: &Record, b: &Record, args: Rest<Value>) -> bool {
			a.tb == b.tb && a.id == b.id
		}
		/// Convert the object to a string
		pub fn toString(&self, args: Rest<Value>) -> String {
			format!("{}:{}", self.tb, self.id)
		}
		/// Convert the object to JSON
		pub fn toJSON(&self, args: Rest<Value>) -> String {
			format!("{}:{}", self.tb, self.id)
		}
	}
}
