#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod record {

	use crate::sql::thing;
	use crate::sql::value::Value;
	use js::Rest;

	#[derive(Clone)]
	#[quickjs(class)]
	#[quickjs(cloneable)]
	pub struct Record {
		#[quickjs(hide)]
		pub(crate) value: thing::Thing,
	}

	impl Record {
		#[quickjs(constructor)]
		pub fn new(tb: String, id: Value, args: Rest<Value>) -> Self {
			Self {
				value: thing::Thing {
					tb,
					id: match id {
						Value::Array(v) => v.into(),
						Value::Object(v) => v.into(),
						Value::Number(v) => v.into(),
						v => v.as_string().into(),
					},
				},
			}
		}
		#[quickjs(get)]
		pub fn tb(&self) -> &str {
			&self.value.tb
		}
		#[quickjs(get)]
		pub fn id(&self) -> String {
			self.value.id.to_raw()
		}
		// Compare two Record instances
		pub fn is(a: &Record, b: &Record, args: Rest<Value>) -> bool {
			a.value == b.value
		}
		/// Convert the object to a string
		pub fn toString(&self, args: Rest<Value>) -> String {
			self.value.to_raw()
		}
		/// Convert the object to JSON
		pub fn toJSON(&self, args: Rest<Value>) -> String {
			self.value.to_raw()
		}
	}
}
