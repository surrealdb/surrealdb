use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use surrealdb_core::val::{Bytes, Function, Value};

/// A hack for dealing with the issue that `<bytes>` is exported as a function call, which causes
/// problems when comparing values.
///
/// This function computes any function call which matches an exported bytes inplace
pub fn compute_bytes_inplace(v: &mut Value) {
	match v {
		Value::Object(x) => x.values_mut().for_each(compute_bytes_inplace),
		Value::Array(x) => x.iter_mut().for_each(compute_bytes_inplace),
		Value::Function(x) => {
			if let Function::Normal(ref name, ref arg) = **x {
				if name == "encoding::base64::decode" && arg.len() == 1 {
					if let Value::Strand(ref s) = arg[0] {
						if let Ok(res) = STANDARD_NO_PAD.decode(&s.0) {
							*v = Value::Bytes(Bytes::from(res));
						}
					}
				}
			}
		}

		_ => {}
	}
}
