//! Fuzzing of the stored-value encoding's round trip on generated values.
//!
//! Each input drives a generator of values of every storable kind, edges and
//! all (see `stored_value::Gen`). A value the encoder writes must be one the
//! decoder reads, as exactly that value, and must read the same in place; the
//! encoder may refuse a value only for nesting deeper than the decoder reads,
//! and never one the record depth guard admits.

#![no_main]

use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use surrealdb_expr::val::stored::{self, Encoder, Error, ValueRef};
use surrealdb_expr::val::{Value, check_value_depth};

mod stored_value;

use stored_value::{Gen, hex, identical, save_seed, walk};

fuzz_target!(|data: &[u8]| {
	let mut u = Unstructured::new(data);
	let v = Gen::new(&mut u).any();
	let admitted = check_value_depth(&v).is_ok();
	let bytes = match stored::encode(&v) {
		Ok(bytes) => bytes,
		Err(Error::TooDeep(_)) if !admitted => return,
		Err(e) => panic!("{v:?} does not encode: {e}"),
	};
	save_seed(&bytes);
	let back = stored::decode(&bytes)
		.unwrap_or_else(|e| panic!("{v:?} encodes to {}, which does not decode: {e}", hex(&bytes)));
	assert!(identical(&back, &v), "written {v:?}\nas      {}\nread as {back:?}", hex(&bytes));
	assert_eq!(stored::encode(&back).expect("a value read back encodes"), bytes);
	walk(ValueRef::new(&bytes).expect("an encoding has a tag"), &v, true);
	// A record keyed by its id is written as its data without the `id` field.
	if let Value::Object(obj) = &v {
		let mut without = obj.clone();
		without.0.remove("id");
		let mut enc = Encoder::default();
		enc.object_without(obj, "id").expect("an object without a field encodes");
		assert_eq!(enc.as_bytes(), stored::encode(&Value::Object(without)).expect("encodes"));
	}
});
