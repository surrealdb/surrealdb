//! A method a program calls is a name and a mask of receiver kinds; the
//! interpreter resolves it from the mask and the host from the registry.

use super::program::MethodSpec;
use crate::exec::function::{FunctionRegistry, ValueKind};
use crate::val::{Array, Number, Object, RecordId, Value};

const KINDS: [ValueKind; 11] = [
	ValueKind::String,
	ValueKind::Array,
	ValueKind::Set,
	ValueKind::Object,
	ValueKind::Bytes,
	ValueKind::Duration,
	ValueKind::Datetime,
	ValueKind::Number,
	ValueKind::Geometry,
	ValueKind::Record,
	ValueKind::File,
];

/// Every kind has a bit of its own.
#[test]
fn every_kind_has_its_own_bit() {
	let mut bits = 0u16;
	for kind in KINDS {
		assert_eq!(bits & kind.bit(), 0, "{kind:?} shares a bit");
		bits |= kind.bit();
	}
}

/// A method's spec answers for every kind of receiver as its descriptor does,
/// for every registered method, from the mask alone.
#[test]
fn a_method_spec_resolves_as_its_descriptor_does() {
	let values = [
		Value::String("a".into()),
		Value::Array(Array::new()),
		Value::Object(Object::default()),
		Value::Number(Number::Int(1)),
		Value::RecordId(RecordId::new("t".into(), 1i64)),
		Value::None,
		Value::Null,
		Value::Bool(true),
	];
	let registry = FunctionRegistry::with_builtins();
	let mut checked = 0;
	for descriptor in registry.methods().descriptors() {
		let spec = MethodSpec {
			name: descriptor.name.to_owned(),
			kinds: descriptor.kind_mask(),
			has_fallback: descriptor.fallback.is_some(),
			plan_depth: 0,
			closure_args: Vec::new(),
			writing: false,
		};
		for kind in KINDS {
			let typed = descriptor.type_impls.iter().any(|(held, _)| *held == kind);
			assert_eq!(
				spec.has_fallback || spec.kinds & kind.bit() != 0,
				typed || descriptor.fallback.is_some(),
				"{} for {kind:?}",
				spec.name
			);
		}
		for value in &values {
			assert_eq!(
				spec.resolves(value),
				descriptor.resolve(value).is_some(),
				"{} for {value:?}",
				spec.name
			);
			checked += 1;
		}
	}
	assert!(checked > 0);
}
