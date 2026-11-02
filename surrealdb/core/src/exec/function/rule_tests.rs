//! A builtin's [`ReturnRule`] is sound: over arguments of every kind the pool
//! below spans, the kind the rule infers from the arguments' kinds admits the
//! value the function returns. The planner chooses typed instructions from
//! these kinds, so a rule that names too narrow a kind would have a typed
//! instruction read a value of another kind.

use super::{FunctionRegistry, ReturnRule};
use crate::expr::Kind;
use crate::val::convert_public::convert_public_value_to_internal;
use crate::val::{Number, Value};

/// The kind the planner proves for a literal value: the kinds of an array's
/// or a set's elements joined, `any` for an empty collection.
fn proven_kind(value: &Value) -> Kind {
	let join = |kinds: Vec<Kind>| -> Kind {
		let mut distinct: Vec<Kind> = Vec::new();
		for kind in kinds {
			if !distinct.contains(&kind) {
				distinct.push(kind);
			}
		}
		match distinct.len() {
			0 => Kind::Any,
			1 => distinct.pop().expect("one kind"),
			_ => Kind::Either(distinct),
		}
	};
	match value {
		Value::None => Kind::None,
		Value::Null => Kind::Null,
		Value::Bool(_) => Kind::Bool,
		Value::Number(Number::Int(_)) => Kind::Int,
		Value::Number(Number::Float(_)) => Kind::Float,
		Value::Number(Number::Decimal(_)) => Kind::Decimal,
		Value::Number(Number::BigDecimal(_)) => Kind::BigDecimal,
		Value::Number(Number::I8(_)) => Kind::I8,
		Value::Number(Number::U8(_)) => Kind::U8,
		Value::Number(Number::I16(_)) => Kind::I16,
		Value::Number(Number::U16(_)) => Kind::U16,
		Value::Number(Number::I32(_)) => Kind::I32,
		Value::Number(Number::U32(_)) => Kind::U32,
		Value::Number(Number::U64(_)) => Kind::U64,
		Value::Number(Number::F16(_)) => Kind::F16,
		Value::Number(Number::F32(_)) => Kind::F32,
		Value::String(_) => Kind::String,
		Value::Object(_) => Kind::Object,
		Value::Array(array) => {
			Kind::Array(Box::new(join(array.iter().map(proven_kind).collect())), None)
		}
		Value::Set(set) => Kind::Set(Box::new(join(set.iter().map(proven_kind).collect())), None),
		Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => Kind::Any,
	}
}

/// Whether `value` is of `kind` exactly: an `int` is not a `float`, as a typed
/// instruction reading it would find.
fn conforms(value: &Value, kind: &Kind) -> bool {
	match (kind, value) {
		(Kind::Any, _) => true,
		(Kind::Either(kinds), value) => kinds.iter().any(|kind| conforms(value, kind)),
		(Kind::None, Value::None) | (Kind::Null, Value::Null) | (Kind::Bool, Value::Bool(_)) => {
			true
		}
		(Kind::Int, Value::Number(Number::Int(_)))
		| (Kind::Float, Value::Number(Number::Float(_)))
		| (Kind::Decimal, Value::Number(Number::Decimal(_)))
		| (Kind::BigDecimal, Value::Number(Number::BigDecimal(_)))
		| (Kind::I8, Value::Number(Number::I8(_)))
		| (Kind::U8, Value::Number(Number::U8(_)))
		| (Kind::I16, Value::Number(Number::I16(_)))
		| (Kind::U16, Value::Number(Number::U16(_)))
		| (Kind::I32, Value::Number(Number::I32(_)))
		| (Kind::U32, Value::Number(Number::U32(_)))
		| (Kind::U64, Value::Number(Number::U64(_)))
		| (Kind::F16, Value::Number(Number::F16(_)))
		| (Kind::F32, Value::Number(Number::F32(_)))
		| (Kind::Number, Value::Number(_))
		| (Kind::String, Value::String(_))
		| (Kind::Object, Value::Object(_)) => true,
		(Kind::Array(element, _), Value::Array(array)) => {
			array.iter().all(|v| conforms(v, element))
		}
		(Kind::Set(element, _), Value::Set(set)) => set.iter().all(|v| conforms(v, element)),
		(
			Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Int
			| Kind::Float
			| Kind::Decimal
			| Kind::I8
			| Kind::U8
			| Kind::I16
			| Kind::U16
			| Kind::I32
			| Kind::U32
			| Kind::U64
			| Kind::F16
			| Kind::F32
			| Kind::Number
			| Kind::String
			| Kind::Object
			| Kind::Array(..)
			| Kind::Set(..),
			_,
		) => false,
		// Kinds no rule here produces are judged as the value's coercion to
		// them would be.
		(kind, value) => value.can_coerce_to_kind(kind),
	}
}

fn value(src: &str) -> Value {
	convert_public_value_to_internal(crate::syn::value(src).expect("a value literal"))
}

#[test]
fn every_return_rule_admits_what_its_function_returns() {
	let registry = FunctionRegistry::with_builtins();
	// Collections of one kind, of mixed kinds and empty, and scalars of each
	// numeric kind and a string, so a rule that keeps the first argument's
	// element kind meets a second argument of another.
	let pool: Vec<Value> = [
		"[1, 2, 3]",
		"[1.5, 2.5]",
		"[1, 2.5]",
		"[1dec, 2dec]",
		"['a', 'b']",
		"[1, 'a']",
		"[]",
		"[[1], [2]]",
		"{1, 2}",
		"{'a', 'b'}",
		"1",
		"-1",
		"10",
		"2.5",
		"3dec",
		"'a'",
		"true",
	]
	.iter()
	.map(|src| value(src))
	.collect();

	let mut checked = 0usize;
	let mut unsound: Vec<String> = Vec::new();
	let mut rules: Vec<_> = registry
		.scalar_functions()
		.filter(|f| f.is_pure() && !f.is_async())
		.filter(|f| f.signature().rule != ReturnRule::Fixed)
		.collect();
	rules.sort_by_key(|f| f.name());
	for func in rules {
		let signature = func.signature();
		let (lower, upper) = signature.arity();
		// A variadic function is called with two and three arguments too, so
		// a rule over its first argument meets the others.
		let upper = upper.unwrap_or(3).min(3);
		for arity in lower.max(1)..=upper {
			let mut call = vec![0usize; arity];
			'calls: loop {
				let args: Vec<Value> = call.iter().map(|&i| pool[i].clone()).collect();
				let kinds: Vec<Kind> = args.iter().map(proven_kind).collect();
				if let Ok(result) = func.invoke(args.clone()) {
					checked += 1;
					let inferred = signature.infer(&kinds);
					if !conforms(&result, &inferred) {
						unsound.push(format!(
							"{}({}) returned {result:?}, but {:?} infers {inferred}",
							func.name(),
							args.iter().map(|a| format!("{a:?}")).collect::<Vec<_>>().join(", "),
							signature.rule,
						));
					}
				}
				for position in (0..arity).rev() {
					call[position] += 1;
					if call[position] < pool.len() {
						continue 'calls;
					}
					call[position] = 0;
				}
				break;
			}
		}
	}
	assert!(checked > 0, "no rule was exercised");
	assert!(unsound.is_empty(), "{} unsound inferences:\n{}", unsound.len(), unsound.join("\n"));
}
