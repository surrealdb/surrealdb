//! Parity tests between the [`FunctionRegistry`] declarations and
//! the `crate::fnc` dispatch layer.
//!
//! These tests permanently prevent the two layers from drifting apart:
//!
//! - `registry_signatures_match_fnc_layer` locks every registered function's declared arity range
//!   (and, where a sample invocation is possible, its declared return kind) to the behaviour of the
//!   `fnc::` layer, which is the source of truth for builtin function signatures.
//! - `fnc_dispatch_names_are_registered` locks the name set: every function dispatchable through
//!   `fnc::synchronous` / `fnc::asynchronous` must be resolvable in the registry (as a scalar,
//!   aggregate, projection or index function).

use std::sync::LazyLock;

use regex::Regex;

use super::{FunctionRegistry, ScalarFunction, Signature};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exec::Error as ExecError;
use crate::expr::{Error as ExprError, Kind};
use crate::val::{Bytes, Datetime, Duration, Number, Object, Uuid, Value};

/// How many argument counts beyond the declared maximum to probe, so that
/// too-lax declarations (accepting more arguments than declared) are caught.
const PROBE_EXTRA: usize = 2;

/// Functions whose accepted argument counts are not a contiguous range and
/// therefore cannot be described exactly by the signature builder.
///
/// The `NoneOrRange` functions accept either zero arguments or exactly two.
/// Only `rand::time` surfaces that here: its first argument is a raw `Value`,
/// so a single placeholder argument reaches the "Expected 0 or 2 arguments"
/// check. For `rand::float` and `rand::int` the placeholder fails coercion
/// first, which this probe counts as an accepted argument count.
fn arity_exemptions(name: &str) -> Option<&'static [usize]> {
	match name {
		"rand::time" => Some(&[0, 2]),
		_ => None,
	}
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Probe {
	/// The argument count passed validation (execution may still have failed
	/// on argument types or missing state, which is irrelevant here).
	Accepted,
	/// The argument count was rejected by arity validation.
	ArityRejected,
	/// The function name is not dispatchable in this layer.
	Unknown,
	/// The function is behind a disabled experimental capability.
	Gated,
}

/// Matches exactly the argument-count error messages produced by
/// `fnc::args::FromArgs` and the hand-rolled equivalents in the fnc layer
/// (`string::join`, the `NoneOrRange` rand functions and
/// `exec::function::check_arity`).
static ARITY_MESSAGE: LazyLock<Regex> = LazyLock::new(|| {
	Regex::new(
		r"^Expected (no arguments|zero or more arguments|at least one argument|\d+ arguments?|\d+ to \d+ arguments|\d+ or more arguments|\d+ or \d+ arguments)$",
	)
	.expect("arity-message regex is valid")
});

type RegistryEntries = Vec<(&'static str, Signature, Option<&'static dyn ScalarFunction>)>;

fn classify(res: anyhow::Result<Value>) -> Probe {
	let err = match res {
		Ok(_) => return Probe::Accepted,
		Err(e) => e,
	};
	if let Some(ExprError::InvalidFunctionArguments {
		message,
		..
	}) = err.downcast_ref::<ExprError>()
		&& ARITY_MESSAGE.is_match(message)
	{
		return Probe::ArityRejected;
	}
	match crate::err::exec_error(&err) {
		Some(ExecError::InvalidFunction {
			message,
			..
		}) => {
			if message.starts_with("Experimental feature") {
				Probe::Gated
			} else if message == "no such builtin function found" {
				Probe::Unknown
			} else {
				Probe::Accepted
			}
		}
		_ => Probe::Accepted,
	}
}

/// Probe the fnc layer with `n` placeholder arguments, reporting how
/// the argument count was treated.
async fn probe_fnc(ctx: &FrozenContext, opt: &Options, name: &str, n: usize) -> Probe {
	let args = vec![Value::None; n];
	let probe = classify(crate::fnc::synchronous(ctx, None, name, args.clone()));
	if probe != Probe::Unknown {
		return probe;
	}
	let res = crate::fnc::asynchronous(ctx, opt, None, name, args).await;
	classify(res)
}

/// A placeholder value which passes coercion for the given declared kind.
fn sample_value(kind: &Kind) -> Option<Value> {
	Some(match kind {
		Kind::Any => Value::None,
		Kind::Bool => Value::Bool(true),
		Kind::Int => Value::from(1i64),
		Kind::Float => Value::from(1.5f64),
		Kind::Number => Value::from(1i64),
		Kind::String => Value::from("a"),
		Kind::Datetime => {
			Value::Datetime(Datetime::from(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH))
		}
		Kind::Duration => Value::Duration(Duration::new(1, 0)),
		Kind::Uuid => Value::Uuid(Uuid::default()),
		Kind::Bytes => Value::Bytes(Bytes::default()),
		Kind::Object => Value::Object(Object::default()),
		Kind::None
		| Kind::Null
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Either(_)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_)
		| Kind::Named(_) => return None,
	})
}

/// Whether a returned value is consistent with the declared return kind.
fn kind_matches(value: &Value, kind: &Kind) -> bool {
	match kind {
		Kind::Any => true,
		Kind::None => matches!(value, Value::None),
		Kind::Bool => matches!(value, Value::Bool(_)),
		Kind::Int => matches!(value, Value::Number(Number::Int(_))),
		Kind::Float => matches!(value, Value::Number(Number::Float(_))),
		Kind::Decimal => matches!(value, Value::Number(Number::Decimal(_))),
		Kind::BigDecimal => matches!(value, Value::Number(Number::BigDecimal(_))),
		Kind::I8 => matches!(value, Value::Number(Number::I8(_))),
		Kind::U8 => matches!(value, Value::Number(Number::U8(_))),
		Kind::I16 => matches!(value, Value::Number(Number::I16(_))),
		Kind::U16 => matches!(value, Value::Number(Number::U16(_))),
		Kind::I32 => matches!(value, Value::Number(Number::I32(_))),
		Kind::U32 => matches!(value, Value::Number(Number::U32(_))),
		Kind::U64 => matches!(value, Value::Number(Number::U64(_))),
		Kind::F16 => matches!(value, Value::Number(Number::F16(_))),
		Kind::F32 => matches!(value, Value::Number(Number::F32(_))),
		Kind::Number => matches!(value, Value::Number(_)),
		Kind::String => matches!(value, Value::String(_)),
		Kind::Datetime => matches!(value, Value::Datetime(_)),
		Kind::Date => matches!(value, Value::Date(_)),
		Kind::Time => matches!(value, Value::Time(_)),
		Kind::Timestamp => matches!(value, Value::Timestamp(_)),
		Kind::Interval => matches!(value, Value::Interval(_)),
		Kind::Duration => matches!(value, Value::Duration(_)),
		Kind::Uuid => matches!(value, Value::Uuid(_)),
		Kind::Bytes => matches!(value, Value::Bytes(_)),
		Kind::Object => matches!(value, Value::Object(_)),
		Kind::Array(..) => matches!(value, Value::Array(_)),
		Kind::Set(..) => matches!(value, Value::Set(_)),
		Kind::Null
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Either(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_)
		| Kind::Named(_) => true,
	}
}

fn format_arity(lower: usize, upper: Option<usize>) -> String {
	match upper {
		Some(upper) if upper == lower => format!("exactly {lower}"),
		Some(upper) => format!("{lower} to {upper}"),
		None => format!("{lower} or more"),
	}
}

/// Check one registered function's declared signature against the fnc
/// layer, collecting any mismatches into `problems`.
async fn check_function(
	ctx: &FrozenContext,
	opt: &Options,
	name: &str,
	sig: &Signature,
	pure: Option<&'static dyn ScalarFunction>,
	problems: &mut Vec<String>,
) {
	let (lower, upper) = sig.arity();
	let probe_hi = upper.unwrap_or(lower) + PROBE_EXTRA;

	// Determine which argument counts the fnc layer accepts.
	let mut fnc_counts: Vec<usize> = Vec::new();
	for n in 0..=probe_hi {
		match probe_fnc(ctx, opt, name, n).await {
			Probe::Accepted => fnc_counts.push(n),
			Probe::ArityRejected => {}
			Probe::Gated => return,
			Probe::Unknown => {
				problems.push(format!(
					"`{name}` is registered in the registry but is not \
					 dispatchable in the fnc layer"
				));
				return;
			}
		}
	}

	// Compare against the declared arity range (or the documented exemption).
	let expected: Vec<usize> = match arity_exemptions(name) {
		Some(set) => set.to_vec(),
		None => (lower..=upper.unwrap_or(probe_hi)).collect(),
	};
	if fnc_counts != expected {
		problems.push(format!(
			"`{name}`: declared arity is {} argument(s) but the fnc layer \
			 accepts argument counts {fnc_counts:?}",
			format_arity(lower, upper),
		));
	}

	if let Some(func) = pure {
		// Pure functions can also be probed directly on the registry side,
		// catching handwritten `invoke` implementations that drift from the
		// fnc layer.
		let mut registry_counts: Vec<usize> = Vec::new();
		for n in 0..=probe_hi {
			if classify(func.invoke(vec![Value::None; n])) == Probe::Accepted {
				registry_counts.push(n);
			}
		}
		if registry_counts != fnc_counts {
			problems.push(format!(
				"`{name}`: the registry's invoke accepts argument counts {registry_counts:?} \
				 but the fnc layer accepts {fnc_counts:?}"
			));
		}

		// Where the declared argument kinds are enough to synthesise a valid
		// call, lock the declared return kind to the actual returned value.
		let samples: Option<Vec<Value>> =
			sig.args.iter().filter(|a| !a.optional).map(|a| sample_value(&a.kind)).collect();
		if let Some(args) = samples
			&& let Ok(value) = crate::fnc::synchronous(ctx, None, name, args)
			&& !matches!(value, Value::None | Value::Null)
			&& !kind_matches(&value, &sig.returns)
		{
			problems.push(format!(
				"`{name}`: declared return kind is {} but the fnc implementation \
				 returned a value of kind {}",
				sig.returns,
				value.kind_of(),
			));
		}
	}
}

#[tokio::test]
async fn registry_signatures_match_fnc_layer() {
	let (ctx, opt) = crate::dbs::test::mock().await;
	let registry = FunctionRegistry::with_builtins();

	// Collect every registered function which has an fnc counterpart.
	// Aggregate registrations are skipped: their accumulator model has no
	// comparable fnc signature, and their scalar twins are checked normally.
	let mut entries: RegistryEntries = Vec::new();
	for func in registry.scalar_functions() {
		let pure = (func.is_pure() && !func.is_async()).then_some(func);
		entries.push((func.name(), func.signature(), pure));
	}
	for func in registry.projection_functions() {
		entries.push((func.name(), func.signature(), None));
	}
	for func in registry.index_functions() {
		entries.push((func.name(), func.signature(), None));
	}
	entries.sort_by_key(|(name, ..)| *name);

	let mut problems = Vec::new();
	for (name, sig, pure) in &entries {
		// A query trace's own functions have no fnc counterpart: no query names them.
		if crate::expr::logical::trace::is_internal(name) {
			continue;
		}
		check_function(&ctx, &opt, name, sig, *pure, &mut problems).await;
	}

	if !problems.is_empty() {
		eprintln!("Function signatures out of sync with the fnc layer:");
		for problem in &problems {
			eprintln!(" - {problem}");
		}
		panic!(
			"{} function declaration(s) in exec/function/builtin do not match the \
			 fnc implementations; the fnc implementation is the source of \
			 truth, so update the registry declaration(s)",
			problems.len()
		);
	}
}

#[test]
fn fnc_dispatch_names_are_registered() {
	let registry = FunctionRegistry::with_builtins();

	// Harvest every function name from the fnc dispatch tables, using the
	// same source scrape as `fnc::tests::implementations_are_present`.
	let fnc_mod = include_str!("../../fnc/mod.rs");
	let idiom_regex = Regex::new(r"(?ms)pub async fn idiom\(.*}").unwrap();
	let fnc_no_idiom = idiom_regex.replace(fnc_mod, "");
	let exp_regex = Regex::new(r"exp\(.*\) ").unwrap();

	let mut problems = Vec::new();
	for line in fnc_no_idiom.lines() {
		let line = line.trim();
		let line = if line.starts_with("exp") {
			&exp_regex.replace(line, "")
		} else {
			line
		};

		if !(line.contains("=>") && (line.starts_with('"') || line.ends_with(','))) {
			continue;
		}

		let (quote, _) = line.split_once("=>").unwrap();
		let name = quote.trim().trim_matches('"');

		let registered = registry.get(name).is_some()
			|| registry.get_aggregate(name).is_some()
			|| registry.is_projection(name)
			|| registry.is_index_function(name);
		if !registered {
			problems.push(format!(
				"`{name}` is dispatchable in the fnc layer but missing from \
				 the FunctionRegistry"
			));
		}
	}

	if !problems.is_empty() {
		eprintln!("Functions missing from the registry:");
		for problem in &problems {
			eprintln!(" - {problem}");
		}
		panic!("{} fnc function(s) are not registered in exec/function/builtin", problems.len());
	}
}
