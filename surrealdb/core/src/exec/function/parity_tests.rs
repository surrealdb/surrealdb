//! Parity tests between the streaming [`FunctionRegistry`] declarations and
//! the legacy `crate::fnc` dispatch layer.
//!
//! These tests permanently prevent the two layers from drifting apart:
//!
//! - `registry_signatures_match_fnc_layer` locks every registered function's declared arity range
//!   (and, where a sample invocation is possible, its declared return kind) to the behaviour of the
//!   legacy `fnc::` layer, which is the source of truth for builtin function signatures.
//! - `legacy_dispatch_names_are_registered` locks the name set: every function dispatchable through
//!   `fnc::synchronous` / `fnc::asynchronous` must be resolvable in the streaming registry (as a
//!   scalar, aggregate, projection or index function).

use std::sync::{Arc, LazyLock};

use reblessive::tree::TreeStack;
use regex::Regex;

use super::{FunctionRegistry, ScalarFunction, Signature};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::Kind;
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

type RegistryEntries<'a> = Vec<(&'static str, Signature, Option<&'a Arc<dyn ScalarFunction>>)>;

fn classify(res: anyhow::Result<Value>) -> Probe {
	let err = match res {
		Ok(_) => return Probe::Accepted,
		Err(e) => e,
	};
	match err.downcast_ref::<Error>() {
		Some(Error::InvalidFunctionArguments {
			message,
			..
		}) if ARITY_MESSAGE.is_match(message) => Probe::ArityRejected,
		Some(Error::InvalidFunction {
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

/// Probe the legacy fnc layer with `n` placeholder arguments, reporting how
/// the argument count was treated.
async fn probe_legacy(ctx: &FrozenContext, opt: &Options, name: &str, n: usize) -> Probe {
	let args = vec![Value::None; n];
	let probe = classify(crate::fnc::synchronous(ctx, None, name, args.clone()));
	if probe != Probe::Unknown {
		return probe;
	}
	let mut stack = TreeStack::new();
	let res =
		stack.enter(|stk| crate::fnc::asynchronous(stk, ctx, opt, None, name, args)).finish().await;
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
		_ => return None,
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
		Kind::Number => matches!(value, Value::Number(_)),
		Kind::String => matches!(value, Value::String(_)),
		Kind::Datetime => matches!(value, Value::Datetime(_)),
		Kind::Duration => matches!(value, Value::Duration(_)),
		Kind::Uuid => matches!(value, Value::Uuid(_)),
		Kind::Bytes => matches!(value, Value::Bytes(_)),
		Kind::Object => matches!(value, Value::Object(_)),
		Kind::Array(..) => matches!(value, Value::Array(_)),
		Kind::Set(..) => matches!(value, Value::Set(_)),
		_ => true,
	}
}

fn format_arity(lower: usize, upper: Option<usize>) -> String {
	match upper {
		Some(upper) if upper == lower => format!("exactly {lower}"),
		Some(upper) => format!("{lower} to {upper}"),
		None => format!("{lower} or more"),
	}
}

/// Check one registered function's declared signature against the legacy fnc
/// layer, collecting any mismatches into `problems`.
async fn check_function(
	ctx: &FrozenContext,
	opt: &Options,
	name: &str,
	sig: &Signature,
	pure: Option<&Arc<dyn ScalarFunction>>,
	problems: &mut Vec<String>,
) {
	let (lower, upper) = sig.arity();
	let probe_hi = upper.unwrap_or(lower) + PROBE_EXTRA;

	// Determine which argument counts the legacy layer accepts.
	let mut legacy: Vec<usize> = Vec::new();
	for n in 0..=probe_hi {
		match probe_legacy(ctx, opt, name, n).await {
			Probe::Accepted => legacy.push(n),
			Probe::ArityRejected => {}
			Probe::Gated => return,
			Probe::Unknown => {
				problems.push(format!(
					"`{name}` is registered in the streaming registry but is not \
					 dispatchable in the legacy fnc layer"
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
	if legacy != expected {
		problems.push(format!(
			"`{name}`: declared arity is {} argument(s) but the legacy fnc layer \
			 accepts argument counts {legacy:?}",
			format_arity(lower, upper),
		));
	}

	if let Some(func) = pure {
		// Pure functions can also be probed directly on the streaming side,
		// catching handwritten `invoke` implementations that drift from the
		// fnc layer.
		let mut streaming: Vec<usize> = Vec::new();
		for n in 0..=probe_hi {
			if classify(func.invoke(vec![Value::None; n])) == Probe::Accepted {
				streaming.push(n);
			}
		}
		if streaming != legacy {
			problems.push(format!(
				"`{name}`: the streaming invoke accepts argument counts {streaming:?} \
				 but the legacy fnc layer accepts {legacy:?}"
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

	// Collect every registered function which has a legacy fnc counterpart.
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
		check_function(&ctx, &opt, name, sig, *pure, &mut problems).await;
	}

	if !problems.is_empty() {
		eprintln!("Function signatures out of sync with the fnc layer:");
		for problem in &problems {
			eprintln!(" - {problem}");
		}
		panic!(
			"{} function declaration(s) in exec/function/builtin do not match the \
			 legacy fnc implementations; the fnc implementation is the source of \
			 truth, so update the registry declaration(s)",
			problems.len()
		);
	}
}

#[test]
fn legacy_dispatch_names_are_registered() {
	let registry = FunctionRegistry::with_builtins();

	// Harvest every function name from the legacy dispatch tables, using the
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
				"`{name}` is dispatchable in the legacy fnc layer but missing from \
				 the streaming FunctionRegistry"
			));
		}
	}

	if !problems.is_empty() {
		eprintln!("Functions missing from the streaming registry:");
		for problem in &problems {
			eprintln!(" - {problem}");
		}
		panic!("{} legacy function(s) are not registered in exec/function/builtin", problems.len());
	}
}
