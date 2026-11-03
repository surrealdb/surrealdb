//! Per-builtin error probes: each declared fallibility class in
//! [`FALLIBILITY`] is checked against what the builtin does when it runs.
//!
//! A probe is `RETURN <fn>(<args>)` (an aggregate runs as a `GROUP ALL` over two rows, a
//! projection function as a `SELECT` over one row) against a `kv-mem` datastore, with arguments
//! drawn from a fixed corpus of well-kinded values for each argument's [`Kind`]. Each call runs
//! under two principals (root owner, database editor) and two capability sets that both allow
//! the builtin (everything allowed, every experimental feature included, and the same with a
//! network allow-list), so a denial by name or by experimental gate, which is the call rule's
//! capability dependency and not the builtin's class, never enters the comparison.
//!
//! An outcome is `Ok` or the error's kind. A class is checked by its strongest member
//! ([`Fallibility::class`]):
//!
//! - `INFALLIBLE`: no probe raises.
//! - `VALUE`: the same arguments give the same outcome in all four contexts. A builtin that draws
//!   entropy or reads the clock is compared by `Ok`-ness alone.
//! - `CONTEXT`: the same arguments in the same context give the same outcome twice.
//! - `DATA`: the probe must complete without panicking; the outcome may depend on anything.
//!
//! `http::*`, `sleep` and any builtin whose effects hold `BLOCKING` are not executed; their class
//! is checked against their effects instead.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;

use super::registry::{FALLIBILITY, FnKind};
use super::{FunctionRegistry, Signature};
use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::dbs::capabilities::{Capabilities, NetTarget, Targets};
use crate::exec::ir::iface::{Fallibility, FallibilityClass};
use crate::expr::Kind;
use crate::expr::logical::EffectSet as E;
use crate::iam::{Level, Role};
use crate::kvs::{Datastore, TransactionType};

/// The namespace and database every probe runs in.
const PROBE_NS: &str = "probe";
const PROBE_DB: &str = "probe";

/// The most argument tuples one builtin is probed with.
const MAX_TUPLES: usize = 64;

/// How many arguments past the lower bound a variadic builtin is probed with.
const VARIADIC_EXTRA: usize = 2;

/// What one probe did: it completed, or it raised an error of this kind.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Outcome {
	Ok,
	Err(&'static str),
}

impl Outcome {
	/// The outcome with the error kind dropped, for builtins whose raise is not a function of
	/// their arguments alone.
	fn ok_ness(&self) -> Outcome {
		match self {
			Outcome::Ok => Outcome::Ok,
			Outcome::Err(_) => Outcome::Err(""),
		}
	}
}

/// The SurrealQL literals a well-kinded argument of `kind` is drawn from. A kind with no
/// literal here answers an empty corpus, and a builtin that needs one is reported as unprobed.
fn corpus(kind: &Kind) -> Vec<String> {
	let lits = |values: &[&str]| values.iter().map(|v| (*v).to_string()).collect::<Vec<_>>();
	match kind {
		Kind::Any => {
			let mut all = Vec::new();
			for kind in [
				Kind::None,
				Kind::Null,
				Kind::Bool,
				Kind::Number,
				Kind::String,
				Kind::Bytes,
				Kind::Duration,
				Kind::Datetime,
				// An array of numbers, not of `any`: the corpus of `array<any>` is drawn from
				// this one, so an `any` element here would never bottom out.
				Kind::Array(Box::new(Kind::Number), None),
				Kind::Object,
				Kind::Uuid,
			] {
				all.extend(corpus(&kind));
			}
			all
		}
		Kind::None => lits(&["NONE"]),
		Kind::Null => lits(&["NULL"]),
		Kind::Bool => lits(&["true", "false"]),
		Kind::Int => lits(&["0", "1", "-1", "9223372036854775807", "-9223372036854775808"]),
		Kind::Float => lits(&["0.0f", "-0.0f", "1.5f", "NaN", "Infinity", "-Infinity"]),
		Kind::Decimal => lits(&["0dec", "1.5dec", "-1.5dec", "79228162514264337593543950335dec"]),
		// The last is beyond a `decimal`, which the functions that compute through one refuse.
		Kind::BigDecimal => {
			lits(&["0bigdec", "1.50bigdec", "-1.5bigdec", "79228162514264337593543950336bigdec"])
		}
		Kind::I8 => lits(&["0i8", "1i8", "-1i8", "127i8", "-128i8"]),
		Kind::U8 => lits(&["0u8", "1u8", "255u8"]),
		Kind::I16 => lits(&["0i16", "1i16", "-1i16", "32767i16", "-32768i16"]),
		Kind::U16 => lits(&["0u16", "1u16", "65535u16"]),
		Kind::I32 => lits(&["0i32", "1i32", "-1i32", "2147483647i32", "-2147483648i32"]),
		Kind::U32 => lits(&["0u32", "1u32", "4294967295u32"]),
		Kind::U64 => lits(&["0u64", "1u64", "18446744073709551615u64"]),
		Kind::F16 => lits(&["0.0f16", "-0.0f16", "1.5f16"]),
		Kind::F32 => lits(&["0.0f32", "-0.0f32", "1.5f32"]),
		Kind::Number => {
			let mut all = corpus(&Kind::Int);
			all.extend(corpus(&Kind::Float));
			all.extend(corpus(&Kind::Decimal));
			all.extend(corpus(&Kind::BigDecimal));
			all.extend(corpus(&Kind::I8));
			all.extend(corpus(&Kind::U8));
			all.extend(corpus(&Kind::I16));
			all.extend(corpus(&Kind::U16));
			all.extend(corpus(&Kind::I32));
			all.extend(corpus(&Kind::U32));
			all.extend(corpus(&Kind::U64));
			all.extend(corpus(&Kind::F16));
			all.extend(corpus(&Kind::F32));
			all
		}
		Kind::String => {
			let long = "a".repeat(4096);
			vec!["''".into(), "'a'".into(), format!("'{long}'"), "'\u{e9}\u{4e16}\u{1f600}'".into()]
		}
		Kind::Bytes => lits(&["<bytes>''", "<bytes>'abc'"]),
		Kind::Duration => lits(&["0ns", "1s", "1y", "9223372036854775807s"]),
		Kind::Datetime => {
			lits(&["d'1970-01-01T00:00:00Z'", "d'2024-02-29T12:00:00Z'", "d'9999-12-31T23:59:59Z'"])
		}
		Kind::Date => lits(&["pd'1970-01-01'", "pd'2024-02-29'", "pd'+262142-12-30'"]),
		Kind::Time => lits(&["pt'00:00:00'", "pt'12:30:45.123456789'", "pt'24:00:00'"]),
		Kind::Timestamp => lits(&[
			"ts'1970-01-01T00:00:00'",
			"ts'2024-02-29T12:00:00.5'",
			"ts'9999-12-31T23:59:59'",
		]),
		Kind::Interval => lits(&[
			"ivl'00:00:00'",
			"ivl'1 mon 2 days 03:04:05.5'",
			"ivl'-2147483648 mons -2147483648 days'",
			"ivl'2147483647 mons 2147483647 days'",
		]),
		Kind::Uuid => lits(&["u'00000000-0000-0000-0000-000000000000'"]),
		Kind::Object => lits(&["{}", "{ a: 1, b: 'x' }", "{ a: { b: { c: [1, 2] } } }"]),
		Kind::Record(_) => lits(&["probe:1", "probe:['a', 1]"]),
		Kind::Table(_) => lits(&["probe"]),
		Kind::Geometry(_) => lits(&["(1.0, 2.0)"]),
		Kind::Regex => lits(&["/a+/"]),
		Kind::Range => lits(&["1..3", "..", "'a'..'c'"]),
		Kind::Function(..) => lits(&["|$v| $v", "|$a, $b| $a"]),
		Kind::Either(kinds) => kinds.iter().flat_map(corpus).collect(),
		Kind::Array(element, _) => {
			let elements = corpus(element);
			let mut all = vec!["[]".to_string()];
			if let Some(first) = elements.first() {
				all.push(format!("[{first}]"));
			}
			if elements.len() > 1 {
				all.push(format!("[{}]", elements.join(", ")));
			}
			if let Some(first) = elements.first() {
				all.push(format!("array::repeat({first}, 1000)"));
			}
			all
		}
		Kind::Set(element, _) => {
			let elements = corpus(element);
			let mut all = vec!["<set>[]".to_string()];
			if elements.len() > 1 {
				all.push(format!("<set>[{}]", elements.join(", ")));
			}
			all
		}
		Kind::Literal(_) | Kind::File(_) | Kind::Enum(_) | Kind::Named(_) => Vec::new(),
	}
}

/// The argument tuples a builtin with `signature` is probed with: for each arity it accepts, the
/// tuple of every argument's first corpus value, and that tuple with one position at a time
/// replaced by each of its other corpus values. `None` when an argument has no corpus.
fn tuples(signature: &Signature) -> Option<Vec<Vec<String>>> {
	let (lower, upper) = signature.arity();
	let upper = upper.unwrap_or(lower + VARIADIC_EXTRA);
	let mut out: Vec<Vec<String>> = Vec::new();
	for arity in lower..=upper {
		let corpora: Vec<Vec<String>> = (0..arity)
			.map(|position| match signature.args.get(position) {
				Some(spec) => corpus(&spec.kind),
				None => corpus(signature.variadic.as_ref().unwrap_or(&Kind::Any)),
			})
			.collect();
		if corpora.iter().any(Vec::is_empty) {
			return None;
		}
		let base: Vec<String> = corpora.iter().map(|c| c[0].clone()).collect();
		out.push(base.clone());
		for (position, values) in corpora.iter().enumerate() {
			for value in &values[1..] {
				let mut tuple = base.clone();
				tuple[position].clone_from(value);
				out.push(tuple);
			}
		}
	}
	out.truncate(MAX_TUPLES);
	Some(out)
}

/// The statement that runs `name` over `args` as its kind is called.
fn statement(kind: FnKind, name: &str, args: &[String]) -> String {
	let list = args.join(", ");
	match kind {
		FnKind::Scalar | FnKind::Index => format!("RETURN {name}({list});"),
		FnKind::Aggregate => {
			let mut row = String::new();
			for (i, arg) in args.iter().enumerate() {
				let _ = write!(
					row,
					"{}v{i}: {arg}",
					if i == 0 {
						""
					} else {
						", "
					}
				);
			}
			let call = (0..args.len()).map(|i| format!("v{i}")).collect::<Vec<_>>().join(", ");
			format!(
				"RETURN (SELECT {name}({call}) AS r FROM [{{ {row} }}, {{ {row} }}] GROUP ALL);"
			)
		}
		FnKind::Projection => format!("RETURN (SELECT {name}({list}) FROM [{{ a: 1 }}]);"),
	}
}

/// One execution context of a probe.
struct Context {
	name: &'static str,
	ds: Arc<Datastore>,
	session: Session,
}

/// The four contexts: two principals by two allowing capability sets.
async fn contexts() -> Vec<Context> {
	let net = Targets::<NetTarget>::Some(
		["127.0.0.1".parse::<NetTarget>().expect("a network target")].into(),
	);
	let sets = [
		("all", Capabilities::all().with_experimental(Targets::All)),
		(
			"all+net-allow-list",
			Capabilities::all().with_experimental(Targets::All).with_network_targets(net),
		),
	];
	let mut out = Vec::new();
	for (caps_name, caps) in sets {
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_capabilities(caps)
			.build_with_path("memory")
			.await
			.expect("a memory datastore");
		let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
		tx.ensure_ns_db(None, PROBE_NS, PROBE_DB).await.expect("the probe namespace");
		tx.commit().await.expect("a commit");
		let principals = [
			("root owner", Session::owner().with_ns(PROBE_NS).with_db(PROBE_DB)),
			(
				"database editor",
				Session::for_level(
					Level::Database(PROBE_NS.to_string(), PROBE_DB.to_string()),
					Role::Editor,
				),
			),
		];
		for (principal_name, session) in principals {
			out.push(Context {
				name: if caps_name == "all" && principal_name == "root owner" {
					"owner/all"
				} else if caps_name == "all" {
					"editor/all"
				} else if principal_name == "root owner" {
					"owner/net"
				} else {
					"editor/net"
				},
				ds: Arc::clone(&ds),
				session,
			});
		}
	}
	out
}

/// Runs `sql` in `context` and answers its outcome.
async fn run(context: &Context, sql: &str) -> Outcome {
	let results =
		context.ds.execute(sql, &context.session, None).await.unwrap_or_else(|e| {
			panic!("{sql}: the request itself failed in {}: {e}", context.name)
		});
	match results.into_iter().last().map(|r| r.result) {
		Some(Ok(_)) | None => Outcome::Ok,
		Some(Err(e)) => Outcome::Err(e.kind_str()),
	}
}

/// What probing one builtin found.
struct Probed {
	name: &'static str,
	/// Probed argument tuples and the statement that ran them.
	sql: String,
	/// The outcome in each of the four contexts, in [`contexts`] order.
	outcomes: Vec<Outcome>,
	/// The outcome of a second run in the first context.
	repeat: Outcome,
}

/// A builtin to probe: its declared class, effects and signature.
struct Subject {
	kind: FnKind,
	name: &'static str,
	class: Fallibility,
	effects: E,
	signature: Signature,
}

/// Every builtin of `kind` named by [`FALLIBILITY`] that the registry holds and SurrealQL text can
/// call, with its signature and effects. A projection or index function carries no effects
/// declaration. A builtin the parser's path table does not hold, such as `array::collect`, which
/// only the planner and the GQL lowering name, cannot be written in a probe and is not a subject.
fn subjects(registry: &FunctionRegistry, kind: FnKind) -> Vec<Subject> {
	let mut out = Vec::new();
	for &(row_kind, name, class) in FALLIBILITY {
		if row_kind != kind || crate::expr::builtin_paths::lookup(name).is_none() {
			continue;
		}
		let (effects, signature) = match kind {
			FnKind::Scalar => match registry.get(name) {
				Some(f) => (f.effects(), f.signature()),
				None => continue,
			},
			FnKind::Aggregate => match registry.get_aggregate(name) {
				Some(f) => (E::PURE, f.signature()),
				None => continue,
			},
			FnKind::Projection => {
				match registry.projection_functions().find(|f| f.name() == name) {
					Some(f) => (E::PURE, f.signature()),
					None => continue,
				}
			}
			FnKind::Index => match registry.index_functions().find(|f| f.name() == name) {
				Some(f) => (E::PURE, f.signature()),
				None => continue,
			},
		};
		out.push(Subject {
			kind,
			name,
			class,
			effects,
			signature,
		});
	}
	out
}

/// Whether `subject` is checked against its effects only.
fn is_excluded(subject: &Subject) -> bool {
	subject.name.starts_with("http::")
		|| subject.name == "sleep"
		|| subject.effects.contains(E::BLOCKING)
}

/// Probes every builtin of `kind` and checks each against its declared class. Fails with every
/// violation, and with every builtin that could not be probed for want of a corpus.
async fn probe_kind(kind: FnKind) {
	use futures::StreamExt as _;

	const WORKERS: usize = 8;

	let registry = FunctionRegistry::with_builtins();
	let all_subjects = subjects(&registry, kind);
	let chunk_size = all_subjects.len().div_ceil(WORKERS).max(1);
	let mut violations: Vec<String> = Vec::new();
	let mut unprobed: BTreeSet<&str> = BTreeSet::new();

	let results: Vec<(BTreeSet<&'static str>, Vec<String>)> =
		futures::stream::iter(all_subjects.chunks(chunk_size))
			.map(|chunk| async move {
				let contexts = contexts().await;
				let mut worker_unprobed = BTreeSet::new();
				let mut worker_violations = Vec::new();
				for subject in chunk {
					if is_excluded(subject) {
						// Reaching outside the transaction or blocking is `CONTEXT | DATA` at
						// least.
						if !subject.class.contains(Fallibility::CONTEXT | Fallibility::DATA) {
							worker_violations.push(format!(
								"{}: not executed, and its class {:?} omits CONTEXT | DATA",
								subject.name, subject.class
							));
						}
						continue;
					}
					let Some(tuples) = tuples(&subject.signature) else {
						worker_unprobed.insert(subject.name);
						continue;
					};
					let nondeterministic = subject.effects.intersects(E::ENTROPY | E::CLOCK);
					for args in tuples {
						let sql = statement(subject.kind, subject.name, &args);
						let mut outcomes = Vec::with_capacity(contexts.len());
						for context in &contexts {
							outcomes.push(run(context, &sql).await);
						}
						let repeat = run(&contexts[0], &sql).await;
						let probed = Probed {
							name: subject.name,
							sql,
							outcomes,
							repeat,
						};
						if let Some(violation) = check(subject, nondeterministic, &probed) {
							worker_violations.push(violation);
						}
					}
				}
				(worker_unprobed, worker_violations)
			})
			.buffer_unordered(WORKERS)
			.collect()
			.await;

	for (worker_unprobed, worker_violations) in results {
		unprobed.extend(worker_unprobed);
		violations.extend(worker_violations);
	}

	assert!(
		unprobed.is_empty(),
		"{kind:?} builtins with an argument kind no probe corpus covers, so their class is \
		 unprobed: {unprobed:?}"
	);
	assert!(
		violations.is_empty(),
		"{} declared classes contradicted by a probe:\n{}",
		violations.len(),
		violations.join("\n")
	);
}

/// The violation `probed` shows of `subject`'s declared class, if any.
fn check(subject: &Subject, nondeterministic: bool, probed: &Probed) -> Option<String> {
	let first = &probed.outcomes[0];
	match subject.class.class() {
		FallibilityClass::Infallible => {
			let raised = probed.outcomes.iter().find(|o| matches!(o, Outcome::Err(_)))?;
			Some(format!("{}: INFALLIBLE but raised {raised:?} for `{}`", probed.name, probed.sql))
		}
		FallibilityClass::Value => {
			let differs = probed.outcomes.iter().any(|o| {
				if nondeterministic {
					o.ok_ness() != first.ok_ness()
				} else {
					o != first
				}
			});
			differs.then(|| {
				format!(
					"{}: VALUE but the outcome depends on the context for `{}`: {:?}",
					probed.name, probed.sql, probed.outcomes
				)
			})
		}
		FallibilityClass::Context => {
			let differs = if nondeterministic {
				probed.repeat.ok_ness() != first.ok_ness()
			} else {
				probed.repeat != *first
			};
			differs.then(|| {
				format!(
					"{}: CONTEXT but one context answered twice differently for `{}`: {:?} then {:?}",
					probed.name, probed.sql, first, probed.repeat
				)
			})
		}
		// A `DATA` builtin may depend on anything; completing the probe is its check.
		FallibilityClass::Data | FallibilityClass::Resource => None,
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_builtins_match_their_declared_fallibility() {
	probe_kind(FnKind::Scalar).await;
}

#[tokio::test]
async fn aggregate_builtins_match_their_declared_fallibility() {
	probe_kind(FnKind::Aggregate).await;
}

#[tokio::test]
async fn projection_builtins_match_their_declared_fallibility() {
	probe_kind(FnKind::Projection).await;
}

#[tokio::test]
async fn index_builtins_match_their_declared_fallibility() {
	probe_kind(FnKind::Index).await;
}
