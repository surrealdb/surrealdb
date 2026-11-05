//! Field migrations: the definition-time checks of `MIGRATE EXISTING ROWS`
//! and the replay of a table's migration steps over the rows stored before
//! them.
//!
//! A step is committed with the generation it minted: the descriptor of that
//! generation carries it ([`crate::catalog::Migration`]). A row stamped
//! before a step's generation in the table's ring passes through the step
//! whenever it is read or adopted as a write's pre-image, so every reader sees
//! migrated values from the moment the migration commits, whether or not the
//! row has been rewritten. A row with no generation is before every step.
//!
//! Replay is applied where a stored record becomes a value of the engine: in
//! the scan of a table's range, in the reads of records by id, and where a
//! write adopts its pre-image. It never runs in the record decoder, so the
//! transaction's record cache holds records as stored, with the generation
//! they are stamped with, and a replayed pre-image is never mistaken for the
//! stored bytes.
//!
//! A step's transform is evaluated in a frame that refuses writes and record
//! dereference, with `$value` the only binding it reads. Only a value error —
//! one that depends on the value alone ([`is_value_error`]) — takes the step's
//! `FALLBACK`; any other failure propagates, so no node stores a fallback for
//! a row another node converts.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};

use anyhow::Context as _;

use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{
	DatabaseDefinition, DatabaseId, ExprText, FieldDefinition, Generation, IdiomText, KindText,
	LogicalBody, Migration, MigrationStep, MigrationTest, NamespaceId, TableDefinition,
	TableLayouts,
};
use crate::exec::logical::{Descend, visit_binding_pruning};
use crate::exec::{Clause, EvalContext, ExecutionContext};
use crate::expr::logical::{EffectSet, LogicalDestructureField, LogicalExpr};
use crate::expr::{ControlFlow, FlowResult, Function, Kind, KindLiteral, Part, PrefixOperator};
use crate::val::{Number, Set, TableName, Value};

/// A failure raised by a migration.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MigrationError {
	/// A migration's transform dereferenced a record. The transform reads
	/// only `$value`, so a record it names is never fetched; the step takes
	/// its `FALLBACK` instead.
	#[error("A migration's transform cannot dereference a record")]
	Dereference,
	/// The definition of a migration was refused.
	#[error("Cannot migrate field `{field}` of table `{table}`: {reason}")]
	Refused {
		field: String,
		table: String,
		reason: String,
	},
	/// A table-level migration was refused.
	#[error("Cannot migrate table `{table}`: {reason}")]
	RefusedTable {
		table: String,
		reason: String,
	},
	/// An index was refused because a pending migration step clears the
	/// values it would read.
	#[error(
		"Cannot index `{column}` on table `{table}`: migration `{name}` removes undeclared fields from the rows stored before it, and has not finished rewriting them"
	)]
	RefusedIndex {
		table: String,
		column: String,
		name: String,
	},
	/// An index was refused because the transaction defining it runs a
	/// migration of its table that has not committed.
	#[error(
		"Cannot define an index on table `{table}` in the transaction that runs migration `{name}`: the index is built from the rows as committed, before the migration. Commit the migration first"
	)]
	RefusedBuild {
		table: String,
		name: String,
	},
	/// A schema change to a field of a table that enforces migrations
	/// ([`enforces_migrations`]) was refused: its existing rows might not
	/// satisfy it, and it does not say what happens to them.
	#[error("Cannot change field `{field}` of table `{table}`: {reason}")]
	Required {
		field: String,
		table: String,
		reason: String,
	},
	/// A step with no `FALLBACK` met a value it could not migrate. Reached
	/// only where the transform was proven total, so it names a defect.
	#[error(
		"Migration `{name}` could not migrate a value of field `{field}` and has no FALLBACK: {reason}"
	)]
	NoFallback {
		name: String,
		field: String,
		reason: String,
	},
}

/// Whether `error` depends on the evaluated value alone, so that every node
/// evaluating the same transform over the same value raises it: a failed cast
/// or coercion, an operator given operands of the wrong type, a builtin
/// refusing its arguments, a dereference the migration frame refuses.
///
/// Timeouts, cancellation, memory and size limits, capability denials, an
/// operation the build was compiled without and storage failures depend on
/// the node or the moment, and are not value errors: a migration propagates
/// them rather than taking its fallback, so no two nodes store different
/// values for the same row.
pub(crate) fn is_value_error(error: &anyhow::Error) -> bool {
	let mut value = false;
	for cause in error.chain() {
		if cause.is::<crate::err::EngineError>() {
			return false;
		}
		if let Some(e) = cause.downcast_ref::<crate::expr::Error>() {
			if matches!(
				e,
				crate::expr::Error::ValueTooDeep { .. }
					| crate::expr::Error::FeatureDisabled { .. }
					| crate::expr::Error::Serialization(_)
			) {
				return false;
			}
			value = true;
		}
		if cause.is::<crate::val::CastError>()
			|| cause.is::<crate::val::CoerceError>()
			|| matches!(cause.downcast_ref::<MigrationError>(), Some(MigrationError::Dereference))
		{
			value = true;
		}
	}
	value
}

/// Refuse `using`, a migration's transform, unless it depends on `$value`
/// alone, `$value` being of kind `value`: it reads no parameter but `$value`
/// and the arguments of closures written in it (each in its own body), no field of the row and no
/// record a link names; it holds no subquery, graph traversal or call whose
/// body the transform does not hold (`fn::`, a model, a script, a module);
/// and its effect set is exactly [`EffectSet::PURE`].
///
/// A field read, `.*` or a destructure fetches the record when the value it
/// steps from is a record link, and the record's contents are not `$value`:
/// such a step is refused unless the kind of the value it steps from admits
/// no link where the step reads ([`may_link`]). That kind is read from
/// `value`, a closure argument's declared kind, a cast and a literal; any
/// other value may be a link.
///
/// Methods and closures written in place are judged by their bodies, as the
/// effect walk judges them; the walk refuses what it cannot see.
pub(crate) fn check_transform(
	using: &LogicalExpr,
	value: &Kind,
	registry: &crate::exec::function::FunctionRegistry,
) -> Result<(), String> {
	let mut params: HashMap<String, Kind> = HashMap::from([("value".to_owned(), value.clone())]);
	let _ = using.visit_deep(&mut |expr| {
		if let LogicalExpr::Closure(closure) = expr {
			for (param, kind) in &closure.args {
				// A name two closures declare with different kinds is read as
				// either.
				params
					.entry(param.as_str().to_owned())
					.and_modify(|known| {
						if known != kind {
							*known = Kind::Any;
						}
					})
					.or_insert_with(|| kind.clone());
			}
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
	if let Some(param) = undeclared_param(using, &mut vec!["value".to_owned()]) {
		return Err(format!("the transform reads only `$value`, not `${param}`"));
	}
	let dereference = |step: &LogicalExpr| {
		format!(
			"the transform may not read a linked record, and `{}` reads through a value that may \
			 be a record link",
			surrealdb_types::ToSql::to_sql(step)
		)
	};
	let dispatched =
		|name: &str| registry.get_method(name).is_some_and(|d| d.dispatches_for_an_object());
	let mut refusal = None;
	let _ =
		using.visit_deep(&mut |expr| {
			let reason = match expr {
			LogicalExpr::Subquery(_) => Some("the transform may not hold a statement".to_owned()),
			LogicalExpr::Lookup {
				..
			} => Some("the transform may not traverse the graph".to_owned()),
			LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => {
				Some("the transform reads only `$value`".to_owned())
			}
			LogicalExpr::Access {
				base,
				..
			}
			| LogicalExpr::Each(base) => may_link(&kind_of(base, &params)).then(|| dereference(expr)),
			LogicalExpr::Destructure {
				base,
				fields,
			} => {
				let kind = kind_of(base, &params);
				(may_link(&kind) || destructure_links(fields, &element_kind(&kind)))
					.then(|| dereference(expr))
			}
			// A method an object does not dispatch is answered by a closure
			// the object holds under its name, which is not a builtin.
			LogicalExpr::Method {
				name,
				..
			} if !dispatched(name.as_str()) => Some(format!(
				"the transform may not call `.{}()` as a method, which an object answers with a \
					 closure it holds: call the builtin function instead",
				name.as_str()
			)),
			LogicalExpr::Call(call) => match &call.receiver {
				Function::Normal(_) => None,
				Function::Custom(_)
				| Function::Model(_)
				| Function::Script(_)
				| Function::Module(..)
				| Function::Silo {
					..
				} => Some(
					"the transform may not call a user-defined function, a model, a script or a \
					 module"
						.to_owned(),
				),
			},
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Param(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Index { .. }
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Flatten { .. }
			| LogicalExpr::Filter { .. }
			| LogicalExpr::Method { .. }
			| LogicalExpr::Optional { .. }
			| LogicalExpr::Recurse(_)
			| LogicalExpr::RepeatRecurse(_)
			| LogicalExpr::Broadcast { .. }
			| LogicalExpr::SetField { .. }
			| LogicalExpr::Prefix { .. }
			| LogicalExpr::Postfix { .. }
			| LogicalExpr::Invoke { .. }
			| LogicalExpr::Binary { .. }
			| LogicalExpr::Closure(_)
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue => None,
		};
			match reason {
				Some(reason) => {
					refusal = Some(reason);
					std::ops::ControlFlow::Break(())
				}
				None => std::ops::ControlFlow::Continue(()),
			}
		});
	if let Some(reason) = refusal {
		return Err(reason);
	}
	let effects = crate::exec::logical::effects_of_expr(using, registry);
	if effects.set != EffectSet::PURE {
		return Err("the transform must have no effects: it may not read data, the catalog, \
		            the session, the clock or randomness"
			.to_owned());
	}
	Ok(())
}

/// The first parameter `expr` reads that `scope` does not declare. `scope`
/// holds `value` and the arguments of the closures enclosing `expr`; a
/// closure's arguments are in scope in its own body and nowhere else, so a
/// name one closure declares is undeclared beside it.
fn undeclared_param(expr: &LogicalExpr, scope: &mut Vec<String>) -> Option<String> {
	let mut found = None;
	visit_binding_pruning(expr, &mut |node| {
		if found.is_some() {
			return Descend::Over;
		}
		if let LogicalExpr::Param(param) = node {
			if !scope.iter().any(|name| name == param.as_str()) {
				found = Some(param.as_str().to_owned());
			}
		} else if let LogicalExpr::Closure(closure) = node {
			let depth = scope.len();
			scope.extend(closure.args.iter().map(|(param, _)| param.as_str().to_owned()));
			found = undeclared_param(&closure.body, scope);
			scope.truncate(depth);
		}
		Descend::Into
	});
	found
}

/// The kind `expr` evaluates to, from what the transform's walk knows:
/// `params`, the kinds of `$value` and of the closure arguments; a cast's
/// target; a literal's kind; and a field read, element or index of a value of
/// a known kind. Anything else is [`Kind::Any`].
fn kind_of(expr: &LogicalExpr, params: &HashMap<String, Kind>) -> Kind {
	match expr {
		LogicalExpr::Param(param) => params.get(param.as_str()).cloned().unwrap_or(Kind::Any),
		LogicalExpr::Literal(literal) => literal.kind(),
		LogicalExpr::Prefix {
			op: PrefixOperator::Cast(kind),
			..
		} => kind.clone(),
		LogicalExpr::Access {
			base,
			field,
		} => field_kind(&kind_of(base, params), field.as_str()),
		LogicalExpr::First(base) | LogicalExpr::Last(base) => element_kind(&kind_of(base, params)),
		LogicalExpr::Index {
			base,
			..
		} => {
			let kind = kind_of(base, params);
			if matches!(
				kind,
				Kind::Array(..) | Kind::Set(..) | Kind::Literal(KindLiteral::Array(_))
			) {
				element_kind(&kind)
			} else {
				Kind::Any
			}
		}
		LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => Kind::Any,
	}
}

/// Whether a field read, `.*` or a destructure applied to a value of `kind`
/// may fetch a record: the value may be a record link, or a collection the
/// step maps over may hold one.
fn may_link(kind: &Kind) -> bool {
	match kind {
		Kind::Any | Kind::Named(_) | Kind::Record(_) => true,
		Kind::Either(kinds) | Kind::Literal(KindLiteral::Array(kinds)) => {
			kinds.iter().any(may_link)
		}
		Kind::Array(inner, _) | Kind::Set(inner, _) => may_link(inner),
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
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
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => false,
	}
}

/// The kind of field `name` read from a value of `kind`. A read maps over a
/// collection; a value that is neither an object nor a collection has no
/// fields.
fn field_kind(kind: &Kind, name: &str) -> Kind {
	match kind {
		Kind::Literal(KindLiteral::Object(fields)) => {
			fields.get(name).cloned().unwrap_or(Kind::Any)
		}
		Kind::Array(inner, _) => Kind::Array(Box::new(field_kind(inner, name)), None),
		Kind::Set(inner, _) => Kind::Set(Box::new(field_kind(inner, name)), None),
		Kind::Either(kinds) => {
			Kind::Either(kinds.iter().map(|kind| field_kind(kind, name)).collect())
		}
		Kind::Any
		| Kind::Named(_)
		| Kind::Object
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Literal(KindLiteral::Array(_)) => Kind::Any,
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
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
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => Kind::None,
	}
}

/// The kind of an element of a value of `kind`: a collection's element, and
/// any other value itself.
fn element_kind(kind: &Kind) -> Kind {
	match kind {
		Kind::Array(inner, _) | Kind::Set(inner, _) => (**inner).clone(),
		Kind::Literal(KindLiteral::Array(kinds)) => Kind::Either(kinds.clone()),
		Kind::Either(kinds) => Kind::Either(kinds.iter().map(element_kind).collect()),
		kind @ (Kind::Any
		| Kind::Named(_)
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
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
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_)) => kind.clone(),
	}
}

/// Whether an entry of a destructure of a value of `kind` fetches a record:
/// `f.*` or `f.{ … }` over a field whose kind may be a link, at any depth.
/// An aliased entry's body reads the destructured value as the current
/// value, which the transform refuses on its own.
fn destructure_links(fields: &[LogicalDestructureField], kind: &Kind) -> bool {
	fields.iter().any(|field| match field {
		LogicalDestructureField::All(name) => may_link(&field_kind(kind, name.as_str())),
		LogicalDestructureField::Destructure(name, nested) => {
			let kind = field_kind(kind, name.as_str());
			may_link(&kind) || destructure_links(nested, &element_kind(&kind))
		}
		LogicalDestructureField::Field(_)
		| LogicalDestructureField::Aliased(..)
		| LogicalDestructureField::Rest => false,
	})
}

/// One committed rewrite step, compiled for evaluation.
pub(crate) struct CompiledStep {
	name: String,
	/// The field path the step rewrites, as written: [`Part::Field`] and
	/// [`Part::All`] segments only.
	path: Vec<Part>,
	/// The path as text, for errors.
	field: String,
	using: Clause,
	fallback: Option<Value>,
	test: ValueTest,
}

/// The whole test a value at a field must pass, compiled: the field's type
/// with the definitions beneath it folded in, and its `ASSERT` over `$value`.
pub(crate) struct ValueTest {
	kind: Option<Kind>,
	assert: Option<Clause>,
}

impl ValueTest {
	async fn compile(
		planner: &crate::exec::planner::Planner<'_>,
		test: &MigrationTest,
	) -> FlowResult<Self> {
		let assert = match &test.assert {
			Some(assert) => Some(compile_body(planner, assert).await?),
			None => None,
		};
		let kind =
			test.kind.as_ref().map(KindText::compile).transpose().map_err(ControlFlow::Err)?;
		Ok(Self {
			kind,
			assert,
		})
	}

	/// `value` coerced to the test's type without loss, when it passes the
	/// whole test; `None` when it does not.
	async fn pass(&self, frame: &ExecutionContext, value: Value) -> FlowResult<Option<Value>> {
		let value = match &self.kind {
			Some(kind) => match lossless_coerce(&value, kind) {
				Some(value) => value,
				None => return Ok(None),
			},
			None => value,
		};
		if let Some(assert) = &self.assert {
			match evaluate(assert, frame, value.clone()).await {
				Ok(passed) if passed.is_truthy() => {}
				Ok(_) => return Ok(None),
				Err(ControlFlow::Err(e)) if is_value_error(&e) => return Ok(None),
				Err(other) => return Err(other),
			}
		}
		Ok(Some(value))
	}

	/// Whether `value`, as stored, passes the whole test unchanged, judged in
	/// the migration frame `frame` ([`migration_frame`]).
	pub(crate) async fn conforms(
		&self,
		frame: &ExecutionContext,
		value: &Value,
	) -> FlowResult<bool> {
		Ok(self.pass(frame, value.clone()).await?.is_some_and(|passed| identical(value, &passed)))
	}
}

impl CompiledStep {
	/// The value the step leaves at one position of its path for a row whose
	/// previous generation left `value` there.
	/// The second element says whether the value is the step's `FALLBACK`.
	async fn run(&self, frame: &ExecutionContext, value: Value) -> FlowResult<(Value, bool)> {
		let failure = match evaluate(&self.using, frame, value).await {
			Ok(result) => match self.test(frame, result).await? {
				Some(passed) => return Ok((passed, false)),
				None => "the result does not pass the field's definition".to_owned(),
			},
			Err(ControlFlow::Err(e)) if is_value_error(&e) => e.to_string(),
			Err(other) => return Err(other),
		};
		self.fallback.clone().map(|fallback| (fallback, true)).ok_or_else(|| {
			ControlFlow::Err(anyhow::Error::new(MigrationError::NoFallback {
				name: self.name.clone(),
				field: self.field.clone(),
				reason: failure,
			}))
		})
	}

	/// `value` coerced to the step's type without loss, when it passes the
	/// step's whole test; `None` when it does not.
	async fn test(&self, frame: &ExecutionContext, value: Value) -> FlowResult<Option<Value>> {
		self.test.pass(frame, value).await
	}
}

/// Evaluate `clause` in the migration frame `frame` with `$value` bound to
/// `value`.
async fn evaluate(clause: &Clause, frame: &ExecutionContext, value: Value) -> FlowResult<Value> {
	let bound = frame.with_param("value", value);
	match clause.evaluate(EvalContext::from_exec_ctx(&bound)).await {
		Ok(v) | Err(ControlFlow::Return(v)) => Ok(v),
		Err(e) => Err(e),
	}
}

/// `value` coerced to `kind`, when coercing it loses nothing: a number that
/// changes representation must convert back to the value it was, and a set
/// must hold apart every element the value held apart. `None` when it does not
/// coerce, or loses precision, range or an element.
pub(crate) fn lossless_coerce(value: &Value, kind: &Kind) -> Option<Value> {
	let coerced = value.clone().coerce_to_kind(kind).ok()?;
	preserved(value, &coerced, kind).then_some(coerced)
}

/// Whether `coerced`, which `original` coerced to under `kind`, holds every
/// number of `original` exactly and every element `original` held apart.
///
/// Coercion changes a value only where its kind is numeric, beneath the
/// element kind of an array or a set, and through the member of a union the
/// value coerces to; this follows the same walk. An array keeps its elements in
/// place. A set orders numbers of every variant as one line and merges the
/// elements that order as equal, so an element of a set is not paired with
/// the element at its position: each element of `original` is coerced again
/// under the set's element kind, and what it becomes must be a member of
/// `coerced` and preserve it. The set must hold as many elements as `original`
/// held apart, so the only elements merged are the duplicates an array held.
fn preserved(original: &Value, coerced: &Value, kind: &Kind) -> bool {
	let kind = coerced_member(original, kind);
	let element = if let Kind::Array(element, _) | Kind::Set(element, _) = kind {
		element.as_ref()
	} else {
		&Kind::Any
	};
	match (original, coerced) {
		(Value::Number(a), Value::Number(b)) => match (a, b) {
			(Number::Int(i), Number::Float(f)) => {
				f.is_finite() && *f >= -(2f64.powi(63)) && *f < 2f64.powi(63) && (*f as i64) == *i
			}
			(Number::Float(f), Number::Int(i)) => (*i as f64) == *f && f.fract() == 0.0,
			_ => a == b,
		},
		(Value::Array(a), Value::Array(b)) => {
			a.len() == b.len() && a.iter().zip(b.iter()).all(|(a, b)| preserved(a, b, element))
		}
		(Value::Array(a), Value::Set(b)) => {
			distinct(a.iter()) == b.len() && members_preserved(a.iter(), b, element)
		}
		(Value::Set(a), Value::Set(b)) => {
			a.len() == b.len() && members_preserved(a.iter(), b, element)
		}
		// A geometry read from an object keeps it only when the object it
		// renders back to is the original: no number changes representation
		// beyond what is exact, and no ring is closed by an added point.
		(Value::Object(_), Value::Geometry(geometry)) => {
			preserved(original, &Value::Object(geometry.as_object()), &Kind::Any)
		}
		(Value::Object(a), Value::Object(b)) => {
			a.len() == b.len()
				&& a.iter().all(|(key, a)| {
					b.get(key.as_str()).is_some_and(|b| preserved(a, b, &Kind::Any))
				})
		}
		_ => true,
	}
}

/// The kind [`Value::coerce_to_kind`] coerces `value` to under `kind`: the
/// member of a union [`Value::union_member`] picks, and `kind` itself
/// otherwise.
fn coerced_member<'a>(value: &Value, mut kind: &'a Kind) -> &'a Kind {
	while let Kind::Either(members) = kind {
		match value.union_member(members, Value::can_coerce_to_kind) {
			Some(member) => kind = member,
			None => break,
		}
	}
	kind
}

/// How many elements a set of `values` holds: those that order as equal are
/// one.
fn distinct<'a>(values: impl Iterator<Item = &'a Value>) -> usize {
	let mut values: Vec<&Value> = values.collect();
	values.sort_unstable();
	values.dedup_by(|a, b| Ord::cmp(*a, *b) == std::cmp::Ordering::Equal);
	values.len()
}

/// Whether every one of `elements`, coerced under `kind`, is a member of `set`
/// and preserves the element it was.
fn members_preserved<'a>(
	mut elements: impl Iterator<Item = &'a Value>,
	set: &Set,
	kind: &Kind,
) -> bool {
	elements.all(|element| {
		element
			.clone()
			.coerce_to_kind(kind)
			.is_ok_and(|member| set.contains(&member) && preserved(element, &member, kind))
	})
}

/// What one committed step does to a row older than it, compiled.
enum StepAction {
	/// Rewrite every position the step's path selects.
	Rewrite(CompiledStep),
	/// Clear every position `path` selects.
	Remove {
		path: Vec<Part>,
	},
	/// Clear every top-level field outside `declared`; the record's own keys
	/// ([`is_record_key`]) are always kept.
	RemoveUndeclared {
		declared: HashSet<String>,
	},
}

/// Whether `name` is one of the record's own top-level keys: `id`, and the
/// `in` and `out` of an edge. A `SCHEMAFULL` write keeps them whatever the
/// table declares ([`crate::expr::Idiom::is_special`]), on a table of any type,
/// so a step that clears undeclared fields keeps them too: an edge stripped of
/// them would still be reached through its graph keys while reading as no edge.
pub(crate) fn is_record_key(name: &str) -> bool {
	matches!(name, "id" | "in" | "out")
}

/// The generation a record's stored bytes are stamped with: `None` for a row
/// with no generation, and for bytes whose envelope does not read (the decode
/// refuses those).
pub(crate) fn stamp_of(bytes: &[u8]) -> Option<Generation> {
	match crate::catalog::StoredRecord::parse(bytes) {
		Ok(Some(stored)) => stored.generation().ok().flatten(),
		_ => None,
	}
}

/// One committed step of a table, compiled.
pub(crate) struct ReplayStep {
	action: StepAction,
}

/// A table's migration steps as one read of the table sees them: what the
/// read applies to a row stamped before the table's newest step.
///
/// Replay works on the whole record, so a step on a nested path is applied
/// within its top-level ancestor's value, and a step on names no field
/// declares reaches them as a step on a declared field does. The steps are
/// compiled when a row first needs them, and kept for as long as the
/// migrations they were compiled from stand.
#[derive(Clone)]
pub(crate) struct Replay {
	layouts: Arc<TableLayouts>,
	ns: NamespaceId,
	db: DatabaseId,
}

impl Replay {
	/// The steps of the table `layouts` describe in the namespace `ns` and
	/// database `db`, or `None` when no generation of its ring may carry one.
	pub(crate) fn of(layouts: &Arc<TableLayouts>, ns: NamespaceId, db: DatabaseId) -> Option<Self> {
		layouts.newest_migration()?;
		Some(Self {
			layouts: Arc::clone(layouts),
			ns,
			db,
		})
	}

	/// Whether a row stamped `stamp` (`None` for a row with no generation) has
	/// steps to pass through.
	pub(crate) fn behind(&self, stamp: Option<Generation>) -> bool {
		self.layouts.behind_migration(stamp)
	}

	/// Whether the stored bytes of a row are stamped at or after the table's
	/// newest step: a reader may judge them as they are. A row with no
	/// generation, and one whose envelope does not read, is not.
	pub(crate) fn current(&self, bytes: &[u8]) -> bool {
		match crate::catalog::StoredRecord::parse(bytes) {
			Ok(Some(stored)) => {
				matches!(stored.generation(), Ok(Some(generation)) if !self.behind(Some(generation)))
			}
			_ => false,
		}
	}

	/// Bring `record`, stamped with `stamp`, through every step it has not
	/// passed.
	pub(crate) async fn apply(
		&self,
		ctx: &ExecutionContext,
		record: &mut Value,
		stamp: Option<Generation>,
	) -> FlowResult<()> {
		self.apply_counted(ctx, record, stamp).await.map(|_| ())
	}

	/// Bring each row of a scan's batch that is behind a step through the
	/// steps, and drop the batch's stamps: the rows are then what a read of the
	/// table sees, so no consumer applies the steps again.
	pub(crate) async fn apply_batch(
		&self,
		ctx: &ExecutionContext,
		batch: &mut crate::exec::ValueBatch,
	) -> FlowResult<()> {
		let Some(stamps) = batch.take_stamps() else {
			return Ok(());
		};
		for (row, stamp) in batch.values_mut().iter_mut().zip(stamps) {
			// A record id stands for a record whose content is not an object. The
			// write that takes it reads that record by id, and replays it then.
			if matches!(row, Value::RecordId(_)) {
				continue;
			}
			self.apply(ctx, row, stamp).await?;
		}
		Ok(())
	}

	/// [`Self::apply`], answering also how many values took a step's
	/// `FALLBACK`.
	///
	/// A record a step applies to has the steps run out of line, as
	/// [`replay_for`] is: a record fetch applies them once per link a read
	/// follows.
	pub(crate) async fn apply_counted(
		&self,
		ctx: &ExecutionContext,
		record: &mut Value,
		stamp: Option<Generation>,
	) -> FlowResult<u64> {
		if !self.behind(stamp) || !record.is_object() {
			return Ok(0);
		}
		crate::exec::vm::out_of_line(|| self.run_steps(ctx, record, stamp)).await
	}

	/// Run every step after `stamp` over `record`, an object.
	async fn run_steps(
		&self,
		ctx: &ExecutionContext,
		record: &mut Value,
		stamp: Option<Generation>,
	) -> FlowResult<u64> {
		let pending = self
			.layouts
			.pending_migrations(stamp)
			.map_err(|e| ControlFlow::Err(anyhow::Error::new(crate::catalog::Error::Layout(e))))?;
		let mut steps = Vec::with_capacity(pending.len());
		for (generation, migration) in pending {
			steps.push(self.compiled(ctx, generation, migration).await?);
		}
		let frame = migration_frame(ctx);
		let mut fallbacks = 0;
		for step in &steps {
			match &step.action {
				StepAction::Rewrite(rewrite) => {
					let mut inputs = Vec::new();
					collect_positions(record, &rewrite.path, &mut inputs);
					let mut outputs = Vec::with_capacity(inputs.len());
					for input in inputs {
						let (migrated, fell_back) = rewrite.run(&frame, input).await?;
						fallbacks += u64::from(fell_back);
						outputs.push(migrated);
					}
					write_positions(record, &rewrite.path, &mut outputs.into_iter());
				}
				StepAction::Remove {
					path,
				} => {
					write_positions(record, path, &mut std::iter::repeat(Value::None));
				}
				StepAction::RemoveUndeclared {
					declared,
				} => {
					if let Value::Object(object) = record {
						object.retain(|key, _| {
							is_record_key(key.as_str()) || declared.contains(key.as_str())
						});
					}
				}
			}
		}
		Ok(fallbacks)
	}

	/// The step of `migration`, the migration of `generation` of the table,
	/// compiled: the one kept for it while the migration is the one it was
	/// compiled from.
	async fn compiled(
		&self,
		ctx: &ExecutionContext,
		generation: Generation,
		migration: &Migration,
	) -> FlowResult<Arc<ReplayStep>> {
		let key: StepKey = (self.ns, self.db, self.layouts.table().as_str().to_owned(), generation);
		if let Some((compiled_from, step)) =
			COMPILED.lock().map_err(|_| poisoned())?.get(&key).cloned()
			&& compiled_from == *migration
		{
			return Ok(step);
		}
		// The step compiles from its own text, as a schema statement's work
		// does: what it reads of the catalog is its data, never a plan's.
		let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
		let step = crate::exec::vm::descriptor::schema(async {
			let planner = crate::exec::vm::descriptor::unit_planner(ctx, db_ctx);
			compile_step(&planner, migration).await
		})
		.await?;
		let step = Arc::new(step);
		let mut compiled = COMPILED.lock().map_err(|_| poisoned())?;
		if compiled.len() >= COMPILED_CAPACITY {
			compiled.clear();
		}
		compiled.insert(key, (migration.clone(), Arc::clone(&step)));
		Ok(step)
	}
}

/// The values at every position `path` selects in `value`, in the order
/// [`write_positions`] visits them, cloned into `out`.
///
/// The positions are those the write path distributes a field path over: a
/// `.b` segment visits `b` in every object its parent holds, `NONE` where the
/// object has no `b`, and spreads over the elements of an array or set; a `*`
/// segment visits every element of an array or set and every value of an
/// object. A parent that is absent, or is not an object, array or set,
/// contributes no positions.
fn collect_positions(value: &Value, path: &[Part], out: &mut Vec<Value>) {
	let Some((first, rest)) = path.split_first() else {
		out.push(value.clone());
		return;
	};
	match (first, value) {
		(Part::Field(name), Value::Object(object)) => {
			let child = object.get(name.as_str()).unwrap_or(&Value::None);
			collect_positions(child, rest, out);
		}
		(Part::Field(_), Value::Array(array)) => {
			for element in array.iter() {
				collect_positions(element, path, out);
			}
		}
		(Part::Field(_), Value::Set(set)) => {
			for element in set.iter() {
				collect_positions(element, path, out);
			}
		}
		(Part::All, Value::Array(array)) => {
			for element in array.iter() {
				collect_positions(element, rest, out);
			}
		}
		(Part::All, Value::Set(set)) => {
			for element in set.iter() {
				collect_positions(element, rest, out);
			}
		}
		(Part::All, Value::Object(object)) => {
			for child in object.values() {
				collect_positions(child, rest, out);
			}
		}
		_ => {}
	}
}

/// Write `outputs`, in order, to the positions [`collect_positions`] visits
/// in `slot`. A position left `NONE` is removed: a field from its object, an
/// element from its array or set. A set whose elements changed is normalised
/// again, so elements that became equal merge.
fn write_positions(slot: &mut Value, path: &[Part], outputs: &mut dyn Iterator<Item = Value>) {
	let Some((first, rest)) = path.split_first() else {
		if let Some(output) = outputs.next() {
			*slot = output;
		}
		return;
	};
	match (first, slot) {
		(Part::Field(name), Value::Object(object)) => {
			let mut child = object.remove(name.as_str()).unwrap_or(Value::None);
			write_positions(&mut child, rest, outputs);
			if !child.is_none() {
				object.insert(name.as_str(), child);
			}
		}
		(Part::Field(_), Value::Array(array)) => {
			for element in array.iter_mut() {
				write_positions(element, path, outputs);
			}
		}
		(Part::Field(_), Value::Set(set)) => {
			let mut elements: Vec<Value> =
				std::mem::replace(set, crate::val::Set::new()).into_iter().collect();
			for element in elements.iter_mut() {
				write_positions(element, path, outputs);
			}
			*set = elements.into_iter().collect();
		}
		(Part::All, Value::Array(array)) => {
			for element in array.iter_mut() {
				write_positions(element, rest, outputs);
			}
			if rest.is_empty() {
				array.retain(|element| !element.is_none());
			}
		}
		(Part::All, Value::Set(set)) => {
			let mut elements: Vec<Value> =
				std::mem::replace(set, crate::val::Set::new()).into_iter().collect();
			for element in elements.iter_mut() {
				write_positions(element, rest, outputs);
			}
			*set = elements.into_iter().filter(|element| !element.is_none()).collect();
		}
		(Part::All, Value::Object(object)) => {
			for child in object.values_mut() {
				write_positions(child, rest, outputs);
			}
			if rest.is_empty() {
				object.retain(|_, child| !child.is_none());
			}
		}
		_ => {}
	}
}

/// The frame a transform or a test runs in: privileged, refusing writes and
/// record dereference.
pub(crate) fn migration_frame(ctx: &ExecutionContext) -> ExecutionContext {
	ctx.clone().privileged_frame().migrating()
}

type StepKey = (NamespaceId, DatabaseId, String, Generation);

/// A migration and the step compiled from it.
type CompiledEntry = (Migration, Arc<ReplayStep>);

/// The compiled step of each migration of each table, by the migration it was
/// compiled from. A generation's descriptor is immutable once committed, but a
/// number can name another descriptor after a table is removed and made again,
/// so an entry is served only while the migration a transaction reads equals
/// the one it was compiled from.
static COMPILED: LazyLock<Mutex<HashMap<StepKey, CompiledEntry>>> = LazyLock::new(Default::default);

/// The most steps kept compiled.
const COMPILED_CAPACITY: usize = 4096;

/// The migration steps of `table`, as a read at `version` of the transaction
/// `ctx` runs in sees them, or `None` when no generation of the table's ring
/// may carry one.
///
/// Every read of a table asks this, a record fetch once per link a read
/// follows. A current read answers from the transaction's memo of the table's
/// layouts when it holds one, without a read or an allocation, and the newest
/// migration the head's descriptor names says whether the table has steps.
/// Any other read loads the layouts out of line: in an unoptimised build an
/// awaited future is reserved whole in its caller's frame, and a frame that
/// recurses through a chain of links would hold it once per link.
pub(crate) async fn replay_for(
	ctx: &ExecutionContext,
	table: &TableName,
	version: Option<u64>,
) -> FlowResult<Option<Replay>> {
	if version.is_none()
		&& let Ok(db_ctx) = ctx.database()
	{
		let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
		if let Some(layouts) = ctx.txn().held_table_layouts(ns, db, table) {
			return Ok(Replay::of(&layouts, ns, db));
		}
	}
	crate::exec::vm::out_of_line(|| layouts_replay(ctx, table, version)).await
}

/// [`replay_for`], in line.
async fn layouts_replay(
	ctx: &ExecutionContext,
	table: &TableName,
	version: Option<u64>,
) -> FlowResult<Option<Replay>> {
	let Ok(db_ctx) = ctx.database() else {
		return Ok(None);
	};
	let (ns, db) = (db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id);
	let layouts =
		ctx.txn().table_layouts(ns, db, table, version).await.map_err(ControlFlow::Err)?;
	Ok(Replay::of(&layouts, ns, db))
}

fn poisoned() -> ControlFlow {
	ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(
		"the compiled migration steps were poisoned".to_owned(),
	)))
}

/// `migration`'s step, compiled by `planner`.
async fn compile_step(
	planner: &crate::exec::planner::Planner<'_>,
	migration: &Migration,
) -> FlowResult<ReplayStep> {
	let action = match &migration.step {
		MigrationStep::Rewrite {
			path,
			using,
			fallback,
			test,
		} => StepAction::Rewrite(
			compile_rewrite(planner, &migration.name, path, using, fallback.clone(), test).await?,
		),
		MigrationStep::Remove {
			path,
		} => StepAction::Remove {
			path: step_path(&migration.name, path)?,
		},
		MigrationStep::RemoveUndeclared {
			declared,
		} => StepAction::RemoveUndeclared {
			declared: declared.iter().cloned().collect(),
		},
	};
	Ok(ReplayStep {
		action,
	})
}

/// The segments of the path `path` a step of the migration `name` changes:
/// a field name, then field names and `*`.
fn step_path(name: &str, path: &IdiomText) -> FlowResult<Vec<Part>> {
	let path = path.compile().map_err(ControlFlow::Err)?;
	match path.0.as_slice() {
		[Part::Field(_), rest @ ..]
			if rest.iter().all(|part| matches!(part, Part::Field(_) | Part::All)) =>
		{
			Ok(path.0)
		}
		_ => Err(ControlFlow::Err(anyhow::Error::new(crate::err::EngineError::Internal(format!(
			"migration `{name}` changes a path that is not a field path"
		))))),
	}
}

/// The rewrite step of the migration `name`, compiled by `planner`.
async fn compile_rewrite(
	planner: &crate::exec::planner::Planner<'_>,
	name: &str,
	path: &IdiomText,
	using: &ExprText,
	fallback: Option<Value>,
	test: &MigrationTest,
) -> FlowResult<CompiledStep> {
	let field = path.as_str().to_owned();
	let path = step_path(name, path)?;
	let using = compile_body(planner, using).await?;
	let test = ValueTest::compile(planner, test).await?;
	Ok(CompiledStep {
		name: name.to_owned(),
		path,
		field,
		using,
		fallback,
		test,
	})
}

async fn compile_body(
	planner: &crate::exec::planner::Planner<'_>,
	text: &ExprText,
) -> FlowResult<Clause> {
	let body = LogicalBody::new(text);
	let logical = body.logical().map_err(ControlFlow::Err)?.clone();
	let metadata = planner
		.scalar_metadata(logical.clone())
		.await
		.with_context(|| format!("a migration's clause did not compile: {}", text.as_str()))
		.map_err(ControlFlow::Err)?;
	Ok(Clause::of(logical, metadata))
}

/// Bring `record`, a record of `table` stamped with `stamp`, through the
/// table's pending steps.
pub(crate) async fn replay_record(
	ctx: &ExecutionContext,
	table: &TableName,
	record: &mut Value,
	stamp: Option<Generation>,
) -> FlowResult<()> {
	match replay_for(ctx, table, ctx.version_stamp()).await? {
		Some(replay) => replay.apply(ctx, record, stamp).await,
		None => Ok(()),
	}
}

/// The error a step's failure is, for a caller that holds no control flow: a
/// step runs in a frame that signals nothing but its failure.
pub(crate) fn into_error(flow: ControlFlow) -> anyhow::Error {
	flow.into_error_or(|other| anyhow::anyhow!("a migration step signalled {other:?}"))
}

/// What a `MIGRATE EXISTING ROWS` clause asks of its statement.
pub(crate) struct MigrateRequest<'a> {
	pub(crate) name: &'a str,
	pub(crate) using: &'a crate::expr::Expr,
	pub(crate) fallback: Option<Value>,
}

/// Commit the migration `request` of the field definition `field`, which
/// replaces `previous` on `table`: check the migration as its definition is
/// formed, mint its generation, which carries its step, and queue the rewrite
/// job. Answers `true` when the table has already run the migration under
/// that name, so the statement does nothing.
///
/// The migration's generation is minted before the statement stores `field`;
/// the statement's own stamp mints the generation after it, with the layout
/// the new definitions derive, when that layout differs. On a table that
/// enforces migrations, a `COMPUTED` `previous` has the values its path still
/// holds removed by a step minted before the migration's, so `$value` reads
/// the field as absent in every row.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn define(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	field: &FieldDefinition,
	previous: Option<&FieldDefinition>,
	request: MigrateRequest<'_>,
) -> FlowResult<bool> {
	let refuse = |reason: String| {
		ControlFlow::Err(anyhow::Error::new(MigrationError::Refused {
			field: field.name.to_raw_string(),
			table: table.as_str().to_owned(),
			reason,
		}))
	};
	let txn = ctx.txn();
	let name = match field.name.0.as_slice() {
		[Part::Field(name), rest @ ..]
			if rest.iter().all(|part| matches!(part, Part::Field(_) | Part::All)) =>
		{
			name.as_str().to_owned()
		}
		_ => {
			return Err(refuse(
				"only a field path of names and `*` segments can be migrated".to_owned(),
			));
		}
	};
	if matches!(name.as_str(), "id" | "in" | "out") {
		return Err(refuse(format!("`{name}` is fixed by the record's key")));
	}
	if field.computed.is_some() {
		return Err(refuse("a COMPUTED field is never stored".to_owned()));
	}
	if field.reference.is_some() {
		return Err(refuse("a REFERENCE field's backlinks hold its stored values".to_owned()));
	}
	if field.inline {
		return Err(refuse("an INLINE field's adjacency keys hold its stored values".to_owned()));
	}
	let definition = txn.get_tb(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	if definition.as_ref().is_some_and(|tb| tb.view.is_some()) {
		return Err(refuse("a view's rows skip their field definitions".to_owned()));
	}
	let views = txn.all_tb_views(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	if let Some(view) = views.first() {
		return Err(refuse(format!("the view `{}` selects from the table", view.name)));
	}
	let indexes = txn.all_tb_indexes(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	for index in indexes.iter() {
		let column = |col: &crate::expr::Idiom| paths_meet(&col.0, &field.name.0);
		let guard = |guard: &crate::expr::Cond| reads_field(&guard.0, &name);
		if index.cols.iter().any(column) || index.count_cond.as_ref().is_some_and(guard) {
			return Err(refuse(format!(
				"the index `{}` reads it: remove the index, migrate, then define it again",
				index.name
			)));
		}
	}
	let fields = txn.all_tb_fields(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	if let Some(reason) = derived_keys_above(&field.name.0, &fields) {
		return Err(refuse(reason));
	}
	check_nesting(field, &fields).map_err(refuse)?;
	let schemafull = definition.as_ref().is_some_and(|tb| tb.schemafull);

	let registry = ctx.function_registry();
	let using_body = LogicalBody::of(request.using);
	let using = using_body.logical().map_err(ControlFlow::Err)?;
	// `$value` is of the previous definition's kind only where the table's
	// claims state that every row it holds follows that definition; otherwise
	// the path may hold any value.
	let claimed_previous = match previous {
		Some(previous)
			if previous.computed.is_none() && claimed(&txn, ns, db, table, previous).await? =>
		{
			Some(previous)
		}
		_ => None,
	};
	let guaranteed = claimed_previous.and_then(|previous| previous.field_kind.clone());
	check_transform(using, &guaranteed.clone().unwrap_or(Kind::Any), registry).map_err(refuse)?;
	// The claim vouches that every value passes the previous definition's
	// whole test, with the definitions beneath it folded in as they stand
	// before the statement.
	let vouched = claimed_previous.and_then(|previous| {
		fold_children(
			previous.field_kind.as_ref()?,
			&previous.name.0,
			&fields,
			schemafull,
			flexible_at(previous, &fields),
		)
		.ok()
	});
	let assert = value_assert(field, registry);
	// The definitions beneath the field are folded into the type a migrated
	// value must pass, as the statement leaves them when the step is minted:
	// the ones the new kind implies for the elements of an array or set among
	// them. A field with no `TYPE` folds as `any`, so definitions beneath it
	// are refused rather than left out of the test.
	let folded = fold_children(
		field.field_kind.as_ref().unwrap_or(&Kind::Any),
		&field.name.0,
		&crate::exec::field_rules::fields_once_defined(&fields, field),
		schemafull,
		flexible_at(field, &fields),
	)
	.map_err(|child| {
		refuse(format!(
			"the type of the field `{}` beneath it is not one a migrated value can be checked \
			 against: migrate that field itself",
			child.to_raw_string()
		))
	})?;
	let test = MigrationTest {
		kind: field.field_kind.as_ref().map(|_| KindText::new(&folded)),
		schemafull,
		flexible: field.flexible,
		assert: assert.map(ExprText::new),
	};

	let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let planner = crate::exec::vm::descriptor::unit_planner(ctx, db_ctx);
	let path = IdiomText::new(&field.name);
	let using_text = ExprText::new(request.using);
	let compiled = compile_rewrite(&planner, request.name, &path, &using_text, None, &test).await?;
	let frame = migration_frame(ctx);
	let fallback = match request.fallback {
		Some(fallback) => match compiled.test(&frame, fallback).await? {
			Some(fallback) => Some(fallback),
			None => {
				return Err(refuse("the FALLBACK does not pass the field's definition".to_owned()));
			}
		},
		None => {
			if !proven_total(&compiled, &frame, using, vouched.as_ref()).await? {
				return Err(refuse(
					"FALLBACK may be left out only when the transform cannot fail and its result \
					 always passes the field's definition"
						.to_owned(),
				));
			}
			None
		}
	};
	let step = MigrationStep::Rewrite {
		path,
		using: using_text,
		fallback,
		test,
	};

	let migration = Migration {
		name: request.name.to_owned(),
		step,
	};
	if already_run(&txn, ns, db, table, &migration).await.map_err(refuse)? {
		return Ok(true);
	}
	// A `COMPUTED` predecessor reads nothing stored, but its path can still hold
	// the values rows were written with before it was computed; on a table that
	// enforces migrations the step reads the path cleared of them.
	if previous.is_some_and(|previous| previous.computed.is_some())
		&& table_enforces(ctx, ns, db, table).await?
	{
		mint_removal(ctx, ns, db, table, &field.name).await?;
	}
	txn.mint_tb_migration(ns, db, table, migration).await.map_err(ControlFlow::Err)?;
	// The rewrite job's entry is written with the step, so the job exists
	// exactly when the migration commits. The migrated field is verified in
	// the same lap, so its claim is made once every row follows it.
	txn.queue_tb_rewrite(ns, db, table).await.map_err(ControlFlow::Err)?;
	queue_verification(ctx, ns, db, table, field, None).await?;
	Ok(false)
}

/// Whether `table` has already run the migration `migration` under its name:
/// `true` when the migration is the one run, or the one run is retired and
/// can no longer be compared, and an error when the name was run for another
/// step. The table's record of names outlives the generations that carried
/// them.
async fn already_run(
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	migration: &Migration,
) -> Result<bool, String> {
	let fail = |e: anyhow::Error| e.to_string();
	let runs = txn.tb_migration_runs(ns, db, table).await.map_err(fail)?;
	let Some((_, run)) = runs.iter().find(|(name, _)| *name == migration.name) else {
		return Ok(false);
	};
	let layouts = txn.table_layouts(ns, db, table, None).await.map_err(fail)?;
	let carried = layouts.get(run.generation).and_then(|layout| match layout {
		crate::catalog::GenerationLayout::Readable(descriptor) => descriptor.migration.as_ref(),
		crate::catalog::GenerationLayout::Refused(_) => None,
	});
	match carried {
		Some(carried) if carried.step != migration.step => Err(format!(
			"the table has already run a different migration named `{}`",
			migration.name
		)),
		_ => Ok(true),
	}
}

/// Whether the transform `using`, compiled as `step`, cannot fail and its
/// result always passes the step's test, judged from the definitions alone:
/// a constant that passes; `$value` itself, or a cast of it to a number
/// kind, over a field whose every value the table's claims vouch passes
/// `vouched`, which the step's test, with the definitions beneath the field
/// folded in, accepts.
///
/// `vouched` is the previous definition's whole test, with the definitions
/// beneath it folded in, and only where the claims state that definition;
/// otherwise the path may hold any value.
async fn proven_total(
	step: &CompiledStep,
	frame: &ExecutionContext,
	using: &LogicalExpr,
	vouched: Option<&Kind>,
) -> FlowResult<bool> {
	if let Some(constant) = crate::exec::planner::static_value(using) {
		return Ok(step.test(frame, constant).await?.is_some());
	}
	if step.test.assert.is_some() {
		return Ok(false);
	}
	let Some(old) = vouched else {
		return Ok(false);
	};
	let new = step.test.kind.as_ref();
	let is_value =
		|expr: &LogicalExpr| matches!(expr, LogicalExpr::Param(p) if p.as_str() == "value");
	Ok(if is_value(using) {
		new.is_none_or(|new| old.widens_to(new))
	} else if let LogicalExpr::Prefix {
		op: PrefixOperator::Cast(target),
		expr,
	} = using
		&& is_value(expr)
	{
		old.is_numeric()
			&& matches!(target, Kind::Float | Kind::Number)
			&& new.is_none_or(|new| target.widens_to(new))
	} else {
		false
	})
}

/// Whether the view `view` reads the field `root` of the rows of its source
/// tables: a projection, its `WHERE` or a group key names it, or `SELECT *`
/// takes the whole row. What cannot be told is judged to read it.
fn view_reads_field(view: &crate::catalog::ViewDefinition, root: &str) -> bool {
	use crate::catalog::ViewDefinition;
	use crate::expr::Field;
	let (fields, condition, groups) = match view {
		ViewDefinition::Materialized {
			fields,
			condition,
			..
		} => (fields, condition, None),
		ViewDefinition::Aggregated {
			fields,
			condition,
			groups,
			..
		} => (fields, condition, Some(groups)),
		ViewDefinition::Select {
			fields,
			condition,
			groups,
			..
		} => (fields, condition, groups.as_ref()),
	};
	let selected = match fields {
		crate::expr::Fields::Value(selector) => reads_field(&selector.expr, root),
		crate::expr::Fields::Select(fields) => fields.iter().any(|field| match field {
			Field::All => true,
			Field::Single(selector) => reads_field(&selector.expr, root),
		}),
	};
	selected
		|| condition.as_ref().is_some_and(|condition| reads_field(condition, root))
		|| groups.is_some_and(|groups| {
			groups.0.iter().any(
				|group| !matches!(group.0.0.first(), Some(Part::Field(name)) if name.as_str() != root),
			)
		})
}

/// The reason a change of the values at `path` is refused when it lies within
/// a `REFERENCE` or `INLINE` field of `fields`: such a field is always
/// top-level, and the backlinks or adjacency keys it holds were written from
/// every value beneath it, which a change of one of them does not update. The
/// definition at `path` itself is not one: its own change is judged where it is
/// made.
fn derived_keys_above(path: &[Part], fields: &[FieldDefinition]) -> Option<String> {
	fields.iter().find_map(|fd| {
		let name = fd.name.0.as_slice();
		if name.len() >= path.len() || !path.starts_with(name) {
			return None;
		}
		if fd.reference.is_some() {
			return Some(format!(
				"it lies within the REFERENCE field `{}`, whose backlinks hold its stored values",
				fd.name.to_raw_string()
			));
		}
		fd.inline.then(|| {
			format!(
				"it lies within the INLINE field `{}`, whose adjacency keys hold its stored values",
				fd.name.to_raw_string()
			)
		})
	})
}

/// Refuse the definition of `field`, migrated, when it no longer fits the
/// definitions around it: its parent's type must still hold it, a literal
/// shape its parent declares must accept its type, and its own type must
/// still hold every definition beneath it and accept their types where it
/// declares a literal shape. A definition beneath it that has nowhere to
/// live must be removed before it is migrated.
fn check_nesting(field: &FieldDefinition, fields: &[FieldDefinition]) -> Result<(), String> {
	let path = field.name.0.as_slice();
	if let Some((segment, parent_path)) = path.split_last()
		&& !parent_path.is_empty()
		&& let Some(parent) = fields.iter().find(|fd| fd.name.0.as_slice() == parent_path)
		&& let Some(kind) = &parent.field_kind
	{
		let parent_name = parent.name.to_raw_string();
		if !holds(kind, segment, parent.flexible) {
			return Err(format!("its parent `{parent_name}` has a type that cannot hold it"));
		}
		if let (Some(slot), Some(child)) = (literal_slot(kind, segment), &field.field_kind)
			&& !child.widens_to(slot)
		{
			return Err(format!(
				"its type does not fit the shape its parent `{parent_name}` declares for it"
			));
		}
	}
	let Some(kind) = &field.field_kind else {
		return Ok(());
	};
	for child in
		fields.iter().filter(|fd| fd.name.0.len() > path.len() && fd.name.0.starts_with(path))
	{
		let segment = &child.name.0[path.len()];
		let child_name = child.name.to_raw_string();
		if !holds(kind, segment, field.flexible) {
			return Err(format!(
				"the field `{child_name}` beneath it would have nowhere to live: remove it first"
			));
		}
		if child.name.0.len() == path.len() + 1
			&& let (Some(slot), Some(child_kind)) = (literal_slot(kind, segment), &child.field_kind)
			&& !child_kind.widens_to(slot)
		{
			return Err(format!(
				"the shape it declares does not accept the type of the field `{child_name}` beneath it"
			));
		}
	}
	Ok(())
}

/// Whether a value of `kind` can hold a child at `segment`: an object holds
/// any key, a literal object the keys it declares (any key when `FLEXIBLE`),
/// an array or set holds `*` and spreads a key over its elements.
fn holds(kind: &Kind, segment: &Part, flexible: bool) -> bool {
	use crate::expr::kind::KindLiteral;
	match kind {
		Kind::Any | Kind::Named(_) | Kind::Object => true,
		Kind::Either(arms) => arms.iter().any(|arm| holds(arm, segment, flexible)),
		Kind::Array(inner, _) | Kind::Set(inner, _) => {
			matches!(segment, Part::All) || holds(inner, segment, flexible)
		}
		Kind::Literal(KindLiteral::Object(shape)) => {
			if let Part::Field(key) = segment {
				flexible || shape.keys().any(|k| k.as_str() == key.as_str())
			} else {
				matches!(segment, Part::All)
			}
		}
		Kind::Literal(KindLiteral::Array(_)) => matches!(segment, Part::All),
		Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
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
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => false,
	}
}

/// The type a literal object shape within `kind` declares for the key at
/// `segment`, looking through unions and the elements of arrays and sets.
fn literal_slot<'a>(kind: &'a Kind, segment: &Part) -> Option<&'a Kind> {
	use crate::expr::kind::KindLiteral;
	match kind {
		Kind::Literal(KindLiteral::Object(shape)) => {
			if let Part::Field(key) = segment {
				shape.iter().find(|(k, _)| k.as_str() == key.as_str()).map(|(_, kind)| kind)
			} else {
				None
			}
		}
		Kind::Either(arms) => arms.iter().find_map(|arm| literal_slot(arm, segment)),
		Kind::Array(inner, _) | Kind::Set(inner, _) if matches!(segment, Part::Field(_)) => {
			literal_slot(inner, segment)
		}
		Kind::Any
		| Kind::Named(_)
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
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
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => None,
	}
}

/// The `ASSERT` of `field` when it reads `$value` alone, so a test can apply
/// it to a value without the row the value is in. It judges a value already
/// coerced to the field's type.
fn value_assert<'a>(
	field: &'a FieldDefinition,
	registry: &crate::exec::function::FunctionRegistry,
) -> Option<&'a crate::expr::Expr> {
	let kind = field.field_kind.clone().unwrap_or(Kind::Any);
	field.assert.as_ref().filter(|assert| {
		LogicalBody::of(assert)
			.logical()
			.ok()
			.is_some_and(|assert| check_transform(assert, &kind, registry).is_ok())
	})
}

/// Whether a value at `field` may hold keys no definition among `fields`
/// declares beneath it: the field or one of its ancestors is `FLEXIBLE`.
fn flexible_at(field: &FieldDefinition, fields: &[FieldDefinition]) -> bool {
	field.flexible
		|| fields.iter().any(|ancestor| {
			ancestor.flexible
				&& ancestor.name.0.len() < field.name.0.len()
				&& field.name.0.starts_with(&ancestor.name.0)
		})
}

/// What a verification pass judges each stored value of `field` against, on
/// a table whose definitions are `fields` and which is `SCHEMAFULL` when
/// `schemafull`: the whole test a migration step of the field would apply,
/// its type with the definitions beneath it folded in ([`fold_children`])
/// and its `ASSERT` over `$value`, compiled in the database context `ctx`.
///
/// The second element is whether the test is the whole definition. It is not
/// when a definition beneath the field has a type the fold cannot carry, or
/// the `ASSERT` reads more than `$value`; the test is then the field's type
/// alone, and what it finds states no claim.
pub(crate) async fn verification_test(
	ctx: &ExecutionContext,
	fields: &[FieldDefinition],
	field: &FieldDefinition,
	schemafull: bool,
) -> FlowResult<(ValueTest, bool)> {
	let assert = value_assert(field, ctx.function_registry());
	let folded = field.field_kind.as_ref().map(|kind| {
		fold_children(kind, &field.name.0, fields, schemafull, flexible_at(field, fields))
	});
	let whole = field.assert.is_none() || assert.is_some();
	let (test, whole) = match folded {
		Some(Ok(folded)) => (
			MigrationTest {
				kind: Some(KindText::new(&folded)),
				schemafull,
				flexible: field.flexible,
				assert: assert.map(ExprText::new),
			},
			whole,
		),
		_ => (
			MigrationTest {
				kind: field.field_kind.as_ref().map(KindText::new),
				schemafull,
				flexible: field.flexible,
				assert: None,
			},
			false,
		),
	};
	let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let planner = crate::exec::vm::descriptor::unit_planner(ctx, db_ctx);
	Ok((ValueTest::compile(&planner, &test).await?, whole))
}

/// `kind`, the type of the field at `path`, with the definitions beneath it
/// folded in, recursively: what a value migrated to the field must pass to
/// satisfy those definitions. Errs with the name of a definition beneath the
/// field whose type the result cannot carry, since a test that left it out
/// would pass values the definition does not admit.
///
/// The definition of the elements (`path.*`) becomes the element type of an
/// array or set. The definitions of keys become a literal object shape in
/// place of `object` or an `object` arm of a union, and narrow the slots of a
/// literal object type. A shape refuses keys it does not declare, so keys fold
/// into `object` only where the stored value may hold no other key: on a
/// `SCHEMAFULL` table, beneath a field that is not `FLEXIBLE`. An `object`
/// there with no definition beneath it folds to the empty shape, which admits
/// no key. No type carries keys beneath an object that may hold others, the
/// elements of an object, keys spread over the elements of an array or set, a
/// definition beneath `any`, or a part other than a key or `*`.
fn fold_children(
	kind: &Kind,
	path: &[Part],
	fields: &[FieldDefinition],
	schemafull: bool,
	flexible: bool,
) -> Result<Kind, crate::expr::Idiom> {
	let open = !schemafull || flexible;
	let mut keys = std::collections::BTreeMap::new();
	let mut key_name = None;
	let mut elements = None;
	for child in fields.iter().filter(|fd| {
		fd.computed.is_none() && fd.name.0.len() == path.len() + 1 && fd.name.0.starts_with(path)
	}) {
		let child_kind = child.field_kind.clone().unwrap_or(Kind::Any);
		let folded = fold_children(&child_kind, &child.name.0, fields, schemafull, child.flexible)?;
		match child.name.0.last() {
			Some(Part::Field(key)) if !open => {
				key_name.get_or_insert_with(|| child.name.clone());
				keys.insert(key.clone(), folded);
			}
			// A key of any type beneath an open object constrains nothing.
			Some(Part::Field(_)) if matches!(folded, Kind::Any) => {}
			Some(Part::All) => elements = Some((child.name.clone(), folded)),
			_ => return Err(child.name.clone()),
		}
	}
	let element_name = elements.as_ref().map(|(name, _)| name);
	let fold = |arm: &Kind| match arm {
		Kind::Array(inner, len) | Kind::Set(inner, len) => {
			if let Some(name) = &key_name {
				return Err(name.clone());
			}
			let inner = match &elements {
				Some((_, element)) => Box::new(element.clone()),
				None => inner.clone(),
			};
			Ok(if let Kind::Set(..) = arm {
				Kind::Set(inner, *len)
			} else {
				Kind::Array(inner, *len)
			})
		}
		Kind::Object => match element_name {
			Some(name) => Err(name.clone()),
			None if open => Ok(Kind::Object),
			None => Ok(Kind::Literal(KindLiteral::Object(keys.clone()))),
		},
		Kind::Literal(KindLiteral::Object(shape)) => match element_name {
			Some(name) => Err(name.clone()),
			None => {
				let mut shape = shape.clone();
				shape.extend(keys.clone());
				Ok(Kind::Literal(KindLiteral::Object(shape)))
			}
		},
		Kind::Any | Kind::Named(_) | Kind::Literal(_) => match element_name.or(key_name.as_ref()) {
			Some(name) => Err(name.clone()),
			None => Ok(arm.clone()),
		},
		other @ (Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
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
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Either(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::File(_)
		| Kind::Enum(_)) => Ok(other.clone()),
	};
	if let Kind::Either(arms) = kind {
		Ok(Kind::Either(arms.iter().map(fold).collect::<Result<_, _>>()?))
	} else {
		fold(kind)
	}
}

/// Commit `ALTER TABLE … MIGRATE EXISTING ROWS AS <name> REMOVE UNDECLARED
/// FIELDS` on `table`: mint a step that clears, from every row stored before
/// it, each top-level field no definition of the table declares now. Answers
/// `false` when the table has already run a migration of that kind under
/// `name`, so the statement does nothing.
///
/// The declared names are the roots of the table's field definitions as they
/// stand when the step is minted ([`declared_roots`]); the record's own keys
/// ([`is_record_key`]) are always kept. An index reading a name the step
/// clears is refused, a `COUNT` index reading it in its `WHERE` guard as much
/// as one with the name as a column. A check for undeclared keys queued on the
/// table runs again over the cleared rows.
pub(crate) async fn remove_undeclared(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	name: &str,
) -> FlowResult<bool> {
	let refuse = |reason: String| {
		ControlFlow::Err(anyhow::Error::new(MigrationError::RefusedTable {
			table: table.as_str().to_owned(),
			reason,
		}))
	};
	let txn = ctx.txn();
	let runs = txn.tb_migration_runs(ns, db, table).await.map_err(ControlFlow::Err)?;
	if let Some((_, run)) = runs.iter().find(|(run, _)| run == name) {
		let layouts = txn.table_layouts(ns, db, table, None).await.map_err(ControlFlow::Err)?;
		let carried = layouts.get(run.generation).and_then(|layout| match layout {
			crate::catalog::GenerationLayout::Readable(descriptor) => descriptor.migration.as_ref(),
			crate::catalog::GenerationLayout::Refused(_) => None,
		});
		return match carried {
			Some(carried) if !matches!(carried.step, MigrationStep::RemoveUndeclared { .. }) => {
				Err(refuse(format!(
					"the table has already run a different migration named `{name}`"
				)))
			}
			_ => Ok(false),
		};
	}
	let definition = txn.get_tb(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	if definition.as_ref().is_some_and(|tb| tb.view.is_some()) {
		return Err(refuse("a view's rows skip their field definitions".to_owned()));
	}
	let views = txn.all_tb_views(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	if let Some(view) = views.first() {
		return Err(refuse(format!("the view `{}` selects from the table", view.name)));
	}
	let fields = txn.all_tb_fields(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	let declared = declared_roots(fields.iter());
	let indexes = txn.all_tb_indexes(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	let cleared = |root: &str| {
		!is_record_key(root) && declared.binary_search_by(|d| d.as_str().cmp(root)).is_err()
	};
	for index in indexes.iter() {
		let clears =
			index.cols.iter().any(
				|col| matches!(col.0.first(), Some(Part::Field(root)) if cleared(root.as_str())),
			) || index.count_cond.as_ref().is_some_and(|guard| reads_any_field(&guard.0, cleared));
		if clears {
			return Err(refuse(format!(
				"the index `{}` reads a field no definition declares: remove the index first",
				index.name
			)));
		}
	}
	txn.mint_tb_migration(
		ns,
		db,
		table,
		Migration {
			name: name.to_owned(),
			step: MigrationStep::RemoveUndeclared {
				declared: declared.clone(),
			},
		},
	)
	.await
	.map_err(ControlFlow::Err)?;
	txn.queue_tb_rewrite(ns, db, table).await.map_err(ControlFlow::Err)?;
	txn.recheck_tb_undeclared(ns, db, table, declared, true).await.map_err(ControlFlow::Err)?;
	Ok(true)
}

/// The names the definitions `fields` declare at the top level, sorted and
/// without repeats: the first part of each definition's path. A step that
/// removes the undeclared fields keeps these ([`remove_undeclared`]), and the
/// check for undeclared keys judges a row against them
/// ([`holds_undeclared_key`]).
pub(crate) fn declared_roots<'a>(fields: impl Iterator<Item = &'a FieldDefinition>) -> Vec<String> {
	let mut declared: Vec<String> = fields
		.filter_map(|fd| match fd.name.0.first() {
			Some(Part::Field(root)) => Some(root.as_str().to_owned()),
			_ => None,
		})
		.collect();
	declared.sort();
	declared.dedup();
	declared
}

/// Whether `row` holds a top-level key that a write of it on a `SCHEMAFULL`
/// table refuses: one `declared`, sorted, does not name, other than the
/// record's own keys ([`is_record_key`]).
pub(crate) fn holds_undeclared_key(row: &crate::val::Object, declared: &[String]) -> bool {
	row.keys().any(|key| {
		!is_record_key(key.as_str())
			&& declared.binary_search_by(|name| name.as_str().cmp(key.as_str())).is_err()
	})
}

/// Run the check for undeclared keys queued on `table` again when the names
/// the table's definitions declare at the top level once the statement's
/// change stands — `declared`, given the definitions stored now — are not the
/// ones it judges rows against: the rows it counted were judged against names
/// that no longer stand. A table with no check queued reads no definitions.
async fn recheck_undeclared(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	declared: impl FnOnce(&[FieldDefinition]) -> Vec<String>,
) -> FlowResult<()> {
	let txn = ctx.txn();
	let job = txn.get_tb_migration_job(ns, db, table).await.map_err(ControlFlow::Err)?;
	if job.is_none_or(|job| job.undeclared.is_none()) {
		return Ok(());
	}
	let fields = txn.all_tb_fields(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	txn.recheck_tb_undeclared(ns, db, table, declared(&fields), false)
		.await
		.map_err(ControlFlow::Err)
}

/// Refuse an index on `table` over `columns` while a step that clears the
/// fields no definition declared is pending: until the rewrite job has
/// passed every row, the index would read values a read of the row does not
/// see.
pub(crate) async fn check_index_columns(
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	columns: &[crate::expr::Idiom],
) -> anyhow::Result<()> {
	// A step is pending while the rewrite job has rows left to pass through it.
	if !txn.get_tb_migration_job(ns, db, table).await?.is_some_and(|job| job.rewrite) {
		return Ok(());
	}
	let layouts = txn.table_layouts(ns, db, table, None).await?;
	for (_, migration) in layouts.pending_migrations(None).map_err(crate::catalog::Error::Layout)? {
		let MigrationStep::RemoveUndeclared {
			declared,
		} = &migration.step
		else {
			continue;
		};
		for column in columns {
			if let Some(Part::Field(root)) = column.0.first()
				&& !is_record_key(root.as_str())
				&& !declared.iter().any(|d| d.as_str() == root.as_str())
			{
				return Err(anyhow::Error::new(MigrationError::RefusedIndex {
					table: table.as_str().to_owned(),
					column: column.to_raw_string(),
					name: migration.name.clone(),
				}));
			}
		}
	}
	Ok(())
}

/// Refuse an index on `table` defined in a transaction that has run a
/// migration of the table it has not committed.
///
/// A build reads the table's rows in transactions of its own, which see the
/// table's ring as committed, as one `committed` opens does: a step minted by this
/// transaction is not in it, so the build would index the values the rows held
/// before the step, while every read after the commit replays it. A migration
/// this transaction ran is one its record of the table's runs holds and a
/// committed read does not. A table that has run no migration costs no
/// committed read.
pub(crate) async fn check_index_build_sees_migrations(
	txn: &crate::kvs::Transaction,
	committed: &crate::kvs::TransactionFactory,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> anyhow::Result<()> {
	let runs = txn.tb_migration_runs(ns, db, table).await?;
	if runs.is_empty() {
		return Ok(());
	}
	let read = committed.transaction(crate::kvs::TransactionType::Read, txn.sequences()).await?;
	let seen = read.tb_migration_runs(ns, db, table).await;
	read.cancel().await?;
	let seen: HashSet<String> = seen?.into_iter().map(|(name, _)| name).collect();
	if let Some((name, _)) = runs.into_iter().find(|(name, _)| !seen.contains(name)) {
		return Err(anyhow::Error::new(MigrationError::RefusedBuild {
			table: table.as_str().to_owned(),
			name,
		}));
	}
	Ok(())
}

/// A record and the generation its bytes were stamped with.
type StampedRecord = (Arc<crate::catalog::Record>, Option<Generation>);

/// [`crate::catalog::providers::TableProvider::get_record`] as a read sees the
/// record: the record and the generation its bytes were stamped with, to be
/// brought through the steps of its table by [`replay_record`]. The read is
/// held on the heap, as the provider's is, so that no frame above it reserves
/// its future whole.
pub(crate) fn stamped_record<'a>(
	txn: &'a crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &'a TableName,
	id: &'a crate::val::RecordIdKey,
	version: Option<u64>,
) -> std::pin::Pin<Box<impl std::future::Future<Output = anyhow::Result<StampedRecord>> + 'a>> {
	Box::pin(txn.get_stamped_record(ns, db, table, id, version))
}

/// [`stamped_record`] as a locked read, registering the record key for
/// commit-time conflict detection.
pub(crate) fn stamped_record_for_update<'a>(
	txn: &'a crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &'a TableName,
	id: &'a crate::val::RecordIdKey,
) -> std::pin::Pin<Box<impl std::future::Future<Output = anyhow::Result<StampedRecord>> + 'a>> {
	Box::pin(txn.get_stamped_record_for_update(ns, db, table, id))
}

/// [`crate::catalog::providers::TableProvider::get_records`] as a read sees
/// the records: each brought through the migration steps of its table it has
/// not passed. A batch no table of which has a migration is answered as the
/// transaction holds it, and so is a record already current.
pub(crate) async fn get_records(
	ctx: &ExecutionContext,
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	rids: &[crate::val::RecordId],
	version: Option<u64>,
	policy: crate::kvs::CachePolicy,
) -> FlowResult<Vec<Arc<crate::catalog::Record>>> {
	let mut replays: Vec<(&TableName, Option<Replay>)> = Vec::new();
	let mut migrated = false;
	for rid in rids {
		if replays.iter().any(|(table, _)| **table == rid.table) {
			continue;
		}
		let replay = replay_for(ctx, &rid.table, version).await?;
		migrated |= replay.is_some();
		replays.push((&rid.table, replay));
	}
	if !migrated {
		return txn.get_records(ns, db, rids, version, policy).await.map_err(ControlFlow::Err);
	}
	crate::exec::vm::out_of_line(|| {
		migrated_records(ctx, txn, ns, db, rids, version, policy, replays)
	})
	.await
}

/// [`get_records`] for a batch some table of which has migration steps, given
/// the steps of each table the batch names.
#[allow(clippy::too_many_arguments)]
async fn migrated_records(
	ctx: &ExecutionContext,
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	rids: &[crate::val::RecordId],
	version: Option<u64>,
	policy: crate::kvs::CachePolicy,
	replays: Vec<(&TableName, Option<Replay>)>,
) -> FlowResult<Vec<Arc<crate::catalog::Record>>> {
	let stamped =
		txn.get_stamped_records(ns, db, rids, version, policy).await.map_err(ControlFlow::Err)?;
	let mut out = Vec::with_capacity(stamped.len());
	for (rid, (record, stamp)) in rids.iter().zip(stamped) {
		let replay = replays
			.iter()
			.find(|(table, _)| **table == rid.table)
			.and_then(|(_, replay)| replay.as_ref());
		match replay {
			Some(replay) if replay.behind(stamp) && !record.data.is_none() => {
				let mut record = crate::catalog::Record::clone(&record);
				replay.apply(ctx, &mut record.data, stamp).await?;
				out.push(Arc::new(record));
			}
			_ => out.push(record),
		}
	}
	Ok(out)
}

/// Whether `a` and `b` are the same value in the same representation: a
/// number of one variant is not identical to an equal number of another, nor
/// is a collection that holds one. A set orders its elements, so two identical
/// sets hold identical elements at each position.
fn identical(a: &Value, b: &Value) -> bool {
	match (a, b) {
		(Value::Number(a), Value::Number(b)) => {
			std::mem::discriminant(a) == std::mem::discriminant(b) && a == b
		}
		(Value::Array(a), Value::Array(b)) => {
			a.len() == b.len() && a.iter().zip(b.iter()).all(|(a, b)| identical(a, b))
		}
		(Value::Set(a), Value::Set(b)) => {
			a.len() == b.len() && a.iter().zip(b.iter()).all(|(a, b)| identical(a, b))
		}
		(Value::Object(a), Value::Object(b)) => {
			a.len() == b.len()
				&& a.iter().all(|(key, a)| b.get(key.as_str()).is_some_and(|b| identical(a, b)))
		}
		(a, b) => std::mem::discriminant(a) == std::mem::discriminant(b) && a == b,
	}
}

/// A digest of `field`'s stored definition: a verification pass promotes the
/// field's claim only while its definition still has the digest the pass
/// started under ([`crate::catalog::definition_digest`]).
pub(crate) fn definition_digest(field: &FieldDefinition) -> anyhow::Result<i64> {
	crate::catalog::definition_digest(&field.to_stored())
}

/// Whether the field definition `field` is one a verification pass can
/// promote: a stored top-level field with a type, other than the key's.
fn verifiable(field: &FieldDefinition) -> bool {
	field.computed.is_none()
		&& field.field_kind.is_some()
		&& matches!(
			field.name.0.as_slice(),
			[crate::expr::Part::Field(name)] if !matches!(name.as_str(), "id" | "in" | "out")
		)
}

/// Whether the claims of `table` state that every row it holds stamped with a
/// generation of its ring has, at every occupied path of `field`, a value the
/// definition `field` coerced.
async fn claimed(
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	field: &FieldDefinition,
) -> FlowResult<bool> {
	let Some(conformance) =
		txn.table_conformance(ns, db, table, None).await.map_err(ControlFlow::Err)?
	else {
		return Ok(false);
	};
	Ok(conformance
		.claim(&field.name.to_raw_string())
		.is_some_and(|claim| claim.holds_for(&field.to_stored())))
}

/// Queue a verification pass of `field`, just stored on `table` over
/// `previous`, when the change could leave stored values that do not conform:
/// a new field, or one whose type, `FLEXIBLE` or `ASSERT` changed. A
/// definition restated unchanged, one on a table with no rows, and any on a
/// store at the Legacy feature level, queues nothing.
///
/// A definition beneath another field starts a pass under way on the table
/// over instead ([`restart_beneath`]). A definition that declares a name at
/// the top level no definition declared runs the table's check for undeclared
/// keys again ([`recheck_undeclared`]).
pub(crate) async fn queue_verification(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	field: &FieldDefinition,
	previous: Option<&FieldDefinition>,
) -> FlowResult<()> {
	restart_beneath(ctx, ns, db, table, field).await?;
	// A migration queues the verification before the statement stores `field`.
	recheck_undeclared(ctx, ns, db, table, |fields| {
		declared_roots(fields.iter().chain(std::iter::once(field)))
	})
	.await?;
	if !verifiable(field) {
		return Ok(());
	}
	if previous.is_some_and(|previous| {
		previous.field_kind == field.field_kind
			&& previous.flexible == field.flexible
			&& previous.assert == field.assert
	}) {
		return Ok(());
	}
	let txn = ctx.txn();
	// A store at the Legacy feature level cannot state a claim, so a verification there has no
	// outcome to record.
	if txn.write_level().is_legacy() {
		return Ok(());
	}
	if txn
		.get_tb(ns, db, table, None)
		.await
		.map_err(ControlFlow::Err)?
		.is_some_and(|tb| tb.view.is_some())
	{
		return Ok(());
	}
	// A table with no rows has nothing to verify. No claim is made for its
	// field: a transaction older than this statement may still commit a row
	// the definition never coerced.
	if !txn.tb_has_records(ns, db, table).await.map_err(ControlFlow::Err)? {
		return Ok(());
	}
	let digest = definition_digest(field).map_err(ControlFlow::Err)?;
	txn.queue_tb_verification(
		ns,
		db,
		table,
		crate::key::table_migration::VerifiedField {
			name: field.name.to_raw_string(),
			definition: digest,
			failures: 0,
			finished: false,
		},
	)
	.await
	.map_err(ControlFlow::Err)
}

/// Start the verification pass under way on `table` over, when `field`, just
/// stored or removed, is beneath another field: a pass judges a field against
/// its type with the definitions beneath it folded in
/// ([`verification_test`]), so the rows it has passed were judged against
/// definitions that no longer stand.
async fn restart_beneath(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	field: &FieldDefinition,
) -> FlowResult<()> {
	if field.name.0.len() < 2 {
		return Ok(());
	}
	ctx.txn().restart_tb_verification(ns, db, table).await.map_err(ControlFlow::Err)
}

/// The `migrations` section of `INFO FOR TABLE` for `table`: the table's
/// current generation, each migration it has run with the generation it
/// minted and the field it rewrote, each field whose claim states that every
/// stamped row follows its definition, the progress of a rewrite lap under way
/// (`{lap, scanned, rewritten, fallbacks}`), each field under or after
/// verification with the rows that did not conform, and the check for rows
/// holding a top-level key no definition declares (`undeclared`, as
/// `{failures, finished}`). `None` for a table that has run no migration,
/// does not enforce migrations ([`enforces_migrations`]) and has no
/// verification that found a non-conforming row, whose answer keeps its
/// shape.
pub(crate) async fn info_section(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> FlowResult<Option<Value>> {
	let txn = ctx.txn();
	let mut runs = txn.tb_migration_runs(ns, db, table).await.map_err(ControlFlow::Err)?;
	let job = txn.get_tb_migration_job(ns, db, table).await.map_err(ControlFlow::Err)?;
	// A field merely queued for verification is not something to show: a
	// default table's answer keeps its shape until a migration runs or a
	// verification finds a row that does not conform.
	let required = table_enforces(ctx, ns, db, table).await?;
	let failing = job.as_ref().is_some_and(|job| {
		job.verify.iter().any(|field| field.failures > 0)
			|| job.undeclared.as_ref().is_some_and(|check| check.failures > 0)
	});
	if runs.is_empty() && !required && !failing {
		return Ok(None);
	}
	let layouts = txn.table_layouts(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	let generation = layouts.head().map_or(0, Generation::get);
	runs.sort_by_key(|(_, run)| run.generation.get());
	let steps: Vec<Value> = runs
		.iter()
		.map(|(name, run)| {
			let mut step = crate::val::Object::default();
			step.insert("name", Value::from(name.as_str()));
			step.insert("generation", Value::from(i64::from(run.generation.get())));
			if let Some(field) = &run.field {
				step.insert("field", Value::from(field.as_str()));
			}
			Value::Object(step)
		})
		.collect();
	let fields = txn.all_tb_fields(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	let mut guaranteed: Vec<Value> = Vec::new();
	for fd in fields.iter() {
		if fd.key_part.is_none() && claimed(&txn, ns, db, table, fd).await? {
			guaranteed.push(Value::from(fd.name.to_raw_string()));
		}
	}
	let mut section = crate::val::Object::default();
	if let Some(job) = job {
		if job.rewrite {
			let mut progress = crate::val::Object::default();
			progress.insert("lap", Value::from(job.lap as i64));
			progress.insert("scanned", Value::from(job.scanned as i64));
			progress.insert("rewritten", Value::from(job.rewritten as i64));
			progress.insert("fallbacks", Value::from(job.fallbacks as i64));
			section.insert("progress", Value::Object(progress));
		}
		let verifying: Vec<Value> = job
			.verify
			.iter()
			.map(|field| {
				let mut entry = crate::val::Object::default();
				entry.insert("field", Value::from(field.name.as_str()));
				entry.insert("failures", Value::from(field.failures as i64));
				entry.insert("finished", Value::Bool(field.finished));
				Value::Object(entry)
			})
			.collect();
		if !verifying.is_empty() {
			section.insert("verification", Value::Array(verifying.into()));
		}
		if let Some(check) = &job.undeclared {
			let mut entry = crate::val::Object::default();
			entry.insert("failures", Value::from(check.failures as i64));
			entry.insert("finished", Value::Bool(check.finished));
			section.insert("undeclared", Value::Object(entry));
		}
	}
	section.insert("generation", Value::from(i64::from(generation)));
	section.insert("migrations", Value::Array(steps.into()));
	section.insert("guaranteed", Value::Array(guaranteed.into()));
	Ok(Some(Value::Object(section)))
}

/// Whether the field definition `field` changed what a stored value must be
/// over `previous`: its type, its `FLEXIBLE`, or whether it is stored.
fn shape_changed(field: &FieldDefinition, previous: Option<&FieldDefinition>) -> bool {
	previous.is_none_or(|previous| {
		previous.field_kind != field.field_kind
			|| previous.flexible != field.flexible
			|| previous.computed.is_some() != field.computed.is_some()
	})
}

/// Whether every value `previous` accepts, `field` accepts unchanged: its
/// type is one `previous`'s widens to, and it does not stop being
/// `FLEXIBLE`.
fn widens(previous: &FieldDefinition, field: &FieldDefinition) -> bool {
	(field.flexible || !previous.flexible)
		&& match (&previous.field_kind, &field.field_kind) {
			(_, None) => true,
			(None, Some(new)) => matches!(new, Kind::Any),
			(Some(old), Some(new)) => old.widens_to(new),
		}
}

/// Whether the paths `a` and `b` can reach a common value of a row: one is a
/// prefix of the other, or they meet a part other than a field name before
/// either names a field the other does not. Two paths are apart only where
/// they name different fields at the same depth, so `tags[0]` and `tags.*`
/// meet, and `meta.tags` and `meta.score` do not.
fn paths_meet(a: &[Part], b: &[Part]) -> bool {
	for pair in a.iter().zip(b) {
		match pair {
			(Part::Field(a), Part::Field(b)) if a == b => {}
			(Part::Field(_), Part::Field(_)) => return false,
			_ => return true,
		}
	}
	true
}

/// Whether the expression `expr` reads the field `name` of the row it is
/// judged over. An expression that does not lower, or that reads the whole
/// row, is judged to read it.
fn reads_field(expr: &crate::expr::Expr, name: &str) -> bool {
	reads_any_field(expr, |field| field == name)
}

/// Whether the expression `expr` reads a top-level field of the row it is
/// judged over that `wanted` accepts. An expression that does not lower, or
/// that reads the whole row, is judged to read one.
fn reads_any_field(expr: &crate::expr::Expr, wanted: impl Fn(&str) -> bool) -> bool {
	let body = LogicalBody::of(expr);
	let Ok(logical) = body.logical() else {
		return true;
	};
	let mut reads = false;
	let _ = logical.visit_deep(&mut |expr| {
		let hit = if let LogicalExpr::Field(field) = expr {
			wanted(field.as_str())
		} else {
			matches!(expr, LogicalExpr::Doc | LogicalExpr::Current)
		};
		if hit {
			reads = true;
			return std::ops::ControlFlow::Break(());
		}
		std::ops::ControlFlow::Continue(())
	});
	reads
}

/// Whether the expression `expr` reads `$value`.
fn reads_value(expr: &crate::expr::Expr) -> bool {
	let body = LogicalBody::of(expr);
	let Ok(logical) = body.logical() else {
		// An expression that does not lower is judged to read it.
		return true;
	};
	let mut reads = false;
	let _ = logical.visit_deep(&mut |expr| {
		if matches!(expr, LogicalExpr::Param(p) if p.as_str() == "value") {
			reads = true;
			return std::ops::ControlFlow::Break(());
		}
		std::ops::ControlFlow::Continue(())
	});
	reads
}

/// The clauses an `ALTER FIELD` that changed a field's type left as they
/// were: whether it kept its `VALUE`, and whether it kept its `ASSERT`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct KeptClauses {
	pub(crate) value: bool,
	pub(crate) assert: bool,
}

/// What a field statement the host applies asks of the rows its table
/// stores: the `MIGRATE EXISTING ROWS` clause it carries, and the clauses a
/// changed type leaves as they were. The host judges it around the write of
/// the definition as the lowered statement does: [`Self::before_store`] once
/// every other check of the definition has passed, [`Self::after_store`] once
/// the definition is written.
#[derive(Default)]
pub(crate) struct FieldMigration<'a> {
	pub(crate) request: Option<MigrateRequest<'a>>,
	pub(crate) kept: KeptClauses,
}

/// What [`FieldMigration::before_store`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldStore {
	/// The statement writes the definition; `migrated` is whether it
	/// committed a migration, which stands in for a verification pass.
	Write {
		migrated: bool,
	},
	/// The table has already run the migration under its name, so the
	/// statement writes nothing.
	AlreadyRun,
}

impl FieldMigration<'_> {
	/// Judge `field`, about to replace `previous` on `table`, against the
	/// rules of a table that enforces migrations ([`check_field_change`]),
	/// then commit the migration the statement carries ([`define`]). Either
	/// can mint a step, so nothing that can refuse the statement follows. An
	/// import judges the clauses of the definition but mints no step.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn before_store(
		self,
		ctx: &ExecutionContext,
		import: bool,
		ns: NamespaceId,
		db: DatabaseId,
		table: &TableName,
		field: &FieldDefinition,
		previous: Option<&FieldDefinition>,
	) -> anyhow::Result<FieldStore> {
		check_field_change(
			ctx,
			ns,
			db,
			table,
			field,
			previous,
			self.request.is_some(),
			self.kept,
			import,
		)
		.await
		.map_err(into_error)?;
		let Some(request) = self.request else {
			return Ok(FieldStore::Write {
				migrated: false,
			});
		};
		Ok(if define(ctx, ns, db, table, field, previous, request).await.map_err(into_error)? {
			FieldStore::AlreadyRun
		} else {
			FieldStore::Write {
				migrated: true,
			}
		})
	}

	/// Queue a verification pass of `field`, written with no migration over
	/// `previous` on `table` ([`queue_verification`]); an import, which
	/// restores definitions as they were, queues none.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn after_store(
		stored: FieldStore,
		ctx: &ExecutionContext,
		import: bool,
		ns: NamespaceId,
		db: DatabaseId,
		table: &TableName,
		field: &FieldDefinition,
		previous: Option<&FieldDefinition>,
	) -> anyhow::Result<()> {
		if import
			|| !matches!(
				stored,
				FieldStore::Write {
					migrated: false
				}
			) {
			return Ok(());
		}
		queue_verification(ctx, ns, db, table, field, previous).await.map_err(into_error)
	}
}

/// The `MIGRATE EXISTING ROWS` clause `clause` as a statement the host applies
/// commits it: its `FALLBACK` must be a constant.
pub(crate) fn request_of(
	clause: &crate::expr::statements::define::MigrateClause,
) -> anyhow::Result<MigrateRequest<'_>> {
	let fallback = match &clause.fallback {
		None => None,
		Some(expr) => Some(constant_fallback(expr).ok_or_else(|| {
			anyhow::Error::new(crate::exec::Error::Thrown(
				"A migration's FALLBACK must be a constant".to_owned(),
			))
		})?),
	};
	Ok(MigrateRequest {
		name: &clause.name,
		using: &clause.using,
		fallback,
	})
}

/// The constant a migration's `FALLBACK` expression `expr` writes, or `None`
/// for one that is no constant.
pub(crate) fn constant_fallback(expr: &crate::expr::Expr) -> Option<Value> {
	let body = crate::catalog::LogicalBody::of(expr);
	body.logical()
		.ok()
		.and_then(crate::exec::planner::static_value)
		.filter(|value| !matches!(value, Value::Closure(_)))
}

/// Refuse a change of `field`'s type, over `previous`, whose other clauses
/// no longer fit it: a constant `DEFAULT` that does not coerce to the new
/// type, and a `VALUE` or `ASSERT` that reads `$value` which an
/// `ALTER FIELD` kept rather than restating.
fn check_clauses(
	field: &FieldDefinition,
	previous: Option<&FieldDefinition>,
	kept: KeptClauses,
) -> Result<(), String> {
	if let Some(kind) = &field.field_kind {
		let default = match &field.default {
			crate::catalog::DefineDefault::None => None,
			crate::catalog::DefineDefault::Set(x) | crate::catalog::DefineDefault::Always(x) => {
				Some(x)
			}
		};
		if let Some(default) = default {
			let body = LogicalBody::of(default);
			let constant = body.logical().ok().and_then(crate::exec::planner::static_value);
			if let Some(constant) = constant
				&& !constant.can_coerce_to_kind(kind)
			{
				return Err(format!(
					"its DEFAULT does not coerce to `{}`",
					surrealdb_types::ToSql::to_sql(kind)
				));
			}
		}
	}
	if let Some(previous) = previous {
		if kept.value && previous.value.as_ref().is_some_and(reads_value) {
			return Err("its VALUE reads `$value`: restate or drop it with the new TYPE".to_owned());
		}
		if kept.assert && previous.assert.as_ref().is_some_and(reads_value) {
			return Err(
				"its ASSERT reads `$value`: restate or drop it with the new TYPE".to_owned()
			);
		}
	}
	Ok(())
}

/// The name a removal step minted by a schema statement is recorded under:
/// the path it removes and the generation it follows, so each removal of a
/// path has its own.
fn removal_name(path: &crate::expr::Idiom, layouts: &TableLayouts) -> String {
	format!("remove {} after {}", path.to_raw_string(), layouts.head().map_or(0, Generation::get))
}

/// Mint a step that removes the values at `path` from every row of `table`
/// stored before it, and queue the rewrite job that deletes them. Refused
/// while an index or a view reads the path, and while the path lies within a
/// `REFERENCE` or `INLINE` field: the index entries, the view's rows and
/// groups, and the backlinks and adjacency keys, were built from the values
/// the removal clears and are not rebuilt by it. Mints nothing for a table
/// that holds no row.
async fn mint_removal(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	path: &crate::expr::Idiom,
) -> FlowResult<()> {
	let txn = ctx.txn();
	if !txn.tb_has_records(ns, db, table).await.map_err(ControlFlow::Err)? {
		return Ok(());
	}
	let root = match path.0.first() {
		Some(Part::Field(root)) => Some(root.as_str()),
		_ => None,
	};
	let views = txn.all_tb_views(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	let reader = views.iter().find(|view| {
		view.view
			.as_ref()
			.is_some_and(|definition| root.is_none_or(|root| view_reads_field(definition, root)))
	});
	if let Some(view) = reader {
		return Err(ControlFlow::Err(anyhow::Error::new(MigrationError::Required {
			field: path.to_raw_string(),
			table: table.as_str().to_owned(),
			reason: format!("the view `{}` reads it: remove the view first", view.name),
		})));
	}
	let fields = txn.all_tb_fields(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	if let Some(reason) = derived_keys_above(&path.0, &fields) {
		return Err(ControlFlow::Err(anyhow::Error::new(MigrationError::Required {
			field: path.to_raw_string(),
			table: table.as_str().to_owned(),
			reason: format!("{reason}: remove that field first"),
		})));
	}
	let indexes = txn.all_tb_indexes(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	// A `COUNT` index's guard reads the field too: the entries it holds were judged on the values
	// the removal clears, so any read of the path's root field counts as reading the path.
	for index in indexes.iter() {
		let column = |col: &crate::expr::Idiom| paths_meet(&col.0, &path.0);
		let guard =
			|guard: &crate::expr::Cond| root.is_some_and(|root| reads_field(&guard.0, root));
		if index.cols.iter().any(column) || index.count_cond.as_ref().is_some_and(guard) {
			return Err(ControlFlow::Err(anyhow::Error::new(MigrationError::Required {
				field: path.to_raw_string(),
				table: table.as_str().to_owned(),
				reason: format!(
					"the index `{}` reads its values, which the removal clears: remove the index first",
					index.name
				),
			})));
		}
	}
	let layouts = txn.table_layouts(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	let name = removal_name(path, &layouts);
	txn.mint_tb_migration(
		ns,
		db,
		table,
		Migration {
			name,
			step: MigrationStep::Remove {
				path: IdiomText::new(path),
			},
		},
	)
	.await
	.map_err(ControlFlow::Err)?;
	txn.queue_tb_rewrite(ns, db, table).await.map_err(ControlFlow::Err)?;
	Ok(())
}

/// Judge the definition `field`, about to replace `previous` (`None` for a
/// new field) on `table`, against the rules of a table that enforces
/// migrations ([`enforces_migrations`]), decided from the definitions alone
/// except that a table holding no row needs no migration:
///
/// - a change that keeps the shape, or widens the type, is accepted;
/// - a new field, or a `COMPUTED` field made stored, is accepted when its type accepts `NONE`;
/// - a stored field made `COMPUTED` mints a step removing its values;
/// - any other change of type or `FLEXIBLE` is refused without `MIGRATE`.
///
/// A `COMPUTED` field reads nothing stored, but its path can still hold the
/// values rows were written with before it was computed: a change at level 3
/// keeps them, and raising a database to level 4 rewrites nothing. A `COMPUTED`
/// field made stored therefore mints a step removing them, once the change is
/// accepted, so every row reads the field as absent.
///
/// When the type changes, a constant `DEFAULT` must coerce to it and a kept
/// `VALUE` or `ASSERT` must not read `$value`; that is also checked on a
/// table that does not enforce migrations when the statement carries
/// `MIGRATE`, and nothing else is checked there. An import checks the same
/// clauses under the same gates, so a restored definition is held to the rules
/// an interactive `DEFINE FIELD` is, and the rest below is skipped for it.
///
/// The empty-table exemption judges the rows the statement's transaction
/// reads. Writers take no lock against it: a row a concurrent transaction
/// commits after the change was written under the definition that
/// transaction read. The exemption marks no field guaranteed, so no reader
/// trusts such a row to conform, and a verification queued later judges it
/// like any other.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn check_field_change(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	field: &FieldDefinition,
	previous: Option<&FieldDefinition>,
	migrating: bool,
	kept: KeptClauses,
	import: bool,
) -> FlowResult<()> {
	let refuse = |reason: String| {
		ControlFlow::Err(anyhow::Error::new(MigrationError::Required {
			field: field.name.to_raw_string(),
			table: table.as_str().to_owned(),
			reason,
		}))
	};
	if matches!(field.name.0.as_slice(), [Part::Field(name)] if matches!(name.as_str(), "id" | "in" | "out"))
	{
		return Ok(());
	}
	let txn = ctx.txn();
	let required = table_enforces(ctx, ns, db, table).await?;
	if !required && !migrating {
		return Ok(());
	}
	if !shape_changed(field, previous) {
		return Ok(());
	}
	check_clauses(field, previous, kept).map_err(refuse)?;
	// An import judges the clauses above as an interactive statement does, and no more: the
	// rows, the nesting and the migration steps it would mint are not its to judge.
	if import || !required || migrating {
		return Ok(());
	}
	let computed_before = previous.is_some_and(|previous| previous.computed.is_some());
	let previous = previous.filter(|previous| previous.computed.is_none());
	if field.computed.is_some() {
		// A stored field made `COMPUTED`: its stored values disappear for
		// every reader at once.
		if previous.is_some() {
			mint_removal(ctx, ns, db, table, &field.name).await?;
		}
		return Ok(());
	}
	let accepted = match previous {
		None => field.field_kind.as_ref().is_none_or(Kind::can_be_none),
		Some(previous) => widens(previous, field),
	};
	if !accepted && txn.tb_has_records(ns, db, table).await.map_err(ControlFlow::Err)? {
		return Err(refuse(match previous {
			None => "the table is SCHEMAFULL in a database at compatibility level 4 and has \
			         rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS \
			         to say what existing rows hold"
				.to_owned(),
			Some(_) => "the table is SCHEMAFULL in a database at compatibility level 4 and has \
			            rows, and the new definition does not accept every value the old one did: \
			            add MIGRATE EXISTING ROWS"
				.to_owned(),
		}));
	}
	let fields = txn.all_tb_fields(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	check_nesting(field, &fields).map_err(refuse)?;
	if computed_before {
		mint_removal(ctx, ns, db, table, &field.name).await?;
	}
	Ok(())
}

/// Mint a removal step for the field definition `field`, removed from
/// `table`, when the table enforces migrations ([`enforces_migrations`]):
/// its stored values disappear for every reader at once, and the rewrite job
/// deletes them. Any other table keeps them. A `COMPUTED` field reads nothing
/// stored, but its path can still hold the values rows were written with
/// before it was computed, which its removal clears too. The removal of a
/// definition beneath another field starts a verification pass under way on
/// the table over ([`restart_beneath`]), and one that leaves a name at the top
/// level undeclared runs the table's check for undeclared keys again
/// ([`recheck_undeclared`]).
pub(crate) async fn remove_field(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	field: &FieldDefinition,
) -> FlowResult<()> {
	restart_beneath(ctx, ns, db, table, field).await?;
	// The statement removes the definition, and every one beneath it, after this.
	recheck_undeclared(ctx, ns, db, table, |fields| {
		declared_roots(fields.iter().filter(|fd| !fd.name.0.starts_with(&field.name.0)))
	})
	.await?;
	if matches!(field.name.0.as_slice(), [Part::Field(name)] if matches!(name.as_str(), "id" | "in" | "out"))
	{
		return Ok(());
	}
	if !table_enforces(ctx, ns, db, table).await? {
		return Ok(());
	}
	mint_removal(ctx, ns, db, table, &field.name).await
}

/// Judge the table definition `updated`, about to replace `previous`
/// (`None` for a new table): while any row of a table that enforces
/// migrations ([`enforces_migrations`]) is behind a migration step, the
/// table's `TYPE` cannot change and it cannot become a view.
///
/// That refusal lifts by itself once the rewrite job has finished, so both
/// statement routes call this after every other refusal of the change: a
/// `TYPE` a record does not admit ([`validate_records_survive_type_change`]),
/// a record id that names its components ([`refuse_named_id_conversion`]), a
/// clause with an effect ([`crate::exec::clause_effects`]) and the rest are
/// refused for what they are, which waiting for the job would not change. A
/// caller is told to wait only for a change nothing else refuses, and what the
/// change does to the table's migrations, below, is done only once nothing
/// has refused it.
///
/// A change that makes the table enforce migrations (a `SCHEMALESS` table
/// made `SCHEMAFULL` in a database at level 4) queues a verification of its
/// rows ([`verify_table`]); an import, which restores definitions as they
/// were, queues none. Any other change that makes the table `SCHEMAFULL`
/// starts a verification pass under way over, since a pass judges the keys
/// beneath an object by whether the table is `SCHEMAFULL`
/// ([`verification_test`]). A change that ends enforcement is accepted as it
/// is; one that makes the table `SCHEMALESS` drops its check for undeclared
/// keys, since such a table refuses none.
///
/// [`validate_records_survive_type_change`]: crate::ddl::define::table::validate_records_survive_type_change
/// [`refuse_named_id_conversion`]: crate::ddl::define::table::refuse_named_id_conversion
pub(crate) async fn check_table_change(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	previous: Option<&TableDefinition>,
	updated: &TableDefinition,
) -> FlowResult<()> {
	let Some(previous) = previous else {
		return Ok(());
	};
	let table = &updated.name;
	let database = current_database(ctx).await?;
	let was_enforced = enforces_migrations(database.as_deref(), previous);
	let enforced = enforces_migrations(database.as_deref(), updated);
	let txn = ctx.txn();
	if was_enforced
		&& (previous.table_type != updated.table_type
			|| (updated.view.is_some() && previous.view != updated.view))
	{
		let pending = txn.get_tb_migration_job(ns, db, table).await.map_err(ControlFlow::Err)?;
		if pending.is_some_and(|job| job.rewrite) {
			return Err(ControlFlow::Err(anyhow::Error::new(MigrationError::RefusedTable {
				table: table.as_str().to_owned(),
				reason: "rows are behind a migration step: the table's TYPE cannot change, nor \
				         can it become a view, until the rewrite job has finished"
					.to_owned(),
			})));
		}
	}
	if previous.schemafull && !updated.schemafull {
		txn.drop_tb_undeclared(ns, db, table).await.map_err(ControlFlow::Err)?;
	}
	let import = ctx.options().is_some_and(|options| options.import);
	if !was_enforced && enforced && !import {
		verify_table(ctx, ns, db, table).await?;
	} else if updated.schemafull && !previous.schemafull {
		// A pass under way judged the rows it has passed with the keys beneath
		// every object left open.
		txn.restart_tb_verification(ns, db, table).await.map_err(ControlFlow::Err)?;
	}
	Ok(())
}

/// Queue a verification of every table of the database `db` of `ns` that
/// enforces migrations, after the database's compatibility level rose from
/// `previous` to `level`: rows stored under level 3 were never held to the
/// rules of level 4. A rise that does not cross level 4, and one under
/// `OPTION IMPORT`, queues nothing. A row that does not conform is reported
/// by the verification (`INFO FOR TABLE`), not refused.
pub(crate) async fn verify_database(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	previous: u8,
	level: u8,
) -> FlowResult<()> {
	let four = crate::catalog::DatabaseCompatibility::LEVEL_4.get();
	if previous >= four || level < four {
		return Ok(());
	}
	if ctx.options().is_some_and(|options| options.import) {
		return Ok(());
	}
	let tables = ctx.txn().all_tb(ns, db, None).await.map_err(ControlFlow::Err)?;
	for tb in tables.iter().filter(|tb| tb.schemafull && tb.view.is_none()) {
		verify_table(ctx, ns, db, &tb.name).await?;
	}
	Ok(())
}

/// Queue a verification pass of every field of `table` a pass can promote,
/// and the check for rows holding a top-level key no definition declares,
/// which a write of the row refuses, when the table has just come to enforce
/// migrations over rows stored without them. A table with no rows queues
/// nothing, for the reason [`queue_verification`] gives. No rewrite is
/// queued: a verification reports the rows that do not conform and changes
/// none, and keys no definition declares stay until `REMOVE UNDECLARED
/// FIELDS` clears them ([`remove_undeclared`]).
async fn verify_table(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> FlowResult<()> {
	let txn = ctx.txn();
	if !txn.tb_has_records(ns, db, table).await.map_err(ControlFlow::Err)? {
		return Ok(());
	}
	let fields = txn.all_tb_fields(ns, db, table, None).await.map_err(ControlFlow::Err)?;
	for field in fields.iter().filter(|field| verifiable(field)) {
		let digest = definition_digest(field).map_err(ControlFlow::Err)?;
		txn.queue_tb_verification(
			ns,
			db,
			table,
			crate::key::table_migration::VerifiedField {
				name: field.name.to_raw_string(),
				definition: digest,
				failures: 0,
				finished: false,
			},
		)
		.await
		.map_err(ControlFlow::Err)?;
	}
	txn.queue_tb_undeclared(ns, db, table, declared_roots(fields.iter()))
		.await
		.map_err(ControlFlow::Err)
}

/// Whether `table`, of the database `db`, enforces migrations: the database
/// is at compatibility level 4 or above, and the table is `SCHEMAFULL` and
/// not a view. A database with no definition row is at level 3.
///
/// A table that enforces migrations refuses a change to a field its stored
/// rows might not satisfy unless the change says what happens to them
/// ([`check_field_change`]), and drops the stored values of a removed field
/// ([`remove_field`]). Any other table accepts such changes and keeps the
/// values; `MIGRATE` works on either.
pub(crate) fn enforces_migrations(
	db: Option<&DatabaseDefinition>,
	table: &TableDefinition,
) -> bool {
	table.schemafull
		&& table.view.is_none()
		&& db.is_some_and(DatabaseDefinition::enforces_migrations)
}

/// Whether the table `table` of the database `db` of `ns` enforces
/// migrations ([`enforces_migrations`]), as the transaction of `ctx` reads
/// the table and its database now. A table with no definition does not.
async fn table_enforces(
	ctx: &ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> FlowResult<bool> {
	let Some(tb) = ctx.txn().get_tb(ns, db, table, None).await.map_err(ControlFlow::Err)? else {
		return Ok(false);
	};
	if !tb.schemafull || tb.view.is_some() {
		return Ok(false);
	}
	let database = current_database(ctx).await?;
	Ok(enforces_migrations(database.as_deref(), &tb))
}

/// The definition of the database `ctx` runs in, as its transaction reads
/// it now: a statement earlier in the transaction may have changed its
/// compatibility level since the context was built. `None` when the
/// database has no definition row.
async fn current_database(ctx: &ExecutionContext) -> FlowResult<Option<Arc<DatabaseDefinition>>> {
	let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
	ctx.txn()
		.get_db_by_name(db_ctx.ns_name(), db_ctx.db_name(), None)
		.await
		.map_err(ControlFlow::Err)
}

#[cfg(test)]
mod transform_tests {
	//! The definition-time check of a migration's transform
	//! ([`check_transform`]) and the kind arithmetic it rests on.
	//!
	//! A transform may read `$value` alone, and a read that steps through a
	//! record link would fetch a record the transform is not given. Each case
	//! names a transform, the kind `$value` has, and what the check answers:
	//! accepted, or refused with its exact reason.

	use super::*;
	use crate::exec::function::FunctionRegistry;
	use crate::syn;

	fn kind(text: &str) -> Kind {
		syn::kind(text).unwrap_or_else(|e| panic!("kind `{text}` does not parse: {e}"))
	}

	fn logical(text: &str) -> LogicalExpr {
		let expr = syn::expr(text).unwrap_or_else(|e| panic!("`{text}` does not parse: {e}"));
		LogicalBody::of(&expr)
			.logical()
			.unwrap_or_else(|e| panic!("`{text}` has no logical form: {e}"))
			.clone()
	}

	fn check(transform: &str, value: &str) -> Result<(), String> {
		let registry = FunctionRegistry::with_builtins();
		check_transform(&logical(transform), &kind(value), &registry)
	}

	const STATEMENT: &str = "the transform may not hold a statement";
	const GRAPH: &str = "the transform may not traverse the graph";
	const ONLY_VALUE: &str = "the transform reads only `$value`";
	const CALLABLE: &str =
		"the transform may not call a user-defined function, a model, a script or a module";
	const EFFECTS: &str = "the transform must have no effects: it may not read data, the catalog, \
	                       the session, the clock or randomness";

	fn deref(step: &str) -> String {
		format!(
			"the transform may not read a linked record, and `{step}` reads through a value \
			 that may be a record link"
		)
	}

	fn method(name: &str) -> String {
		format!(
			"the transform may not call `.{name}()` as a method, which an object answers with \
			 a closure it holds: call the builtin function instead"
		)
	}

	/// What the check answers for one transform.
	enum Verdict {
		Accepted,
		Refused(String),
	}
	use Verdict::{Accepted, Refused};

	/// Every case: the transform, the kind of `$value`, and the verdict.
	fn cases() -> Vec<(&'static str, &'static str, Verdict)> {
		vec![
			// Nothing but `$value`, builtins and operators.
			("$value", "int", Accepted),
			("<int>$value", "any", Accepted),
			("$value + 1", "any", Accepted),
			("string::uppercase($value)", "string", Accepted),
			("$value ?? 'USD'", "any", Accepted),
			("record::id($value)", "record<person>", Accepted),
			("<string>record::id($value)", "option<record<person>>", Accepted),
			// A field read through a record link fetches the record.
			("$value.name", "record<person>", Refused(deref("$value.name"))),
			("$value.name", "option<record<person>>", Refused(deref("$value.name"))),
			("$value.name", "array<record<person>>", Refused(deref("$value.name"))),
			("$value.name", "set<record<person>>", Refused(deref("$value.name"))),
			("$value.name", "record<a> | record<b>", Refused(deref("$value.name"))),
			("$value.name", "record", Refused(deref("$value.name"))),
			("$value.name", "any", Refused(deref("$value.name"))),
			("$value.*", "any", Refused(deref("$value.*"))),
			("$value.*", "record<person>", Refused(deref("$value.*"))),
			("$value.*", "array<record<person>>", Refused(deref("$value.*"))),
			("$value.{ name }", "any", Refused(deref("$value.{ name }"))),
			("$value.{ name }", "record<person>", Refused(deref("$value.{ name }"))),
			// A value whose kind admits no link steps freely.
			("$value.name", "int", Accepted),
			("$value.name", "string | int", Accepted),
			("$value.name", "option<int>", Accepted),
			("$value.name", "object", Accepted),
			("$value.*", "int", Accepted),
			("$value.name", "{ name: string }", Accepted),
			("$value.name", "{ name: string, link: record<person> }", Accepted),
			("$value.a.b", "{ a: { b: int } }", Accepted),
			// The link a literal object holds is read, not fetched, until a step
			// goes through it.
			("$value.link", "{ link: record<person> }", Accepted),
			("$value.link.name", "{ link: record<person> }", Refused(deref("$value.link.name"))),
			("$value.link.*", "{ link: record<person> }", Refused(deref("$value.link.*"))),
			("$value.a.link", "{ a: { link: record<p> } }", Accepted),
			("$value.a.link.x", "{ a: { link: record<p> } }", Refused(deref("$value.a.link.x"))),
			(
				"$value.list.name",
				"{ list: array<record<person>> }",
				Refused(deref("$value.list.name")),
			),
			("$value.list.name", "{ list: array<{ name: string }> }", Accepted),
			// A field a literal object does not declare may hold anything.
			("$value.other.name", "{ name: string }", Refused(deref("$value.other.name"))),
			// An untyped object's field may hold a link.
			("$value.next.name", "object", Refused(deref("$value.next.name"))),
			("<string>$value.name", "any", Refused(deref("$value.name"))),
			// A cast says what the value is.
			("(<object>$value).name", "any", Accepted),
			(
				"(<record<person>>$value).name",
				"any",
				Refused(deref("(<record<person>> $value).name")),
			),
			("(<{ link: record<p> }>$value).link", "any", Accepted),
			(
				"(<{ link: record<p> }>$value).link.name",
				"any",
				Refused(deref("(<{ link: record<p> }> $value).link.name")),
			),
			// The first and last element of a collection take its element kind.
			("$value[0]", "any", Accepted),
			("$value[0].name", "array<{ name: string }>", Accepted),
			("$value[0].name", "array<record<person>>", Refused(deref("$value[0].name"))),
			("$value[$].name", "array<record<person>>", Refused(deref("$value[$].name"))),
			("$value[0].name", "any", Refused(deref("$value[0].name"))),
			// A destructure judges every entry that steps through a link.
			("$value.{ a, b }", "{ a: int, b: int }", Accepted),
			("$value.{ link }", "{ link: record<p> }", Accepted),
			("$value.{ link.* }", "{ link: record<p> }", Refused(deref("$value.{ link.* }"))),
			("$value.{ n.* }", "{ n: { link: record<p> } }", Accepted),
			(
				"$value.{ n.{ link.* } }",
				"{ n: { link: record<p> } }",
				Refused(deref("$value.{ n.{ link.* } }")),
			),
			(
				"$value.{ link.* }",
				"array<{ link: record<p> }>",
				Refused(deref("$value.{ link.* }")),
			),
			("$value.{ a }", "array<{ a: int }>", Accepted),
			// A closure's argument has the kind it declares, and `any` otherwise.
			(
				"array::map($value, |$v| $v.name)",
				"array<{ name: string }>",
				Refused(deref("$v.name")),
			),
			(
				"array::map($value, |$v: { name: string }| $v.name)",
				"array<{ name: string }>",
				Accepted,
			),
			(
				"array::map($value, |$v: record<person>| $v.name)",
				"array<record<person>>",
				Refused(deref("$v.name")),
			),
			(
				"array::map($value, |$v: record<person>| <string>record::id($v))",
				"array<record<person>>",
				Accepted,
			),
			// A name two closures declare with different kinds is read as either.
			("[|$v: int| $v, |$v: record<person>| $v.name]", "any", Refused(deref("$v.name"))),
			("[|$v: { name: string }| $v.name, |$v: { name: string }| $v.name]", "any", Accepted),
			// A method an object does not dispatch is answered by a closure it
			// holds; a builtin function is not.
			("$value.map(|$v: int| $v)", "array<int>", Refused(method("map"))),
			("array::map($value, |$v: int| $v)", "array<int>", Accepted),
			("$value.unknown_method()", "any", Refused(method("unknown_method"))),
			// A call that has a body the transform does not hold.
			("fn::conv($value)", "any", Refused(CALLABLE.to_owned())),
			// Nothing reads the database, the graph or the row.
			("(SELECT * FROM person)", "any", Refused(STATEMENT.to_owned())),
			("$value->knows->person", "record<person>", Refused(GRAPH.to_owned())),
			("name", "any", Refused(ONLY_VALUE.to_owned())),
			// Parameters: `$value` and the closure's own.
			("$p", "any", Refused("the transform reads only `$value`, not `$p`".to_owned())),
			(
				"$value + $p",
				"any",
				Refused("the transform reads only `$value`, not `$p`".to_owned()),
			),
			("array::map($value, |$v| $v + 1)", "array<int>", Accepted),
			// A closure's argument is in scope in its own body alone.
			(
				"[array::map($value, |$p| $p), $p]",
				"array<int>",
				Refused("the transform reads only `$value`, not `$p`".to_owned()),
			),
			(
				"[array::map($value, |$p| $p), array::map($value, |$q| $q + $p)]",
				"array<int>",
				Refused("the transform reads only `$value`, not `$p`".to_owned()),
			),
			("array::map($value, |$a| array::map($value, |$b| $a + $b))", "array<int>", Accepted),
			// Effects.
			("time::now()", "any", Refused(EFFECTS.to_owned())),
			("rand::int()", "any", Refused(EFFECTS.to_owned())),
		]
	}

	#[test]
	fn the_check_accepts_and_refuses_each_transform_for_the_kind_of_value() {
		let mut failures = Vec::new();
		for (transform, value, verdict) in cases() {
			let answer = check(transform, value);
			let ok = match (&verdict, &answer) {
				(Accepted, Ok(())) => true,
				(Refused(expected), Err(actual)) => expected == actual,
				_ => false,
			};
			if !ok {
				failures.push(format!(
					"`{transform}` with $value: {value}\n    expected {}\n    got      {answer:?}",
					match &verdict {
						Accepted => "Ok(())".to_owned(),
						Refused(reason) => format!("Err({reason:?})"),
					}
				));
			}
		}
		assert!(failures.is_empty(), "{} cases differ:\n{}", failures.len(), failures.join("\n"));
	}

	/// An `ASSERT` the migration applies is judged by the same check, against
	/// the type of the migrated field.
	#[test]
	fn the_assert_of_a_migrated_field_is_judged_against_its_own_type() {
		assert_eq!(check("$value > 0", "int"), Ok(()));
		assert_eq!(check("string::len($value) > 0", "string"), Ok(()));
		assert_eq!(check("$value.name != ''", "record<person>"), Err(deref("$value.name")));
	}

	#[test]
	fn may_link_is_true_for_a_kind_that_may_hold_or_collect_a_record_link() {
		for yes in [
			"any",
			"record",
			"record<person>",
			"record<a | b>",
			"option<record<person>>",
			"record<person> | int",
			"array<record<person>>",
			"set<record<person>>",
			"array<array<record<person>>>",
			"array<any>",
			"[int, record<person>]",
		] {
			assert!(may_link(&kind(yes)), "{yes} may hold a link");
		}
		for no in [
			"int",
			"string",
			"option<int>",
			"int | string",
			"object",
			"array<int>",
			"set<string>",
			"{ link: record<person> }",
			"array<{ link: record<person> }>",
			"[int, string]",
			"none",
			"bool",
		] {
			assert!(!may_link(&kind(no)), "{no} admits no link at the value itself");
		}
	}

	#[test]
	fn field_kind_maps_over_collections_and_unions_and_defaults_to_any_or_none() {
		let field = |kind_text: &str, name: &str| field_kind(&kind(kind_text), name);
		assert_eq!(field("{ a: int }", "a"), kind("int"));
		assert_eq!(field("{ a: int }", "b"), Kind::Any, "a field the shape does not declare");
		assert_eq!(field("array<{ a: int }>", "a"), kind("array<int>"));
		assert_eq!(field("set<{ a: int }>", "a"), kind("set<int>"));
		assert_eq!(field("array<array<{ a: int }>>", "a"), kind("array<array<int>>"));
		assert_eq!(field("option<{ a: int }>", "a"), kind("none | int"));
		assert_eq!(field("{ a: int } | { a: string }", "a"), kind("int | string"));
		for any in ["any", "object", "record<person>", "geometry", "[int, string]"] {
			assert_eq!(field(any, "a"), Kind::Any, "{any} may hold anything at `a`");
		}
		for none in ["int", "string", "bool", "none", "float"] {
			assert_eq!(field(none, "a"), Kind::None, "{none} has no fields");
		}
	}

	#[test]
	fn element_kind_is_the_collections_element_and_any_other_kind_itself() {
		let element = |kind_text: &str| element_kind(&kind(kind_text));
		assert_eq!(element("array<int>"), kind("int"));
		assert_eq!(element("set<string>"), kind("string"));
		assert_eq!(element("array<array<int>>"), kind("array<int>"));
		assert_eq!(element("[int, string]"), kind("int | string"));
		assert_eq!(element("option<array<int>>"), kind("none | int"));
		assert_eq!(element("array<int> | set<string>"), kind("int | string"));
		for itself in ["int", "any", "object", "record<person>", "{ a: int }"] {
			assert_eq!(element(itself), kind(itself));
		}
	}

	#[test]
	fn a_value_error_depends_on_the_value_alone() {
		let value = |e: crate::expr::Error| is_value_error(&anyhow::Error::new(e));
		assert!(value(crate::expr::Error::TryAdd("1".to_owned(), "'a'".to_owned())));
		assert!(value(crate::expr::Error::InvalidRegex("(".to_owned())));
		// What the build, the node's limits or the encoding decide is the same
		// for no two nodes, and takes no fallback.
		assert!(!value(crate::expr::Error::FeatureDisabled {
			feature: "text-diff",
			what: "text patches",
		}));
		assert!(!value(crate::expr::Error::ValueTooDeep {
			max: 1,
		}));
		assert!(!value(crate::expr::Error::Serialization("x".to_owned())));
		// A cause beneath context is judged as the cause.
		assert!(!is_value_error(
			&anyhow::Error::new(crate::expr::Error::FeatureDisabled {
				feature: "text-diff",
				what: "text patches",
			})
			.context("the transform failed")
		));
	}

	#[test]
	fn kind_of_reads_the_kind_from_the_value_a_cast_a_literal_and_a_step() {
		let params = |value: &str| HashMap::from([("value".to_owned(), kind(value))]);
		let of = |text: &str, value: &str| kind_of(&logical(text), &params(value));
		assert_eq!(of("$value", "{ a: int }"), kind("{ a: int }"));
		assert_eq!(of("$other", "int"), Kind::Any, "a parameter nothing declared");
		assert_eq!(of("5", "any"), Kind::Int);
		assert_eq!(of("'x'", "any"), Kind::String);
		assert_eq!(of("<record<person>>$value", "any"), kind("record<person>"));
		assert_eq!(of("<int>$value", "string"), Kind::Int);
		assert_eq!(of("$value.a", "{ a: int }"), Kind::Int);
		assert_eq!(of("$value.a.b", "{ a: { b: string } }"), Kind::String);
		assert_eq!(of("$value.a", "array<{ a: int }>"), kind("array<int>"));
		assert_eq!(of("$value.a", "int"), Kind::None);
		assert_eq!(of("$value[0]", "array<string>"), Kind::String);
		assert_eq!(of("$value[$]", "array<string>"), Kind::String);
		assert_eq!(of("$value[0]", "int"), Kind::Any, "an index into a non-collection");
		assert_eq!(of("$value[$i]", "array<string>"), Kind::String);
		assert_eq!(of("$value[$i]", "string"), Kind::Any, "an index into a non-collection");
		// The first and last element of a collection take its element kind; any
		// other value passes through.
		let first_and_last = |value: &str| {
			let base = Box::new(logical("$value"));
			let params = params(value);
			(
				kind_of(&LogicalExpr::First(base.clone()), &params),
				kind_of(&LogicalExpr::Last(base), &params),
			)
		};
		assert_eq!(first_and_last("array<string>"), (Kind::String, Kind::String));
		assert_eq!(first_and_last("set<int>"), (Kind::Int, Kind::Int));
		assert_eq!(first_and_last("int"), (Kind::Int, Kind::Int));
		assert_eq!(first_and_last("[int, string]"), (kind("int | string"), kind("int | string")));
		assert_eq!(of("$value + 1", "int"), Kind::Any, "an operator's result is not read");
		assert_eq!(of("string::uppercase($value)", "string"), Kind::Any, "nor is a call's");
	}
}

#[cfg(test)]
mod coercion_tests {
	//! What a step's coercion of a value to the field's type may change: the
	//! representation of a number that converts back to the value it was, and
	//! nothing else ([`lossless_coerce`]); and what a value must be, as stored,
	//! to pass a test unchanged ([`identical`]).

	use surrealdb_types::ToSql;

	use super::*;
	use crate::syn;

	fn kind(text: &str) -> Kind {
		syn::kind(text).unwrap_or_else(|e| panic!("kind `{text}` does not parse: {e}"))
	}

	fn value(text: &str) -> Value {
		Value::from(syn::value(text).unwrap_or_else(|e| panic!("`{text}` does not parse: {e}")))
	}

	/// The value `text` coerces to under `kind`, rendered, or `None` where the
	/// coercion loses something.
	fn coerced(text: &str, kind_text: &str) -> Option<String> {
		lossless_coerce(&value(text), &kind(kind_text)).map(|v| v.to_sql())
	}

	#[test]
	fn a_number_keeps_its_value_or_the_coercion_is_lossy() {
		assert_eq!(coerced("3", "float").as_deref(), Some("3f"));
		assert_eq!(coerced("9007199254740993", "float"), None);
		assert_eq!(coerced("2.5f", "int"), None);
		assert_eq!(coerced("4f", "int").as_deref(), Some("4"));
		assert_eq!(coerced("0.10000000000000000555dec", "float"), None);
		assert_eq!(coerced("1.5dec", "float").as_deref(), Some("1.5f"));
	}

	#[test]
	fn an_array_keeps_each_number_in_place() {
		assert_eq!(coerced("[1, 2]", "array<float>").as_deref(), Some("[1f, 2f]"));
		assert_eq!(coerced("[9007199254740993]", "array<float>"), None);
		assert_eq!(coerced("[[9007199254740993]]", "array<array<float>>"), None);
	}

	#[test]
	fn a_set_keeps_every_element_apart_and_each_number() {
		assert_eq!(coerced("{1, 2}", "set<float>").as_deref(), Some("{1f, 2f}"));
		// Two ints that round to one float merge.
		assert_eq!(coerced("{9007199254740992, 9007199254740993}", "set<float>"), None);
		// One that rounds alone changes.
		assert_eq!(coerced("{9007199254740993,}", "set<float>"), None);
		assert_eq!(coerced("{[9007199254740993], [1]}", "set<array<float>>"), None);
		assert_eq!(coerced("{[2], [1]}", "set<array<float>>").as_deref(), Some("{[1f], [2f]}"));
		assert_eq!(coerced("[{9007199254740993,}]", "array<set<float>>"), None);
		assert_eq!(coerced("{{9007199254740993,},}", "set<set<float>>"), None);
	}

	#[test]
	fn an_array_that_becomes_a_set_merges_only_its_duplicates() {
		assert_eq!(coerced("[2, 1, 2]", "set<float>").as_deref(), Some("{1f, 2f}"));
		assert_eq!(coerced("[1, 1f]", "set<float>").as_deref(), Some("{1f,}"));
		assert_eq!(coerced("[9007199254740992, 9007199254740993]", "set<float>"), None);
	}

	#[test]
	fn a_union_is_judged_by_the_member_the_value_coerces_to() {
		assert_eq!(coerced("{9007199254740993,}", "set<float> | string"), None);
		assert_eq!(coerced("{1, 2}", "set<float> | string").as_deref(), Some("{1f, 2f}"));
		assert_eq!(coerced("{1, 2}", "set<int> | set<float>").as_deref(), Some("{1, 2}"));
		assert_eq!(coerced("[{9007199254740993,}]", "array<set<float> | int>"), None);
	}

	#[test]
	fn a_value_coercion_leaves_alone_is_kept() {
		assert_eq!(
			coerced("{ s: {9007199254740993,} }", "object").as_deref(),
			Some("{ s: {9007199254740993,} }")
		);
		assert_eq!(coerced("{9007199254740993,}", "set").as_deref(), Some("{9007199254740993,}"));
		assert_eq!(coerced("{'a', 'b'}", "set<string>").as_deref(), Some("{'a', 'b'}"));
	}

	#[test]
	fn identical_tells_the_variant_of_every_element() {
		let same = |a: &str, b: &str| identical(&value(a), &value(b));
		assert!(same("{1f, 2f}", "{1f, 2f}"));
		assert!(!same("{1, 2}", "{1f, 2f}"), "a set of ints is not the set of the same floats");
		assert!(!same("[{1,}]", "[{1f,}]"));
		assert!(!same("{[1],}", "{[1f],}"));
		assert!(!same("[1]", "[1f]"));
		assert!(!same("{ a: 1 }", "{ a: 1f }"));
		assert!(same("{'a', 'b'}", "{'a', 'b'}"));
		assert!(!same("{1,}", "{1, 2}"));
	}
}
