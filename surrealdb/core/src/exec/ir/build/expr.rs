//! Building expression ops from the planner's logical expressions.
//!
//! [`bind_expr`] turns one [`LogicalExpr`] into ops at the end of the host's current block, and
//! [`bind_body`] does the same for the body of a region. The binder decides nothing: each choice
//! it copies (how a name resolves, whether a field is `COMPUTED`, which form a per-element
//! expression takes) is an answer of the [`ExprHost`], which the logical binder implements. Every
//! op goes through [`ExprHost::create`], so the host's frame, snapshot and location apply to it.
//!
//! Operands are bound in the order [`LogicalExpr::for_each_child`] visits them. A sub-expression
//! that ends its block with a terminator makes the whole expression [`Built::Diverged`], and
//! nothing after it is bound. Every result carries the knowledge [`result_type`] states for its
//! op.
//!
//! A lazily evaluated operand, an arm of an `IF`, the arguments of an aggregate and an alias of a
//! destructure are bound inside regions the host enters. Every region entered is left on the error
//! path as well, and the value `Current` denotes is restored the same way.
//!
//! An index operand and the arguments of a method are step arguments: they are bound against the
//! document root the host answers ([`ExprHost::step_root`]), which the row and the current value
//! denote inside them, and both are restored on the error path as well.

#![deny(clippy::wildcard_enum_match_arm)]

use std::ops::Bound;
use std::sync::Arc;

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::Strand;
use surrealdb_types::ToSql;

use crate::catalog::FunctionDefinition;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{
	CallAttrs, ComputedReadAttrs, ConvertAttrs, ConvertMode, ConvertSite, DestructureEntry,
	DispatchTarget, ExprOp, KeyBound, KeyRange, KeyShape, NowWhich, OnFail, RaiseKind,
	RecordIdAttrs, ShortCircuitOp, passes_row_names,
};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ErrorScope, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::seam::invariant;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::logical::pipelines::aggregate_combine;
use crate::exec::planner::name_scope;
use crate::exec::vm::payload::Refusal;
use crate::expr::logical::{
	LogicalCall, LogicalClosure, LogicalDestructureField, LogicalExpr, LogicalLiteral,
	LogicalLookup, LogicalOp, LogicalRecordIdKey, LogicalRecordIdKeyRange, LogicalRecordIdLit,
	LogicalRecurse, coercion_is_identity,
};
use crate::expr::{BinaryOperator, Function, PrefixOperator};
use crate::val::{RecordId, RecordIdKey, Value};

/// What a bound expression produced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Built {
	/// The expression's value.
	Value(ValueId),
	/// The expression ended its block with a terminator (`THROW`, a raise, `BREAK`, `CONTINUE`),
	/// so it has no value and nothing after it in the block is bound.
	Diverged,
}

/// How a name resolves at the point it is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NameRef {
	/// A LET, FOR, argument, capture, row name, or (inside a subscriber frame) session name
	/// binds it to this value.
	Value(ValueId),
	/// No binding in scope names it.
	Free,
}

/// How a field read off the row resolves at the bound catalog version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum FieldRead {
	/// No `COMPUTED` definition covers the field on any table the row may come from.
	Plain,
	/// The field is `COMPUTED` by this definition.
	Computed(Box<ComputedReadAttrs>),
}

/// A per-element form that becomes a `logical.loop`. The binder of the loop calls back into
/// [`bind_expr`] for the sub-expressions.
#[derive(Clone, Copy, Debug)]
pub(crate) enum LoopSite<'e> {
	/// A builtin running a closure written in the call, or a closure value whose creation op is
	/// visible; `slot` is the closure's argument position.
	Closure {
		call: &'e LogicalCall,
		slot: usize,
	},
	/// The method spelling of the same form: a method some builtin of which runs a closure, called
	/// with a closure written in the call or a closure value whose creation op is visible. The
	/// receiver is `base` and `slot` indexes `args`, the receiver not counted. The receiver's type
	/// decides at run time whether the closure-running builtin is the one reached, so the host
	/// keeps the plain method call for the other receivers. `args` are step arguments: the host
	/// binds them through [`ExprHost::step_root`], as [`bind_expr`] binds the arguments of a
	/// method that is no loop site.
	MethodClosure {
		base: &'e LogicalExpr,
		name: &'e Strand,
		args: &'e [LogicalExpr],
		slot: usize,
	},
	ElementFilter {
		base: &'e LogicalExpr,
		predicate: &'e LogicalExpr,
	},
	GraphStep {
		base: &'e LogicalExpr,
		lookup: &'e LogicalLookup,
	},
	Broadcast {
		base: &'e LogicalExpr,
		body: &'e LogicalExpr,
		flatten: bool,
	},
	Recurse(&'e LogicalRecurse),
}

/// A loop-control expression in value position.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Control {
	Break,
	Continue,
}

/// What the logical binder answers while expressions are bound. Implemented by the logical
/// binder's context. Every op the expression binder creates goes through [`ExprHost::create`],
/// at the end of the current block, under the current frame, snapshot and location.
pub(crate) trait ExprHost {
	fn ir(&self) -> &Ir;
	fn registry(&self) -> &FunctionRegistry;
	/// Creates `kind` at the end of the current block, with one result per entry of `results`.
	fn create(
		&mut self,
		kind: OpKind,
		operands: SmallVec<[ValueId; 4]>,
		results: SmallVec<[TypeKnowledge; 2]>,
	) -> Result<OpId, IrError>;
	/// Creates a region of `kind` owned by `owner`, with one block whose arguments are typed by
	/// `args`, and makes it the innermost region and its block current until `leave_region`.
	fn enter_region(
		&mut self,
		owner: OpId,
		kind: RegionKind,
		args: &[TypeKnowledge],
	) -> Result<(RegionId, BlockId), IrError>;
	/// Ends the innermost region entered through `enter_region`; the owner's block is current
	/// again.
	fn leave_region(&mut self) -> Result<(), IrError>;
	fn lookup(&self, name: &str) -> NameRef;
	/// The row a field read or `@` reads at this point: the field base where one is set
	/// ([`Self::replace_field_base`]), otherwise the frame's row. Where no row frame is entered the
	/// host may answer a value it creates for the absent document.
	fn row(&mut self) -> Result<ValueId, IrError>;
	/// The value `LogicalExpr::Current` denotes at this point. Where no value is bound, the host
	/// may answer a value it creates for the absent document, as [`Self::row`] does.
	fn current(&mut self) -> Result<ValueId, IrError>;
	/// Makes `value` what `LogicalExpr::Current` denotes, and answers the previous value.
	fn replace_current(&mut self, value: Option<ValueId>) -> Option<ValueId>;
	/// Makes `value` the row a field read or `@` reads, and answers the previous one; `None` is no
	/// row, as the frame has none.
	fn replace_row(&mut self, value: Option<ValueId>) -> Option<ValueId>;
	/// Makes `value` what a field read or `@` reads in place of the row, and answers the previous
	/// one; `None` reads the row. A step's argument reads the document root, never the base
	/// ([`Self::step_root`]). A host that keeps no base answers `None` and its fields read the row.
	fn replace_field_base(&mut self, value: Option<ValueId>) -> Option<ValueId> {
		let _ = value;
		None
	}
	/// The document root an index, or an argument of a method, taken from `receiver` is evaluated
	/// against: the row where the frame has one, otherwise `receiver`. The host may create the op
	/// that selects between them where the frame cannot say which.
	fn step_root(&mut self, receiver: ValueId) -> Result<ValueId, IrError>;
	/// Binds `$parent` to `value` in the innermost region entered, until that region ends. A host
	/// that keeps no names ignores it, and `$parent` then reads as a free name.
	fn declare_parent_as(&mut self, value: ValueId) {
		let _ = value;
	}
	/// Binds `$this` and `$self` to `value` until [`Self::end_row_names`] is handed the mark this
	/// answers, and `$parent` too where the frame has no row and no name binds it. A host that
	/// keeps no names ignores it, and the names then read as free names.
	fn declare_row_names_until(&mut self, value: ValueId) -> usize {
		let _ = value;
		0
	}
	/// Ends the bindings made since `mark` ([`Self::declare_row_names_until`]).
	fn end_row_names(&mut self, mark: usize) {
		let _ = mark;
	}
	/// The statement timestamp argument visible at this point.
	fn timestamp(&self) -> Option<ValueId>;
	/// Whether the request's capabilities allow a call of the builtin `name`. The binder reads a
	/// builtin as an op that names no callee only where this holds, so the call form, which
	/// carries the run-time check, stays for a denied name.
	fn allows_function(&self, name: &str) -> bool;
	fn field(&self, name: &Strand) -> FieldRead;
	/// Whether an aggregate function called here is an aggregate of the enclosing statement.
	fn aggregating(&self) -> bool;
	/// A conversion site distinct from every other handed out for this request.
	fn next_convert_site(&mut self) -> ConvertSite;
	/// The row-name operands in scope here, in the order of `ROW_PARAMS`.
	fn row_name_operands(&self) -> SmallVec<[ValueId; 2]>;
	/// Binds a statement in value position (`LogicalExpr::Subquery`), `expr.subquery` and its
	/// region included.
	fn bind_value_statement(&mut self, op: &LogicalOp) -> Result<Built, IrError>;
	fn bind_loop_site(&mut self, site: LoopSite<'_>) -> Result<Built, IrError>;
	/// Binds a closure literal: `expr.closure_create` with its captures and `ClosureBody`.
	fn bind_closure(&mut self, closure: &LogicalClosure) -> Result<ValueId, IrError>;
	/// The definition of the user-defined function `name` when a call written here may run in the
	/// caller in place of its body: the caller may call it, no caller is refused by its
	/// `PERMISSIONS`, its `AUTH LIMIT` leaves the caller's authority as it is, and the catalog the
	/// bind read is the one the call meets when it runs, which a call in a closure body, run where
	/// the closure is invoked, does not. A host that inlines nothing answers `None`.
	fn inlinable_definition(&mut self, name: &str) -> Option<Arc<FunctionDefinition>> {
		let _ = name;
		None
	}
	/// Binds `body`, the expression of the function `definition`, in a `FunctionBody` region owned
	/// by `owner`, whose arguments after the statement timestamp are the function's parameters,
	/// typed by `types`. The body reads nothing but them.
	///
	/// `text` is the expression the call stands for, with the arguments written for the
	/// parameters, which the op that computes the body's value states as its text; `None` where
	/// the host records no text.
	fn bind_function_body(
		&mut self,
		owner: OpId,
		definition: &FunctionDefinition,
		types: &[TypeKnowledge],
		body: &LogicalExpr,
		text: Option<Arc<String>>,
	) -> Result<(), IrError> {
		let _ = (owner, definition, types, body, text);
		Err(invariant("a host that inlines no function binds no function body"))
	}
	/// Notes that a call of `function` was bound, which a later call is bound after.
	fn note_call(&mut self, function: &Function) {
		let _ = function;
	}
	fn bind_control(&mut self, control: Control) -> Result<Built, IrError>;
	/// Whether a `.@` marker bound now stands in the body of a recursion: the nearest loop around
	/// the point being bound, with no region isolated from above between them, is a `Recurse`
	/// loop.
	fn in_recursion_body(&self) -> bool;
	/// Whether the surface text of each expression bound is recorded on the op that computes it.
	/// A host that records none answers `false`, and [`ExprHost::block_len`] and
	/// [`ExprHost::write_text`] are never called.
	fn writing(&self) -> bool {
		false
	}
	/// The number of ops in the current block.
	fn block_len(&self) -> usize {
		0
	}
	/// Records `expr` as written on the op that computes `value`, when that op is one of the
	/// current block's ops from position `mark` on: a value that is read rather than computed,
	/// a name or a parameter, has no op of the expression to record it on.
	fn write_text(&mut self, mark: usize, value: ValueId, expr: &LogicalExpr) {
		let _ = (mark, value, expr);
	}
	/// Records `vector`, as written, on the op that computes the right operand of the `KNN`
	/// operator whose value is `knn`, where no text or name is held for it: the text a refusal of
	/// the query vector's value names when the statement runs.
	fn note_knn_vector(&mut self, knn: ValueId, vector: &LogicalExpr) {
		let _ = (knn, vector);
	}
	/// Records `expr`, the `MATCHES` operator whose value is `matches`, as written on the op that
	/// computes it, where no text is held for it: the expression a refusal for want of an index
	/// names when no operand of the operator has a form of its own to name.
	fn note_matches(&mut self, matches: ValueId, expr: &LogicalExpr) {
		let _ = (matches, expr);
	}
}

/// The names that denote the caller's session rather than a parameter.
const SESSION_NAMES: [&str; 4] = ["auth", "session", "token", "access"];

/// Binds `expr` at the end of the current block.
///
/// When the host records written text, the op that computes the value of `expr` carries the text
/// of `expr`, and so does the op of every sub-expression bound through this function.
pub(crate) fn bind_expr(host: &mut dyn ExprHost, expr: &LogicalExpr) -> Result<Built, IrError> {
	if !host.writing() {
		return bind_node(host, expr);
	}
	let mark = host.block_len();
	let built = bind_node(host, expr)?;
	if let Built::Value(value) = built {
		host.write_text(mark, value, expr);
	}
	Ok(built)
}

fn bind_node(host: &mut dyn ExprHost, expr: &LogicalExpr) -> Result<Built, IrError> {
	match expr {
		LogicalExpr::Literal(lit) => {
			value(emit(host, ExprOp::Literal(Box::new(literal_value(lit))), SmallVec::new()))
		}
		LogicalExpr::Array(items) => bind_collection(host, expr, items, ExprOp::Array),
		LogicalExpr::Set(items) => bind_collection(host, expr, items, ExprOp::Set),
		LogicalExpr::Object(entries) => {
			let keys: Box<[String]> = entries.iter().map(|(key, _)| key.clone()).collect();
			bind_collection(
				host,
				expr,
				entries.iter().map(|(_, item)| item),
				ExprOp::Object {
					keys,
				},
			)
		}
		LogicalExpr::RecordId(rid) => {
			let key_shape = key_shape(&rid.key)?;
			let mut keys = Vec::new();
			key_exprs(&rid.key, &mut keys);
			let Some(operands) = bind_all(host, keys)? else {
				return Ok(Built::Diverged);
			};
			let attrs = RecordIdAttrs {
				table: rid.table.clone(),
				key_shape,
			};
			value(emit(host, ExprOp::RecordId(Box::new(attrs)), operands))
		}
		LogicalExpr::Param(param) => match host.lookup(param.as_str()) {
			NameRef::Value(v) => Ok(Built::Value(v)),
			NameRef::Free => {
				let name = param.clone().into_strand();
				let op = if SESSION_NAMES.contains(&param.as_str()) {
					ExprOp::SessionParam {
						name,
					}
				} else {
					ExprOp::FreeParam {
						name,
					}
				};
				value(emit(host, op, SmallVec::new()))
			}
		},
		LogicalExpr::Table(name) => value(emit(
			host,
			ExprOp::Table {
				name: name.clone(),
			},
			SmallVec::new(),
		)),
		LogicalExpr::Mock(mock) => {
			value(emit(host, ExprOp::Mock(Box::new(mock.clone())), SmallVec::new()))
		}
		LogicalExpr::Constant(c) => value(emit(
			host,
			ExprOp::Constant {
				c: c.clone(),
			},
			SmallVec::new(),
		)),
		LogicalExpr::Field(name) => {
			let op = match host.field(name) {
				FieldRead::Plain => ExprOp::Field {
					name: name.clone(),
				},
				FieldRead::Computed(attrs) => ExprOp::ComputedRead(attrs),
			};
			let row = host.row()?;
			value(emit(host, op, smallvec![row]))
		}
		LogicalExpr::Doc => {
			let row = host.row()?;
			value(emit(host, ExprOp::Doc, smallvec![row]))
		}
		LogicalExpr::Current => Ok(Built::Value(host.current()?)),
		LogicalExpr::Access {
			base,
			field,
		} => {
			let op = ExprOp::Access {
				field: field.clone(),
			};
			bind_step(host, &[&**base], op)
		}
		LogicalExpr::Index {
			base,
			index,
		} => {
			let Built::Value(subject) = bind_expr(host, base)? else {
				return Ok(Built::Diverged);
			};
			let Built::Value(at) = bind_step_arg(host, subject, index)? else {
				return Ok(Built::Diverged);
			};
			value(emit(host, ExprOp::Index, smallvec![subject, at]))
		}
		LogicalExpr::First(base) => bind_step(host, &[&**base], ExprOp::First),
		LogicalExpr::Last(base) => bind_step(host, &[&**base], ExprOp::Last),
		LogicalExpr::Each(base) => bind_step(host, &[&**base], ExprOp::Each),
		LogicalExpr::Flatten {
			base,
			explicit,
		} => {
			let op = ExprOp::Flatten {
				explicit: *explicit,
			};
			bind_step(host, &[&**base], op)
		}
		// A marker that stands in no recursion's body raises when it is reached, after its base
		// is evaluated, so every marker the IR holds stands in the body of a `Recurse` loop.
		LogicalExpr::RepeatRecurse(base) => {
			let Built::Value(base) = bind_expr(host, base)? else {
				return Ok(Built::Diverged);
			};
			if !host.in_recursion_body() {
				let raise = RaiseKind::Refused(Box::new(Refusal::UnsupportedRepeatRecurse));
				host.create(
					OpKind::Expr(ExprOp::Raise(Box::new(raise))),
					SmallVec::new(),
					SmallVec::new(),
				)?;
				return Ok(Built::Diverged);
			}
			value(emit(host, ExprOp::RepeatRecurse, smallvec![base]))
		}
		LogicalExpr::SetField {
			base,
			field,
			value: new_value,
		} => {
			let op = ExprOp::SetField {
				field: field.clone(),
			};
			bind_step(host, &[&**base, &**new_value], op)
		}
		LogicalExpr::Filter {
			base,
			predicate,
		} => host.bind_loop_site(LoopSite::ElementFilter {
			base,
			predicate,
		}),
		LogicalExpr::Lookup {
			base,
			lookup,
		} => host.bind_loop_site(LoopSite::GraphStep {
			base,
			lookup,
		}),
		LogicalExpr::Recurse(recurse) => host.bind_loop_site(LoopSite::Recurse(recurse)),
		LogicalExpr::Broadcast {
			base,
			body,
			flatten,
		} => host.bind_loop_site(LoopSite::Broadcast {
			base,
			body,
			flatten: *flatten,
		}),
		LogicalExpr::Closure(closure) => Ok(Built::Value(host.bind_closure(closure)?)),
		LogicalExpr::Break => host.bind_control(Control::Break),
		LogicalExpr::Continue => host.bind_control(Control::Continue),
		LogicalExpr::Subquery(op) => host.bind_value_statement(op),
		LogicalExpr::Method {
			base,
			name,
			args,
		} => {
			if let Some(slot) = visible_method_closure_slot(host, name, args) {
				return host.bind_loop_site(LoopSite::MethodClosure {
					base,
					name,
					args,
					slot,
				});
			}
			let Built::Value(receiver) = bind_expr(host, base)? else {
				return Ok(Built::Diverged);
			};
			let target = DispatchTarget::Method(name.clone());
			let dispatch = emit(host, ExprOp::Dispatch(Box::new(target)), smallvec![receiver])?;
			let mut operands: SmallVec<[ValueId; 4]> = smallvec![dispatch, receiver];
			for arg in args {
				let Built::Value(bound) = bind_step_arg(host, receiver, arg)? else {
					return Ok(Built::Diverged);
				};
				operands.push(bound);
			}
			let op = ExprOp::Method {
				name: name.clone(),
			};
			value(emit(host, op, operands))
		}
		LogicalExpr::Invoke {
			target,
			args,
		} => {
			let Built::Value(callee) = bind_expr(host, target)? else {
				return Ok(Built::Diverged);
			};
			let dispatch =
				emit(host, ExprOp::Dispatch(Box::new(DispatchTarget::Invoke)), smallvec![callee])?;
			let Some(operands) = bind_with(host, smallvec![dispatch], args)? else {
				return Ok(Built::Diverged);
			};
			value(emit(host, ExprOp::Invoke, operands))
		}
		LogicalExpr::Destructure {
			base,
			fields,
		} => {
			let Built::Value(subject) = bind_expr(host, base)? else {
				return Ok(Built::Diverged);
			};
			let mut aliased = Vec::new();
			let shape = destructure_shape(fields, &mut aliased);
			let op = emit_op(
				host,
				ExprOp::Destructure {
					shape,
				},
				smallvec![subject],
			)?;
			for body in aliased {
				bind_document_region(host, op, body)?;
			}
			value(result_of(host, op))
		}
		LogicalExpr::Optional {
			base,
			then,
		} => {
			let Built::Value(subject) = bind_expr(host, base)? else {
				return Ok(Built::Diverged);
			};
			let op = emit_op(host, ExprOp::Optional, smallvec![subject])?;
			bind_current_region(host, op, then)?;
			value(result_of(host, op))
		}
		LogicalExpr::Prefix {
			op,
			expr: operand,
		} => match op {
			// A cast to a type that names a type of the catalog reads the catalog, which only a
			// function reaches: `<T> x` is `enum::cast('T', x)`.
			PrefixOperator::Cast(kind) if kind.has_named_type() => {
				let call = LogicalExpr::Call(Box::new(crate::expr::logical::LogicalCall {
					receiver: Function::Normal("enum::cast".to_owned()),
					args: vec![
						LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::String(
							Strand::new(surrealdb_types::ToSql::to_sql(kind)),
						)),
						(**operand).clone(),
					],
				}));
				bind_expr(host, &call)
			}
			PrefixOperator::Cast(kind) => {
				let Built::Value(subject) = bind_expr(host, operand)? else {
					return Ok(Built::Diverged);
				};
				let attrs = ConvertAttrs {
					mode: ConvertMode::Cast,
					kind: kind.clone(),
					site: host.next_convert_site(),
					on_fail: OnFail::Error,
				};
				value(emit(host, ExprOp::Convert(Box::new(attrs)), smallvec![subject]))
			}
			PrefixOperator::Not
			| PrefixOperator::Positive
			| PrefixOperator::Negate
			| PrefixOperator::Range
			| PrefixOperator::RangeInclusive => {
				bind_step(host, &[&**operand], ExprOp::Prefix(Box::new(op.clone())))
			}
		},
		LogicalExpr::Postfix {
			expr: operand,
			op,
		} => {
			let op = ExprOp::Postfix {
				op: op.clone(),
			};
			bind_step(host, &[&**operand], op)
		}
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => match short_circuit_op(op) {
			Some(lazy) => bind_short_circuit(host, lazy, left, right),
			None => {
				let built = bind_step(
					host,
					&[&**left, &**right],
					ExprOp::Binary {
						op: op.clone(),
					},
				)?;
				match (op, &built) {
					(BinaryOperator::NearestNeighbor(_), Built::Value(knn)) => {
						host.note_knn_vector(*knn, right);
					}
					(BinaryOperator::Matches(_), Built::Value(matches)) => {
						host.note_matches(*matches, expr);
					}
					_ => {}
				}
				Ok(built)
			}
		},
		LogicalExpr::Call(call) => bind_call(host, call),
		LogicalExpr::If(chain) => bind_if(host, &chain.branches, chain.otherwise.as_ref()),
		LogicalExpr::Throw(thrown) => {
			let Built::Value(error) = bind_expr(host, thrown)? else {
				return Ok(Built::Diverged);
			};
			host.create(OpKind::Expr(ExprOp::Throw), smallvec![error], SmallVec::new())?;
			Ok(Built::Diverged)
		}
	}
}

/// Binds `expr` as the body of the current region: its value, then `expr.yield` of it. A body
/// that diverged has its terminator already and gets no `yield`.
pub(crate) fn bind_body(host: &mut dyn ExprHost, expr: &LogicalExpr) -> Result<(), IrError> {
	match bind_expr(host, expr)? {
		Built::Value(v) => yield_values(host, smallvec![v]),
		Built::Diverged => Ok(()),
	}
}

/// Binds the `FETCH` path `path` as the body of the current region, which the fetch walks rather
/// than evaluates.
///
/// A `.@` on the steps the walk takes is no recursion: it is a step the walk does not go through,
/// and the walk ends there. Such a path is bound as the path up to the `.@` followed by a
/// recursion marker, the region's yielded value, so the walk takes the steps before the `.@` and
/// ends at the marker; the steps written after the `.@` are not bound, as the walk never reaches
/// them. Every other path is bound as [`bind_body`] binds it.
pub(crate) fn bind_fetch_path(host: &mut dyn ExprHost, path: &LogicalExpr) -> Result<(), IrError> {
	let Some(walked) = walked_before_recursion(path) else {
		return bind_body(host, path);
	};
	let Built::Value(walked) = bind_expr(host, &walked)? else {
		return Ok(());
	};
	let marker = emit(host, ExprOp::RepeatRecurse, smallvec![walked])?;
	yield_values(host, smallvec![marker])
}

/// The steps of the `FETCH` path `path` before the first `.@` its walk reaches, root first, where
/// the walk reaches one. The walk goes through the base of each step ([`walk_base`]), then through
/// the body of a broadcast, which continues the path over each element; it does not go into a
/// step's arguments or into the body of a recursion, whose `.@` binds where it is written.
///
/// Steps cut from the body of a broadcast whose root is the element continue the broadcast's base
/// directly: the walk fans out over each element where a step meets an array, so the two walk
/// alike, and a broadcast is no step the walk takes.
fn walked_before_recursion(path: &LogicalExpr) -> Option<LogicalExpr> {
	if let LogicalExpr::RepeatRecurse(base) = path {
		return walked_before_recursion(base).or_else(|| Some((**base).clone()));
	}
	if let LogicalExpr::Broadcast {
		base,
		body,
		flatten,
	} = path
	{
		return walked_before_recursion(base).or_else(|| {
			let body = walked_before_recursion(body)?;
			rebased(body.clone(), base).or_else(|| {
				Some(LogicalExpr::Broadcast {
					base: base.clone(),
					body: Box::new(body),
					flatten: *flatten,
				})
			})
		});
	}
	walked_before_recursion(walk_base(path)?)
}

/// `path` with `root` in place of the value its steps are taken from ([`walk_base`]), where that
/// value is [`LogicalExpr::Current`].
fn rebased(mut path: LogicalExpr, root: &LogicalExpr) -> Option<LogicalExpr> {
	match walk_base_mut(&mut path) {
		Some(base) => {
			let taken = std::mem::replace(base, LogicalExpr::Current);
			*base = rebased(taken, root)?;
			Some(path)
		}
		None => matches!(path, LogicalExpr::Current).then(|| root.clone()),
	}
}

/// The value the step `path` of a `FETCH` path is taken from, where `path` is a step the walk
/// can reach through: the base of a path step, of a recursion, and of a broadcast.
fn walk_base(path: &LogicalExpr) -> Option<&LogicalExpr> {
	match path {
		LogicalExpr::Access {
			base,
			..
		}
		| LogicalExpr::Index {
			base,
			..
		}
		| LogicalExpr::First(base)
		| LogicalExpr::Last(base)
		| LogicalExpr::Each(base)
		| LogicalExpr::Flatten {
			base,
			..
		}
		| LogicalExpr::Filter {
			base,
			..
		}
		| LogicalExpr::Method {
			base,
			..
		}
		| LogicalExpr::Destructure {
			base,
			..
		}
		| LogicalExpr::Optional {
			base,
			..
		}
		| LogicalExpr::Lookup {
			base,
			..
		}
		| LogicalExpr::RepeatRecurse(base)
		| LogicalExpr::Broadcast {
			base,
			..
		} => Some(&**base),
		LogicalExpr::Recurse(recurse) => Some(&recurse.base),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
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
		| LogicalExpr::Subquery(_) => None,
	}
}

/// [`walk_base`], mutable.
fn walk_base_mut(path: &mut LogicalExpr) -> Option<&mut LogicalExpr> {
	match path {
		LogicalExpr::Access {
			base,
			..
		}
		| LogicalExpr::Index {
			base,
			..
		}
		| LogicalExpr::First(base)
		| LogicalExpr::Last(base)
		| LogicalExpr::Each(base)
		| LogicalExpr::Flatten {
			base,
			..
		}
		| LogicalExpr::Filter {
			base,
			..
		}
		| LogicalExpr::Method {
			base,
			..
		}
		| LogicalExpr::Destructure {
			base,
			..
		}
		| LogicalExpr::Optional {
			base,
			..
		}
		| LogicalExpr::Lookup {
			base,
			..
		}
		| LogicalExpr::RepeatRecurse(base)
		| LogicalExpr::Broadcast {
			base,
			..
		} => Some(&mut **base),
		LogicalExpr::Recurse(recurse) => Some(&mut recurse.base),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
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
		| LogicalExpr::Subquery(_) => None,
	}
}

fn yield_values(host: &mut dyn ExprHost, values: SmallVec<[ValueId; 4]>) -> Result<(), IrError> {
	host.create(OpKind::Expr(ExprOp::Yield), values, SmallVec::new())?;
	Ok(())
}

fn value(v: Result<ValueId, IrError>) -> Result<Built, IrError> {
	v.map(Built::Value)
}

/// Creates `op` over `operands` with one result typed by [`result_type`], and answers the op.
fn emit_op(
	host: &mut dyn ExprHost,
	op: ExprOp,
	operands: SmallVec<[ValueId; 4]>,
) -> Result<OpId, IrError> {
	let ty = {
		let ir = host.ir();
		let mut known: SmallVec<[&TypeKnowledge; 4]> = SmallVec::with_capacity(operands.len());
		for operand in &operands {
			known.push(&ir.value(*operand).ok_or(IrError::Stale("operand value"))?.ty);
		}
		result_type(&op, &known)
	};
	host.create(OpKind::Expr(op), operands, smallvec![ty])
}

/// The first result of `op`.
fn result_of(host: &dyn ExprHost, op: OpId) -> Result<ValueId, IrError> {
	host.ir()
		.op(op)
		.and_then(|created| created.results.first().copied())
		.ok_or(IrError::Stale("created op"))
}

/// Creates `op` over `operands` with one result typed by [`result_type`], and answers the result.
fn emit(
	host: &mut dyn ExprHost,
	op: ExprOp,
	operands: SmallVec<[ValueId; 4]>,
) -> Result<ValueId, IrError> {
	let id = emit_op(host, op, operands)?;
	result_of(host, id)
}

/// Binds each of `exprs` in order. Answers `None` when one diverged, after which none of the
/// later ones is bound.
fn bind_all<'e>(
	host: &mut dyn ExprHost,
	exprs: impl IntoIterator<Item = &'e LogicalExpr>,
) -> Result<Option<SmallVec<[ValueId; 4]>>, IrError> {
	bind_with(host, SmallVec::new(), exprs)
}

/// [`bind_all`] with the values of `operands` already in front.
fn bind_with<'e>(
	host: &mut dyn ExprHost,
	mut operands: SmallVec<[ValueId; 4]>,
	exprs: impl IntoIterator<Item = &'e LogicalExpr>,
) -> Result<Option<SmallVec<[ValueId; 4]>>, IrError> {
	for expr in exprs {
		match bind_expr(host, expr)? {
			Built::Value(v) => operands.push(v),
			Built::Diverged => return Ok(None),
		}
	}
	Ok(Some(operands))
}

/// `arg`, an index or an argument of a method taken from `receiver`, bound against the document
/// root the host answers ([`ExprHost::step_root`]): the root is what a field read, `@` and the
/// current value denote inside it, what `$this` and `$self` name, and what `$parent` names in a
/// frame with no row where no name binds it ([`ExprHost::declare_row_names_until`]), so a
/// statement or a block the argument holds reads it through those names.
/// An argument that reads no row and holds no statement has the one value whatever the root, so
/// it is bound as it stands. The row, the current value and the names are restored whether or not
/// the bind fails.
fn bind_step_arg(
	host: &mut dyn ExprHost,
	receiver: ValueId,
	arg: &LogicalExpr,
) -> Result<Built, IrError> {
	if !name_scope::step_arg_reads_row(arg) && !holds_statement(arg) {
		return bind_expr(host, arg);
	}
	let root = host.step_root(receiver)?;
	let names = host.declare_row_names_until(root);
	let row = host.replace_row(Some(root));
	let base = host.replace_field_base(None);
	let current = host.replace_current(Some(root));
	let built = bind_expr(host, arg);
	host.replace_current(current);
	host.replace_field_base(base);
	host.end_row_names(names);
	host.replace_row(row);
	built
}

/// Whether `expr` holds a statement or a block outside any closure literal: one reads the row it
/// is entered from through `$this` and `$parent`. A closure body is a frame of its own.
fn holds_statement(expr: &LogicalExpr) -> bool {
	if matches!(expr, LogicalExpr::Subquery(_)) {
		return true;
	}
	if matches!(expr, LogicalExpr::Closure(_)) {
		return false;
	}
	let mut found = false;
	expr.for_each_child(&mut |child| found = found || holds_statement(child));
	found
}

/// A path step: `op` over the values of `operands`, bound in order.
fn bind_step(
	host: &mut dyn ExprHost,
	operands: &[&LogicalExpr],
	op: ExprOp,
) -> Result<Built, IrError> {
	match bind_all(host, operands.iter().copied())? {
		Some(values) => value(emit(host, op, values)),
		None => Ok(Built::Diverged),
	}
}

/// An array, set or object. One `literal` of the collapsed value when `whole` collapses, otherwise
/// `op` over the bound elements.
fn bind_collection<'e>(
	host: &mut dyn ExprHost,
	whole: &LogicalExpr,
	items: impl IntoIterator<Item = &'e LogicalExpr>,
	op: ExprOp,
) -> Result<Built, IrError> {
	if let Some(collapsed) = collapsed(whole) {
		return value(emit(host, ExprOp::Literal(Box::new(collapsed)), SmallVec::new()));
	}
	match bind_all(host, items)? {
		Some(values) => value(emit(host, op, values)),
		None => Ok(Built::Diverged),
	}
}

/// The value of a literal, a record id whose key is a number, a string or a uuid, an array or set
/// of collapsing elements, or an object of collapsing values; `None` for every other expression.
/// An object is built through a sorted map, as evaluation builds it, so key order and
/// duplicate-key resolution agree with it. A collection that collapses is one constant operand
/// however many elements it holds, where one built from its elements takes an operand each.
fn collapsed(expr: &LogicalExpr) -> Option<Value> {
	collapse(expr, false)
}

/// [`collapsed`], and with `ids` a record id whose key collapses likewise (an array or object key
/// included) and a table as well, each the value evaluation builds from it, at any depth.
fn collapse(expr: &LogicalExpr, ids: bool) -> Option<Value> {
	match expr {
		LogicalExpr::Literal(lit) => Some(literal_value(lit)),
		LogicalExpr::RecordId(rid) if ids => record_id_value(rid).map(Value::RecordId),
		LogicalExpr::Table(table) if ids => Some(Value::Table(table.clone())),
		LogicalExpr::RecordId(record) => {
			let key = match &record.key {
				LogicalRecordIdKey::Number(number) => RecordIdKey::Number(*number),
				LogicalRecordIdKey::String(string) => RecordIdKey::String(string.clone()),
				LogicalRecordIdKey::Uuid(uuid) => RecordIdKey::Uuid(*uuid),
				LogicalRecordIdKey::Array(_)
				| LogicalRecordIdKey::Object(_)
				| LogicalRecordIdKey::Generate(_)
				| LogicalRecordIdKey::Range(_) => return None,
			};
			Some(Value::RecordId(RecordId::new(record.table.clone(), key)))
		}
		LogicalExpr::Array(items) => {
			let values =
				items.iter().map(|item| collapse(item, ids)).collect::<Option<Vec<_>>>()?;
			Some(Value::Array(crate::val::Array(values)))
		}
		LogicalExpr::Set(items) => {
			let mut set = crate::val::Set::new();
			for item in items {
				set.insert(collapse(item, ids)?);
			}
			Some(Value::Set(set))
		}
		LogicalExpr::Object(entries) => {
			let mut map = std::collections::BTreeMap::new();
			for (key, item) in entries {
				map.insert(key.clone(), collapse(item, ids)?);
			}
			Some(Value::Object(crate::val::Object::from(map)))
		}
		LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
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
		| LogicalExpr::Subquery(_) => None,
	}
}

/// Runs `body` with a region of `kind` owned by `owner` entered, handing it the arguments of the
/// region's block. The region is left whether or not `body` fails, so the host's region stack is
/// as it was found; `body`'s error takes precedence over the leave's.
fn in_region(
	host: &mut dyn ExprHost,
	owner: OpId,
	kind: RegionKind,
	args: &[TypeKnowledge],
	body: impl FnOnce(&mut dyn ExprHost, &[ValueId]) -> Result<(), IrError>,
) -> Result<(), IrError> {
	let (_, block) = host.enter_region(owner, kind, args)?;
	let result = match host.ir().block(block).map(|data| data.args.clone()) {
		Some(block_args) => body(host, &block_args),
		None => Err(IrError::Stale("region block")),
	};
	let left = host.leave_region();
	result.and(left)
}

/// Runs `body` with `value` as what `LogicalExpr::Current` denotes, and restores the previous
/// value whether or not `body` fails.
fn with_current(
	host: &mut dyn ExprHost,
	value: ValueId,
	body: impl FnOnce(&mut dyn ExprHost) -> Result<(), IrError>,
) -> Result<(), IrError> {
	let previous = host.replace_current(Some(value));
	let result = body(host);
	host.replace_current(previous);
	result
}

/// A `LazyOperand` region of `owner` with one argument, which `body` reads as `Current` and as
/// the base of a field read or `@`; a step's argument in it reads the document root.
fn bind_current_region(
	host: &mut dyn ExprHost,
	owner: OpId,
	body: &LogicalExpr,
) -> Result<(), IrError> {
	in_region(host, owner, RegionKind::LazyOperand, &[TypeKnowledge::Unknown], |host, args| {
		let Some(&argument) = args.first() else {
			return Err(IrError::Stale("region argument"));
		};
		with_current(host, argument, |host| {
			let base = host.replace_field_base(Some(argument));
			let bound = bind_body(host, body);
			host.replace_field_base(base);
			bound
		})
	})
}

/// A `LazyOperand` region of the destructure `owner` with one argument, the value destructured,
/// which `body` reads as its document: as `Current`, as the base of a field read or `@`, as the
/// root a step's argument reads and as `$parent`, `$this` and `$self`, a statement it holds
/// included. The body reads nothing of the row of the frame around the destructure.
fn bind_document_region(
	host: &mut dyn ExprHost,
	owner: OpId,
	body: &LogicalExpr,
) -> Result<(), IrError> {
	in_region(host, owner, RegionKind::LazyOperand, &[TypeKnowledge::Unknown], |host, args| {
		let Some(&argument) = args.first() else {
			return Err(IrError::Stale("region argument"));
		};
		host.declare_parent_as(argument);
		let names = host.declare_row_names_until(argument);
		let bound = with_current(host, argument, |host| {
			let row = host.replace_row(Some(argument));
			let base = host.replace_field_base(Some(argument));
			let bound = bind_body(host, body);
			host.replace_field_base(base);
			host.replace_row(row);
			bound
		});
		host.end_row_names(names);
		bound
	})
}

/// The operator of a binary expression whose right side is evaluated only when the left does not
/// decide the result, or `None` for a strict operator.
fn short_circuit_op(op: &BinaryOperator) -> Option<ShortCircuitOp> {
	match op {
		BinaryOperator::And => Some(ShortCircuitOp::And),
		BinaryOperator::Or => Some(ShortCircuitOp::Or),
		BinaryOperator::NullCoalescing => Some(ShortCircuitOp::NullCoalescing),
		BinaryOperator::TenaryCondition => Some(ShortCircuitOp::TernaryCondition),
		BinaryOperator::Subtract
		| BinaryOperator::Add
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Equal
		| BinaryOperator::ExactEqual
		| BinaryOperator::NotEqual
		| BinaryOperator::AllEqual
		| BinaryOperator::AnyEqual
		| BinaryOperator::LessThan
		| BinaryOperator::LessThanEqual
		| BinaryOperator::MoreThan
		| BinaryOperator::MoreThanEqual
		| BinaryOperator::Contain
		| BinaryOperator::NotContain
		| BinaryOperator::ContainAll
		| BinaryOperator::ContainAny
		| BinaryOperator::ContainNone
		| BinaryOperator::Inside
		| BinaryOperator::NotInside
		| BinaryOperator::AllInside
		| BinaryOperator::AnyInside
		| BinaryOperator::NoneInside
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive
		| BinaryOperator::Matches(_)
		| BinaryOperator::NearestNeighbor(_) => None,
	}
}

/// `op` over the left value, whose right side is the body of a `LazyOperand` region.
fn bind_short_circuit(
	host: &mut dyn ExprHost,
	op: ShortCircuitOp,
	left: &LogicalExpr,
	right: &LogicalExpr,
) -> Result<Built, IrError> {
	let Built::Value(lhs) = bind_expr(host, left)? else {
		return Ok(Built::Diverged);
	};
	let owner = emit_op(
		host,
		ExprOp::ShortCircuit {
			op,
		},
		smallvec![lhs],
	)?;
	in_region(host, owner, RegionKind::LazyOperand, &[], |host, _| bind_body(host, right))?;
	value(result_of(host, owner))
}

/// The entries of a destructure pattern, with the body of each aliased entry appended to `bodies`
/// in depth-first source order: the order of the owner's `LazyOperand` regions.
fn destructure_shape<'e>(
	fields: &'e [LogicalDestructureField],
	bodies: &mut Vec<&'e LogicalExpr>,
) -> Box<[DestructureEntry]> {
	fields
		.iter()
		.map(|field| match field {
			LogicalDestructureField::All(name) => DestructureEntry::All(name.clone()),
			LogicalDestructureField::Field(name) => DestructureEntry::Field(name.clone()),
			LogicalDestructureField::Aliased(name, body) => {
				bodies.push(body);
				DestructureEntry::Aliased(name.clone())
			}
			LogicalDestructureField::Destructure(name, nested) => {
				DestructureEntry::Destructure(name.clone(), destructure_shape(nested, bodies))
			}
			LogicalDestructureField::Rest => DestructureEntry::Rest,
		})
		.collect()
}

/// An `IF` chain. The first condition is the operand of `expr.if`, its value the then region, and
/// the rest of the chain the else region: a nested `expr.if`, or the `ELSE` value, or `NONE`.
///
/// An `IF` whose conditions are all settled holds only the arm it falls through to, which is the
/// `IF true` of that arm: the arm keeps the region of a branch, whose nesting a write in it spends
/// of the computation-depth budget ([`crate::exec::vm::depth`]).
fn bind_if(
	host: &mut dyn ExprHost,
	branches: &[(LogicalExpr, LogicalExpr)],
	otherwise: Option<&LogicalExpr>,
) -> Result<Built, IrError> {
	let none = LogicalExpr::Literal(LogicalLiteral::None);
	let Some(((condition, then), rest)) = branches.split_first() else {
		let Some(arm) = otherwise else {
			return bind_expr(host, &none);
		};
		let Built::Value(test) =
			bind_expr(host, &LogicalExpr::Literal(LogicalLiteral::Bool(true)))?
		else {
			return Ok(Built::Diverged);
		};
		return if_over(host, test, arm, |host| bind_body(host, &none));
	};
	let Built::Value(test) = bind_expr(host, condition)? else {
		return Ok(Built::Diverged);
	};
	if_over(host, test, then, |host| {
		if rest.is_empty() {
			return bind_body(host, otherwise.unwrap_or(&none));
		}
		match bind_if(host, rest, otherwise)? {
			Built::Value(nested) => yield_values(host, smallvec![nested]),
			Built::Diverged => Ok(()),
		}
	})
}

/// Binds `IF test THEN then ELSE NONE` over the condition `test`, already bound, as the body of
/// the current region: the `expr.if` of a permission column, whose guard is bound under names its
/// value is not.
pub(crate) fn bind_judgement(
	host: &mut dyn ExprHost,
	test: ValueId,
	then: &LogicalExpr,
) -> Result<(), IrError> {
	let none = LogicalExpr::Literal(LogicalLiteral::None);
	match if_over(host, test, then, |host| bind_body(host, &none))? {
		Built::Value(judged) => yield_values(host, smallvec![judged]),
		Built::Diverged => Ok(()),
	}
}

/// One `expr.if` over the bound condition `test`: `then` is bound as its then region, and
/// `otherwise` fills its else region.
fn if_over(
	host: &mut dyn ExprHost,
	test: ValueId,
	then: &LogicalExpr,
	otherwise: impl FnOnce(&mut dyn ExprHost) -> Result<(), IrError>,
) -> Result<Built, IrError> {
	let owner = emit_op(host, ExprOp::If, smallvec![test])?;
	in_region(host, owner, RegionKind::ValueIf, &[], |host, _| bind_body(host, then))?;
	in_region(host, owner, RegionKind::ValueIf, &[], |host, _| otherwise(host))?;
	value(result_of(host, owner))
}

/// A call, in the first of these forms that applies: an aggregate of the enclosing statement, the
/// statement clock, a builtin that runs a visible closure, or a plain call.
///
/// `array::distinct` whose first argument is itself an aggregate call is no aggregate of the
/// statement: it is a plain call over the answer of the call inside it, as the planner reads it.
fn bind_call(host: &mut dyn ExprHost, call: &LogicalCall) -> Result<Built, IrError> {
	if let Function::Normal(name) = &call.receiver {
		if host.aggregating()
			&& !distinct_over_aggregate(host.registry(), name, &call.args)
			&& let Some(kind) = host.registry().get_aggregate(name).map(|function| function.name())
		{
			return bind_aggregate(host, name, kind, &call.args);
		}
		if let Some(now) = bind_clock_read(host, name, &call.args)? {
			return Ok(now);
		}
		if let Some(slot) = visible_closure_slot(host, name, call) {
			return host.bind_loop_site(LoopSite::Closure {
				call,
				slot,
			});
		}
	}
	let Some(mut operands) = bind_all(host, &call.args)? else {
		return Ok(Built::Diverged);
	};
	let row_names = if passes_row_names(&call.receiver, host.registry()) {
		host.row_name_operands()
	} else {
		SmallVec::new()
	};
	let attrs = CallAttrs {
		function: call.receiver.clone(),
		row_names: u8::try_from(row_names.len())
			.map_err(|_| IrError::Capacity("row names of a call"))?,
		callee: None,
	};
	let inlining = match &call.receiver {
		Function::Custom(name) => inline_site(host, name, &operands),
		Function::Normal(_)
		| Function::Script(_)
		| Function::Model(_)
		| Function::Module(..)
		| Function::Silo {
			..
		} => None,
	};
	operands.extend(row_names);
	let op = emit_op(host, ExprOp::Call(Box::new(attrs)), operands)?;
	match inlining {
		Some(site) => {
			let definition = site.definition;
			let Some(body) =
				crate::exec::ir::build::inline::inlinable_body(&definition, host.registry())
			else {
				return value(result_of(host, op));
			};
			let text = host.writing().then(|| substituted_text(&definition, body, &call.args));
			host.bind_function_body(op, &definition, &site.types, body, text)?;
		}
		None => host.note_call(&call.receiver),
	}
	value(result_of(host, op))
}

/// The text of `body`, the expression of `definition`, with the arguments `args` written for its
/// parameters, and `NONE` for a parameter the call leaves out.
fn substituted_text(
	definition: &FunctionDefinition,
	body: &LogicalExpr,
	args: &[LogicalExpr],
) -> Arc<String> {
	let mut substituted = body.clone();
	let Ok(()) = substituted.visit_mut(&mut |node| {
		if let LogicalExpr::Param(param) = node
			&& let Some(at) = definition.args.iter().position(|(name, _)| name == param.as_str())
		{
			*node = args.get(at).cloned().unwrap_or(LogicalExpr::Literal(LogicalLiteral::None));
		}
		Ok::<(), std::convert::Infallible>(())
	});
	Arc::new(substituted.to_sql())
}

/// A call of a user-defined function whose body is bound beneath it.
struct InlineSite {
	definition: Arc<FunctionDefinition>,
	/// What the body is told of each parameter: the kind of its argument when the declared kind
	/// coerces it by identity, and nothing otherwise.
	types: Vec<TypeKnowledge>,
}

/// The definition of `name` and the types of its parameters, when the call of it with `args` may
/// have its body bound beneath it: the host admits the function, its body is one that may stand in
/// the caller, and the call names no more arguments than the function declares.
fn inline_site(host: &mut dyn ExprHost, name: &str, args: &[ValueId]) -> Option<InlineSite> {
	let definition = host.inlinable_definition(name)?;
	crate::exec::ir::build::inline::inlinable_body(&definition, host.registry())?;
	if args.len() > definition.args.len() {
		return None;
	}
	let ir = host.ir();
	let types = definition
		.args
		.iter()
		.enumerate()
		.map(|(at, (_, declared))| match args.get(at).and_then(|arg| ir.value(*arg)) {
			Some(value) => match &value.ty {
				TypeKnowledge::Fact {
					kind,
					..
				} if coercion_is_identity(declared, kind) => value.ty.clone(),
				TypeKnowledge::Fact {
					..
				}
				| TypeKnowledge::Hint {
					..
				}
				| TypeKnowledge::Token(_)
				| TypeKnowledge::Unknown => TypeKnowledge::Unknown,
			},
			None => TypeKnowledge::Unknown,
		})
		.collect();
	Some(InlineSite {
		definition,
		types,
	})
}

/// Whether a call of `name` with `args` is `array::distinct` over an aggregate call written as its
/// first argument, which the planner keeps as a plain call around that aggregate.
fn distinct_over_aggregate(registry: &FunctionRegistry, name: &str, args: &[LogicalExpr]) -> bool {
	name == "array::distinct"
		&& matches!(
			args.first(),
			Some(LogicalExpr::Call(inner))
				if matches!(&inner.receiver, Function::Normal(inner) if registry.get_aggregate(inner).is_some())
		)
}

/// An aggregate call: no operands, and the arguments are the yield of an `ErrorScope` region.
fn bind_aggregate(
	host: &mut dyn ExprHost,
	name: &str,
	kind: &'static str,
	args: &[LogicalExpr],
) -> Result<Built, IrError> {
	let owner = emit_op(
		host,
		ExprOp::Agg {
			kind,
			combine: aggregate_combine(name),
		},
		SmallVec::new(),
	)?;
	let scope = RegionKind::ErrorScope(ErrorScope::ShapeToNone);
	in_region(host, owner, scope, &[], |host, _| match bind_all(host, args)? {
		Some(values) => yield_values(host, values),
		None => Ok(()),
	})?;
	value(result_of(host, owner))
}

/// `time::now()` as a read of the statement timestamp in scope; `None` for any other call, for a
/// clock call where no timestamp is in scope, and for one the capabilities deny, which stays a
/// call so that the capability check runs and refuses it.
fn bind_clock_read(
	host: &mut dyn ExprHost,
	name: &str,
	args: &[LogicalExpr],
) -> Result<Option<Built>, IrError> {
	if name != "time::now" || !args.is_empty() {
		return Ok(None);
	}
	if !host.allows_function(name) {
		return Ok(None);
	}
	let Some(timestamp) = host.timestamp() else {
		return Ok(None);
	};
	let read = ExprOp::Now {
		which: NowWhich::Datetime,
	};
	Ok(Some(value(emit(host, read, smallvec![timestamp]))?))
}

/// The position of the first closure argument of the builtin `name` that holds a visible closure.
fn visible_closure_slot(host: &dyn ExprHost, name: &str, call: &LogicalCall) -> Option<usize> {
	let function = host.registry().get(name)?;
	function
		.closure_args()
		.iter()
		.copied()
		.find(|slot| call.args.get(*slot).is_some_and(|arg| is_visible_closure(host, arg)))
}

/// The position in `args` of the first argument of the method `name` that holds a visible closure
/// where some builtin the name may resolve to runs its argument at that position.
fn visible_method_closure_slot(
	host: &dyn ExprHost,
	name: &Strand,
	args: &[LogicalExpr],
) -> Option<usize> {
	host.registry()
		.method_closure_positions(name.as_str())
		.into_iter()
		.find(|slot| args.get(*slot).is_some_and(|arg| is_visible_closure(host, arg)))
}

/// Whether `expr` is a closure literal, or a name bound to the result of a `closure_create`.
fn is_visible_closure(host: &dyn ExprHost, expr: &LogicalExpr) -> bool {
	if let LogicalExpr::Closure(_) = expr {
		return true;
	}
	let LogicalExpr::Param(param) = expr else {
		return false;
	};
	let NameRef::Value(bound) = host.lookup(param.as_str()) else {
		return false;
	};
	let ir = host.ir();
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(bound).map(|data| data.def)
	else {
		return false;
	};
	ir.op(op).is_some_and(|data| {
		let OpKind::Expr(ExprOp::ClosureCreate(_)) = &data.kind else {
			return false;
		};
		true
	})
}

/// The value a leaf literal denotes, variant for variant as the planner reads it.
fn literal_value(lit: &LogicalLiteral) -> Value {
	use crate::val::Number;
	match lit {
		LogicalLiteral::None => Value::None,
		LogicalLiteral::Null => Value::Null,
		LogicalLiteral::UnboundedRange => Value::Range(Box::new(crate::val::Range::unbounded())),
		LogicalLiteral::Bool(x) => Value::Bool(*x),
		LogicalLiteral::Float(x) => Value::Number(Number::Float(*x)),
		LogicalLiteral::Integer(x) => Value::Number(Number::Int(*x)),
		LogicalLiteral::Decimal(x) => Value::Number(Number::Decimal(*x)),
		LogicalLiteral::BigDecimal(x) => Value::Number(Number::BigDecimal(x.clone())),
		LogicalLiteral::I8(x) => Value::Number(Number::I8(*x)),
		LogicalLiteral::U8(x) => Value::Number(Number::U8(*x)),
		LogicalLiteral::I16(x) => Value::Number(Number::I16(*x)),
		LogicalLiteral::U16(x) => Value::Number(Number::U16(*x)),
		LogicalLiteral::I32(x) => Value::Number(Number::I32(*x)),
		LogicalLiteral::U32(x) => Value::Number(Number::U32(*x)),
		LogicalLiteral::U64(x) => Value::Number(Number::U64(*x)),
		LogicalLiteral::F16(x) => Value::Number(Number::F16(*x)),
		LogicalLiteral::F32(x) => Value::Number(Number::F32(*x)),
		LogicalLiteral::String(x) => Value::String(x.clone()),
		LogicalLiteral::Bytes(x) => Value::Bytes(x.clone()),
		LogicalLiteral::Regex(x) => Value::Regex(x.clone()),
		LogicalLiteral::Duration(x) => Value::Duration(*x),
		LogicalLiteral::Datetime(x) => Value::Datetime(*x),
		LogicalLiteral::Date(x) => Value::Date(*x),
		LogicalLiteral::Time(x) => Value::Time(*x),
		LogicalLiteral::Timestamp(x) => Value::Timestamp(*x),
		LogicalLiteral::Interval(x) => Value::Interval(*x),
		LogicalLiteral::Uuid(x) => Value::Uuid(*x),
		LogicalLiteral::Geometry(x) => Value::Geometry(x.clone()),
		LogicalLiteral::File(x) => Value::File(x.clone()),
	}
}

/// The shape of a record id's key. A range bound that is itself a range has no shape.
fn key_shape(key: &LogicalRecordIdKey) -> Result<KeyShape, IrError> {
	Ok(match key {
		LogicalRecordIdKey::Number(n) => KeyShape::Number(*n),
		LogicalRecordIdKey::String(s) => KeyShape::String(s.clone()),
		LogicalRecordIdKey::Uuid(u) => KeyShape::Uuid(*u),
		LogicalRecordIdKey::Generate(g) => KeyShape::Generate(g.clone()),
		LogicalRecordIdKey::Array(items) => KeyShape::Array {
			len: key_len(items.len())?,
		},
		LogicalRecordIdKey::Object(entries) => KeyShape::Object {
			keys: entries.iter().map(|(key, _)| key.clone()).collect(),
		},
		LogicalRecordIdKey::Range(range) => KeyShape::Range(Box::new(KeyRange {
			start: key_bound(&range.start)?,
			end: key_bound(&range.end)?,
		})),
	})
}

/// The value of `expr` when it is an array of record ids whose keys are numbers, strings, uuids, or
/// arrays and objects of values that collapse ([`record_id_value`]), written whole: the list a
/// value source reads as one value, which no op of the plan reads an element of. A compound key is
/// the one evaluation builds from the same elements. `None` for every other expression.
pub(super) fn record_id_list(expr: &LogicalExpr) -> Option<Value> {
	let LogicalExpr::Array(items) = expr else {
		return None;
	};
	if items.is_empty() {
		return None;
	}
	let ids = items
		.iter()
		.map(|item| {
			let LogicalExpr::RecordId(rid) = item else {
				return None;
			};
			record_id_value(rid).map(Value::RecordId)
		})
		.collect::<Option<Vec<_>>>()?;
	Some(Value::Array(crate::val::Array(ids)))
}

/// The record id `rid` writes, when its key is a number, a string or a uuid, or an array or object
/// whose elements collapse, record ids and tables among them; `None` for a generated key and a
/// range.
fn record_id_value(rid: &LogicalRecordIdLit) -> Option<RecordId> {
	let key = match &rid.key {
		LogicalRecordIdKey::Number(n) => RecordIdKey::Number(*n),
		LogicalRecordIdKey::String(s) => RecordIdKey::String(s.clone()),
		LogicalRecordIdKey::Uuid(u) => RecordIdKey::Uuid(*u),
		LogicalRecordIdKey::Array(items) => RecordIdKey::Array(crate::val::Array(
			items.iter().map(|item| collapse(item, true)).collect::<Option<Vec<_>>>()?,
		)),
		LogicalRecordIdKey::Object(entries) => {
			let mut map = std::collections::BTreeMap::new();
			for (key, item) in entries {
				map.insert(key.clone(), collapse(item, true)?);
			}
			RecordIdKey::Object(crate::val::Object::from(map))
		}
		LogicalRecordIdKey::Generate(_) | LogicalRecordIdKey::Range(_) => return None,
	};
	Some(RecordId {
		table: rid.table.clone(),
		key,
	})
}

/// The shape of a key range, whose bounds' operands [`range_key_exprs`] lists.
pub(super) fn key_range_shape(range: &LogicalRecordIdKeyRange) -> Result<KeyRange, IrError> {
	Ok(KeyRange {
		start: key_bound(&range.start)?,
		end: key_bound(&range.end)?,
	})
}

/// The expressions a key range's bounds hold, in the order [`KeyShape`] names its operands: the
/// start's before the end's.
pub(super) fn range_key_exprs(range: &LogicalRecordIdKeyRange) -> Vec<&LogicalExpr> {
	let mut out = Vec::new();
	for bound in [&range.start, &range.end] {
		if let Bound::Included(key) | Bound::Excluded(key) = bound {
			key_exprs(key, &mut out);
		}
	}
	out
}

fn key_bound(bound: &Bound<LogicalRecordIdKey>) -> Result<Bound<KeyBound>, IrError> {
	let (key, wrap): (_, fn(KeyBound) -> Bound<KeyBound>) = match bound {
		Bound::Unbounded => return Ok(Bound::Unbounded),
		Bound::Included(key) => (key, Bound::Included),
		Bound::Excluded(key) => (key, Bound::Excluded),
	};
	Ok(wrap(match key {
		LogicalRecordIdKey::Number(n) => KeyBound::Number(*n),
		LogicalRecordIdKey::String(s) => KeyBound::String(s.clone()),
		LogicalRecordIdKey::Uuid(u) => KeyBound::Uuid(*u),
		LogicalRecordIdKey::Generate(g) => KeyBound::Generate(g.clone()),
		LogicalRecordIdKey::Array(items) => KeyBound::Array {
			len: key_len(items.len())?,
		},
		LogicalRecordIdKey::Object(entries) => KeyBound::Object {
			keys: entries.iter().map(|(key, _)| key.clone()).collect(),
		},
		LogicalRecordIdKey::Range(_) => {
			return Err(invariant("a record id range holds a bound that is itself a range"));
		}
	}))
}

fn key_len(len: usize) -> Result<u32, IrError> {
	u32::try_from(len).map_err(|_| IrError::Capacity("record id key elements"))
}

/// The expressions of a record id's key, in the order [`KeyShape`] names its operands: the
/// elements or values of the key, and for a range the start's before the end's.
fn key_exprs<'e>(key: &'e LogicalRecordIdKey, out: &mut Vec<&'e LogicalExpr>) {
	match key {
		LogicalRecordIdKey::Number(_)
		| LogicalRecordIdKey::String(_)
		| LogicalRecordIdKey::Uuid(_)
		| LogicalRecordIdKey::Generate(_) => {}
		LogicalRecordIdKey::Array(items) => out.extend(items),
		LogicalRecordIdKey::Object(entries) => out.extend(entries.iter().map(|(_, item)| item)),
		LogicalRecordIdKey::Range(range) => {
			for bound in [&range.start, &range.end] {
				if let Bound::Included(key) | Bound::Excluded(key) = bound {
					key_exprs(key, out);
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ops::ControlFlow;

	use surrealdb_types::ToSql;

	use super::*;

	/// The list the `Values` leaf of `sql` collapses to, `None` where it does not collapse.
	fn list(sql: &str) -> Option<String> {
		let ast = crate::syn::parse(sql).expect("statement should parse");
		let top = ast.expressions.into_iter().next().expect("one statement");
		let surrealdb_expr::parser::TopLevelExpr::Expr(expr) = top else {
			panic!("expected a bare expression");
		};
		let plan = crate::expr::logical::lower(expr).expect("statement should lower");
		let mut found = None;
		let _ = plan.visit(&mut |op| {
			if let LogicalOp::Values(values) = op {
				found = Some(record_id_list(&values.expr));
			}
			ControlFlow::<()>::Continue(())
		});
		found.expect("a Values leaf").map(|list| list.to_sql())
	}

	/// A list of record ids with compound keys collapses to one value whatever the elements of
	/// the keys: values, record ids, or arrays and objects of either. A key holding anything
	/// evaluated does not collapse.
	#[test]
	fn a_compound_key_collapses_with_the_record_ids_it_holds() {
		for sql in [
			"SELECT * FROM [edge:[person:1, person:2]]",
			"SELECT * FROM [edge:{ from: person:1, to: person:2 }, edge:[1, 'a']]",
			"SELECT * FROM [edge:[[person:1], { k: person:2 }]]",
			"SELECT * FROM [edge:[person:[1, 2], edge:{ k: person:3 }]]",
		] {
			let collapsed = list(sql).unwrap_or_else(|| panic!("`{sql}` does not collapse"));
			let from = sql.trim_start_matches("SELECT * FROM ");
			assert_eq!(collapsed.replace(' ', ""), from.replace(' ', ""), "{sql}");
		}
		for sql in [
			"SELECT * FROM [edge:[person:1, $x]]",
			"SELECT * FROM [edge:[person:1, rand::uuid()]]",
			"SELECT * FROM [edge:[person:1, person:2..4]]",
		] {
			assert_eq!(list(sql), None, "{sql}");
		}
	}
}
