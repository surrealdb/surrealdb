//! The effects a schema clause must not have, and the check a definition
//! holding such a clause is refused by where it is defined.
//!
//! The clauses are `COMPUTED`, every `PERMISSIONS` clause (a table's, a
//! field's, a function's, a parameter's), `ASSERT`, `VALUE` and `DEFAULT`. Each
//! is code the database runs on behalf of somebody who did not write it — a
//! permission per row and per live notification as the subscriber, a computed
//! field on every read — so none may have an effect that outlives its
//! evaluation. [`REFUSED`] is that set: a write, a schema statement, a builtin
//! that advances state outside the transaction (`sequence::nextval`), and
//! `SLEEP`. A `VALUE` or `DEFAULT` clause refuses the same set but for the
//! sequence advance ([`REFUSED_FIELD_WRITE`]): it runs on the write of a record,
//! so it may draw the next number of a sequence. Randomness and the
//! clock are in neither set: a `VALUE` or `DEFAULT` clause may stamp
//! `rand::uuid()` or `time::now()`, and a permission may compare against the
//! clock. A call whose effects cannot be known where the clause is defined — a
//! closure whose body is not written in the clause, a script, module or silo
//! function, a statement-evaluating builtin — is refused with them, since
//! nothing can show it holds none. A `DEFINE EVENT` body is no such clause, and
//! may write.
//!
//! [`refuse_clause`] judges one clause where its definition is written. What
//! the clause does is read from the declared [`EffectSet`]s of the builtins
//! and methods it calls and from the statements it holds, and what a stored
//! function it calls does is read from that function's body in the catalog,
//! transitively. A function the catalog does not define is not judged: the
//! clause is accepted, and calling the function where the clause runs raises
//! the error of calling an undefined function.
//!
//! A clause stored before this rule is refused where it runs: the
//! [`NoWriteFrame`] it is evaluated in refuses each effect at the point it is
//! reached, with the same error ([`ExecError::SchemaClauseEffect`]).
//!
//! [`NoWriteFrame`]: crate::dbs::NoWriteFrame

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ops::ControlFlow;

use anyhow::Result;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{
	DatabaseId, DefineDefault, Error as CatalogError, FieldDefinition, FunctionDefinition,
	LogicalBody, NamespaceId, ParamDefinition, Permission, TableDefinition,
};
use crate::exec::Error as ExecError;
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::effects::Oracle;
use crate::expr::logical::{
	EffectOracle, EffectSet, LogicalEval, LogicalExpr, LogicalLet, LogicalOp,
	may_carry_unseen_closure,
};
use crate::expr::{Expr, Function};
use crate::kvs::Transaction;

/// The effects a schema clause must not have, by the declaration of a builtin
/// or by the statement that has them.
pub(crate) const REFUSED: EffectSet = EffectSet::WRITES_DATA
	.union(EffectSet::CHANGES_CATALOG)
	.union(EffectSet::ADVANCES_STATE)
	.union(EffectSet::BLOCKING);

/// The effects a `VALUE` or `DEFAULT` clause must not have: [`REFUSED`] but for
/// the advance of a sequence, which such a clause may make when a write
/// evaluates it.
pub(crate) const REFUSED_FIELD_WRITE: EffectSet =
	EffectSet::WRITES_DATA.union(EffectSet::CHANGES_CATALOG).union(EffectSet::BLOCKING);

/// The effects the clause named `clause` (as an error names it) must not have.
pub(crate) fn refused_by(clause: &str) -> EffectSet {
	match clause {
		"VALUE" | "DEFAULT" => REFUSED_FIELD_WRITE,
		// Every caller's invocation evaluates them with permissions off, so an
		// external write in one would be any caller's to trigger.
		"BINDINGS" => REFUSED.union(EffectSet::WRITES_EXTERNAL),
		_ => REFUSED,
	}
}

/// The effects for which the clause named `clause` refuses the call of a
/// builtin itself, where it runs. A builtin that writes data through the
/// transaction is refused where its write is made, by the statement that makes
/// it.
///
/// A `LIVE` clause also refuses a builtin that writes to an external system
/// (`http::post`, `file::put`). It is evaluated at notification time inside the
/// triggering write's transaction, so such a call would let a subscriber inject
/// an external side effect into another user's write (GHSA-pvv9-h693-m8r4). An
/// extension's `BINDINGS` refuse it too: they run with permissions off on
/// every caller's invocation, so any caller could trigger the write. The
/// other clauses leave an external write unrefused here: they are stored by a
/// definer who may author one through a `DEFINE EVENT`, and a read of an
/// external system (`http::get`) is left to every clause, so a notification may
/// still enrich itself without leaving a side effect behind.
pub(crate) fn refused_builtin_by(clause: &str) -> EffectSet {
	match clause {
		"VALUE" | "DEFAULT" => EffectSet::BLOCKING,
		"LIVE" | "BINDINGS" => {
			EffectSet::WRITES_EXTERNAL.union(EffectSet::ADVANCES_STATE).union(EffectSet::BLOCKING)
		}
		_ => EffectSet::ADVANCES_STATE.union(EffectSet::BLOCKING),
	}
}

/// What a construct that has `effects` does, in the words an error names it
/// with: the first of the refused effects it has, or that what it does cannot
/// be known.
pub(crate) fn effect_text(effects: EffectSet) -> &'static str {
	if effects.contains(EffectSet::OPAQUE_BODY) {
		"runs code whose effects cannot be known where the clause is defined"
	} else if effects.contains(EffectSet::WRITES_DATA) {
		"writes data"
	} else if effects.contains(EffectSet::WRITES_EXTERNAL) {
		"writes to an external system"
	} else if effects.contains(EffectSet::CHANGES_CATALOG) {
		"changes the schema"
	} else if effects.contains(EffectSet::ADVANCES_STATE) {
		"advances state outside the transaction"
	} else {
		"blocks"
	}
}

/// The construct a write statement of kind `kind` (`CREATE`, `UPDATE`, ...)
/// is, as a frame refusing it names it.
pub(crate) fn write_text(kind: &str) -> String {
	format!("`{kind}` writes data")
}

/// A schema statement, as a frame refusing it names it.
pub(crate) const SCHEMA_STATEMENT_TEXT: &str = "a schema statement changes the schema";

/// `SLEEP`, as a frame refusing it names it.
pub(crate) const SLEEP_TEXT: &str = "`SLEEP` blocks";

/// The builtin `name` that declares `effects`, as a frame refusing its call
/// names it.
pub(crate) fn builtin_text(name: &str, effects: EffectSet) -> String {
	format!("`{name}` {}", effect_text(effects))
}

/// The parameters a clause binds with `LET` to a closure written in it, whose
/// bodies the walk of the clause reads like any other child.
///
/// A name is held only when every `LET` of it in the clause binds a value
/// that cannot be a closure from elsewhere ([`may_carry_unseen_closure`]), so
/// calling it runs a body the clause shows. Any other closure applied in the
/// clause — a parameter it does not bind so, a field, the result of a call —
/// has a body that is not in the clause.
#[derive(Default)]
struct WrittenClosures(HashMap<String, bool>);

impl WrittenClosures {
	/// The bindings of the clause `expr`.
	fn of_expr(expr: &LogicalExpr) -> Self {
		let mut written = Self::default();
		let _ = expr.visit_deep(&mut |inner| {
			if let LogicalExpr::Subquery(plan) = inner {
				written.add(plan);
			}
			ControlFlow::<()>::Continue(())
		});
		written
	}

	/// The bindings of the body `op`.
	fn of_op(op: &LogicalOp) -> Self {
		let mut written = Self::default();
		written.add(op);
		let _: Result<(), ()> = op.visit_scalars_deep(&mut |scalar| {
			let _ = scalar.visit_deep(&mut |inner| {
				if let LogicalExpr::Subquery(plan) = inner {
					written.add(plan);
				}
				ControlFlow::<()>::Continue(())
			});
			Ok(())
		});
		written
	}

	fn add(&mut self, op: &LogicalOp) {
		let _ = op.visit(&mut |node| {
			if let LogicalOp::Let(LogicalLet {
				name,
				value,
				..
			}) = node
			{
				let written = matches!(
					&**value,
					LogicalOp::Eval(LogicalEval { expr }) if !may_carry_unseen_closure(expr)
				);
				*self.0.entry(name.clone()).or_insert(true) &= written;
			}
			ControlFlow::<()>::Continue(())
		});
	}

	/// Whether `expr` may evaluate to a closure whose body is not in the
	/// clause.
	fn unseen(&self, expr: &LogicalExpr) -> bool {
		if let LogicalExpr::Param(param) = expr {
			self.0.get(param.as_str()) != Some(&true)
		} else {
			may_carry_unseen_closure(expr)
		}
	}
}

/// The first construct of `expr`, or of a statement it holds, that has an
/// effect a schema clause must not have, in the words an error names it with;
/// the stored functions `expr` calls are added to `calls` and not judged.
fn refused_in_expr(
	expr: &LogicalExpr,
	registry: &FunctionRegistry,
	refused: EffectSet,
	written: &WrittenClosures,
	calls: &mut BTreeSet<String>,
) -> Option<String> {
	let oracle = Oracle::new(registry);
	let found = expr.visit_deep(&mut |inner| match refused_at(
		inner, registry, refused, &oracle, written, calls,
	) {
		Some(construct) => ControlFlow::Break(construct),
		None => ControlFlow::Continue(()),
	});
	match found {
		ControlFlow::Break(construct) => Some(construct),
		ControlFlow::Continue(()) => None,
	}
}

/// The first construct of the statement `op`, or of one nested in it, that
/// has an effect a schema clause must not have; the stored functions it calls
/// are added to `calls`.
fn refused_in_op(
	op: &LogicalOp,
	registry: &FunctionRegistry,
	refused: EffectSet,
	calls: &mut BTreeSet<String>,
) -> Option<String> {
	if let Some(construct) = refused_statement(op) {
		return Some(construct);
	}
	let written = WrittenClosures::of_op(op);
	let mut found = None;
	let walked: Result<(), String> = op.visit_scalars_deep(&mut |scalar| match refused_in_expr(
		scalar, registry, refused, &written, calls,
	) {
		Some(construct) => Err(construct),
		None => Ok(()),
	});
	if let Err(construct) = walked {
		found = Some(construct);
	}
	found
}

/// The first operator of the operator tree of `op` that has an effect a schema
/// clause must not have. The plans nested in its scalars are judged where the
/// scalar walk reaches them.
fn refused_statement(op: &LogicalOp) -> Option<String> {
	let found = op.visit(&mut |node| match node {
		LogicalOp::Write(write) => ControlFlow::Break(write_text(write.statement.kind())),
		LogicalOp::Ddl(ddl) => {
			ControlFlow::Break(format!("`{}` changes the schema", ddl.statement.kind()))
		}
		LogicalOp::Sleep(_) => ControlFlow::Break(SLEEP_TEXT.to_owned()),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Window(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_) => ControlFlow::Continue(()),
	});
	match found {
		ControlFlow::Break(construct) => Some(construct),
		ControlFlow::Continue(()) => None,
	}
}

/// What the node `expr` does itself that a schema clause must not, its
/// children not included.
fn refused_at(
	expr: &LogicalExpr,
	registry: &FunctionRegistry,
	refused: EffectSet,
	oracle: &Oracle<'_>,
	written: &WrittenClosures,
	calls: &mut BTreeSet<String>,
) -> Option<String> {
	match expr {
		LogicalExpr::Subquery(plan) => refused_statement(plan),
		LogicalExpr::Call(call) => match &call.receiver {
			Function::Normal(name) => {
				let effects = oracle.builtin(name).unwrap_or(EffectSet::PURE);
				let unseen = oracle.closure_args(name).is_some_and(|slots| {
					slots
						.iter()
						.filter_map(|&slot| call.args.get(slot))
						.any(|arg| written.unseen(arg))
				});
				if effects.intersects(refused.union(EffectSet::OPAQUE_BODY)) {
					Some(builtin_text(name, effects))
				} else if unseen {
					Some(format!(
						"`{name}` runs a closure whose effects cannot be known where the clause \
						 is defined"
					))
				} else {
					None
				}
			}
			Function::Custom(name) => {
				calls.insert(name.clone());
				None
			}
			Function::Script(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => Some(format!(
				"a script, module or silo function {}",
				effect_text(EffectSet::OPAQUE_BODY)
			)),
			Function::Model(_) => None,
		},
		LogicalExpr::Method {
			base,
			name,
			args,
		} => refused_method(base, name.as_str(), args, registry, refused, written),
		LogicalExpr::Invoke {
			target,
			..
		} => written.unseen(target).then(|| {
			"calling a closure value runs a body whose effects cannot be known where the \
			 clause is defined"
				.to_owned()
		}),
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
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => None,
	}
}

/// What the method call `base.name(args)` does itself that a schema clause
/// must not.
///
/// A name the registry defines runs that builtin, judged by its declared
/// effects and, in each position it runs a closure from, by whether the
/// argument may be a closure not written in the call. A name it does not
/// define calls the closure the receiver holds under that name, which is
/// unseen unless the receiver is written in the clause. A registered name
/// whose builtin has no implementation for an object also degrades to that
/// call on an object receiver, such as a parameter holding an object with a
/// closure field. That call is admitted here: its body is not judged where the
/// clause is defined, and the frame the clause runs in refuses any effect the
/// body reaches.
fn refused_method(
	base: &LogicalExpr,
	name: &str,
	args: &[LogicalExpr],
	registry: &FunctionRegistry,
	refused: EffectSet,
	written: &WrittenClosures,
) -> Option<String> {
	let Some(descriptor) = registry.get_method(name) else {
		return written.unseen(base).then(|| {
			format!(
				"the method `{name}` calls a closure its receiver holds, whose effects cannot be \
				 known where the clause is defined"
			)
		});
	};
	let effects = registry.method_effects(name);
	if effects.intersects(refused) {
		return Some(format!("the method `{name}` {}", effect_text(effects)));
	}
	// The receiver is the builtin's position 0; the method's arguments follow.
	let unseen = descriptor
		.type_impls
		.iter()
		.map(|(_, function)| function)
		.chain(descriptor.fallback.iter())
		.flat_map(|function| function.closure_args().iter())
		.filter_map(|&slot| slot.checked_sub(1).and_then(|slot| args.get(slot)))
		.any(|arg| written.unseen(arg));
	unseen.then(|| {
		format!(
			"the method `{name}` runs a closure whose effects cannot be known where the clause \
			 is defined"
		)
	})
}

/// The first construct of `expr`, a clause with no logical form, that has an
/// effect a schema clause must not have, read from the facts of its syntax.
fn refused_in_syntax(
	expr: &Expr,
	registry: &FunctionRegistry,
	refused: EffectSet,
	calls: &mut BTreeSet<String>,
) -> Option<String> {
	let facts = expr.function_facts_with(Some(registry));
	refused_in_facts(&facts, registry, refused, calls)
}

/// [`refused_in_syntax`] over the facts of a body already scanned.
fn refused_in_facts(
	facts: &crate::expr::function_facts::FunctionFacts,
	registry: &FunctionRegistry,
	refused: EffectSet,
	calls: &mut BTreeSet<String>,
) -> Option<String> {
	calls.extend(facts.calls.iter().cloned());
	if facts.direct_writes {
		return Some("a statement writes data or changes the schema".to_owned());
	}
	if facts.opaque_effects || facts.unseen_closures {
		return Some(format!("a call {}", effect_text(EffectSet::OPAQUE_BODY)));
	}
	for name in &facts.builtins {
		let effects = registry.get(name).map_or(EffectSet::PURE, |function| function.effects());
		if effects.intersects(refused) {
			return Some(builtin_text(name, effects));
		}
	}
	for name in &facts.methods {
		let effects = registry.method_effects(name);
		if effects.intersects(refused.union(EffectSet::OPAQUE_BODY)) {
			return Some(format!("the method `{name}` {}", effect_text(effects)));
		}
	}
	None
}

/// What the stored function `definition`'s body does that a schema clause must
/// not, the functions it calls added to `calls` and not judged.
fn refused_in_function(
	definition: &FunctionDefinition,
	registry: &FunctionRegistry,
	refused: EffectSet,
	calls: &mut BTreeSet<String>,
) -> Option<String> {
	match definition.logical_body() {
		Ok(body) => refused_in_op(body, registry, refused, calls),
		Err(_) => refused_in_facts(
			&definition.block.function_facts_with(Some(registry)),
			registry,
			refused,
			calls,
		),
	}
}

/// Where a schema clause is judged: the catalog its calls resolve against,
/// and the registry its builtins are declared in.
pub(crate) struct ClauseCheck<'a> {
	pub(crate) txn: &'a Transaction,
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	pub(crate) registry: &'a FunctionRegistry,
}

impl ClauseCheck<'_> {
	/// Refuse the clause `clause` (`COMPUTED`, `PERMISSIONS`, ...) holding
	/// `expr` when it, or a stored function it reaches, has an effect a schema
	/// clause must not have, with [`ExecError::SchemaClauseEffect`] naming the
	/// clause and the first such construct found.
	///
	/// The stored functions are read from the catalog as `txn` holds it, so a
	/// function defined earlier in the same request is judged; one it does not
	/// define is not, and calling it where the clause runs fails as calling any
	/// undefined function does.
	pub(crate) async fn refuse_clause(&self, clause: &str, expr: &Expr) -> Result<()> {
		let refused = refused_by(clause);
		let mut calls = BTreeSet::new();
		let body = LogicalBody::of(expr);
		let own = match body.logical() {
			Ok(logical) => refused_in_expr(
				logical,
				self.registry,
				refused,
				&WrittenClosures::of_expr(logical),
				&mut calls,
			),
			Err(_) => refused_in_syntax(expr, self.registry, refused, &mut calls),
		};
		let construct = match own {
			Some(construct) => Some(construct),
			None => self.refused_in_callees(refused, calls).await?,
		};
		match construct {
			Some(construct) => Err(ExecError::SchemaClauseEffect {
				clause: clause.to_owned(),
				construct,
			}
			.into()),
			None => Ok(()),
		}
	}

	/// [`Self::refuse_clause`] for a permission: only a `WHERE` predicate holds
	/// code.
	pub(crate) async fn refuse_permission(&self, permission: &Permission) -> Result<()> {
		match permission {
			Permission::Specific(expr) => self.refuse_clause("PERMISSIONS", expr).await,
			Permission::None | Permission::Full => Ok(()),
		}
	}

	/// What the first of the stored functions `roots`, or a function one of
	/// them reaches, does that a clause refusing `refused` must not, naming the
	/// root the clause calls and, where it differs, the function whose body has
	/// the effect. A function the catalog does not define holds no effect to
	/// judge and is passed over.
	async fn refused_in_callees(
		&self,
		refused: EffectSet,
		roots: BTreeSet<String>,
	) -> Result<Option<String>> {
		// Each function reached, by the root it was first reached from. A
		// function reached twice is judged once: its answer is the same.
		let mut reached: HashMap<String, String> =
			roots.iter().map(|name| (name.clone(), name.clone())).collect();
		let mut pending: VecDeque<String> = roots.into_iter().collect();
		while let Some(name) = pending.pop_front() {
			let definition = match self.txn.get_db_function(self.ns, self.db, &name, None).await {
				Ok(definition) => definition,
				Err(e)
					if matches!(
						e.downcast_ref::<CatalogError>(),
						Some(CatalogError::FcNotFound { .. })
					) =>
				{
					continue;
				}
				Err(e) => return Err(e),
			};
			let mut calls = BTreeSet::new();
			let root = reached.get(&name).cloned().unwrap_or_else(|| name.clone());
			if let Some(construct) =
				refused_in_function(&definition, self.registry, refused, &mut calls)
			{
				return Ok(Some(match root == name {
					true => format!("it calls `fn::{name}`, in whose body {construct}"),
					false => format!(
						"it calls `fn::{root}`, which reaches `fn::{name}`, in whose body {construct}"
					),
				}));
			}
			for call in calls {
				if !reached.contains_key(&call) {
					reached.insert(call.clone(), root.clone());
					pending.push_back(call);
				}
			}
		}
		Ok(None)
	}

	/// Refuse a field definition whose `COMPUTED`, `VALUE`, `DEFAULT`, `ASSERT`
	/// or `PERMISSIONS` clauses break the rule.
	///
	/// With `previous`, the definition the field had before an `ALTER`, only a
	/// clause the statement changed is judged: a clause stored before the rule
	/// is refused where it runs, and altering another clause of its field
	/// leaves it as it was.
	pub(crate) async fn refuse_field(
		&self,
		previous: Option<&FieldDefinition>,
		field: &FieldDefinition,
	) -> Result<()> {
		let default = |field: &FieldDefinition| match &field.default {
			DefineDefault::Set(expr) | DefineDefault::Always(expr) => Some(expr.clone()),
			DefineDefault::None => None,
		};
		let materialized = |field: &FieldDefinition| {
			field
				.reference
				.as_ref()
				.and_then(|reference| reference.materialize.as_ref())
				.and_then(|materialize| materialize.filter.clone())
		};
		let clauses = [
			("MATERIALIZE INTO", materialized(field), previous.map(materialized)),
			("COMPUTED", field.computed.clone(), previous.map(|p| p.computed.clone())),
			("VALUE", field.value.clone(), previous.map(|p| p.value.clone())),
			("DEFAULT", default(field), previous.map(default)),
			("ASSERT", field.assert.clone(), previous.map(|p| p.assert.clone())),
		];
		for (clause, expr, held) in clauses {
			if let Some(expr) = &expr
				&& held.as_ref().is_none_or(|held| held.as_ref() != Some(expr))
			{
				self.refuse_clause(clause, expr).await?;
			}
		}
		let permissions = [
			(&field.select_permission, previous.map(|p| &p.select_permission)),
			(&field.create_permission, previous.map(|p| &p.create_permission)),
			(&field.update_permission, previous.map(|p| &p.update_permission)),
		];
		for (permission, held) in permissions {
			if held != Some(permission) {
				self.refuse_permission(permission).await?;
			}
		}
		Ok(())
	}

	/// Refuse a table definition whose `PERMISSIONS` clauses break the rule;
	/// with `previous`, only a clause changed from it, as for
	/// [`Self::refuse_field`].
	pub(crate) async fn refuse_table(
		&self,
		previous: Option<&TableDefinition>,
		table: &TableDefinition,
	) -> Result<()> {
		let (now, held) = (&table.permissions, previous.map(|p| &p.permissions));
		let permissions = [
			(&now.select, held.map(|p| &p.select)),
			(&now.create, held.map(|p| &p.create)),
			(&now.update, held.map(|p| &p.update)),
			(&now.delete, held.map(|p| &p.delete)),
		];
		for (permission, held) in permissions {
			if held != Some(permission) {
				self.refuse_permission(permission).await?;
			}
		}
		Ok(())
	}

	/// Refuse a function definition whose `PERMISSIONS` clause breaks the rule.
	pub(crate) async fn refuse_function(&self, function: &FunctionDefinition) -> Result<()> {
		self.refuse_permission(&function.permissions).await
	}

	/// Refuse a parameter definition whose `PERMISSIONS` clause breaks the
	/// rule.
	pub(crate) async fn refuse_param(&self, param: &ParamDefinition) -> Result<()> {
		self.refuse_permission(&param.permissions).await
	}
}
