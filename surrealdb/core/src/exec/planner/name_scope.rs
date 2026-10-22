//! Where the names and row reads of a closure body take their values from.
//!
//! A closure captures lexically: every free name its body reads and the row it
//! reads at the place the literal is written are valued where the literal is
//! evaluated, and the value carries them. This module is the one analysis of
//! what a literal reads from that place ([`captures_of`]) and of how a name is
//! scoped ([`NameScope::of`]).
//!
//! It is a logical-layer analysis: a pure function of the bound logical
//! expression that reads no catalog and no data. Lowering consumes its
//! verdicts to decide what a closure value captures and which row captures its
//! body unit expects; it derives none of them again.
//!
//! The current value and the document root are two reads of one row:
//!
//! - A bare field, `$this`, `$self`, `@` and a lookup from the current value read the **current
//!   value** at the literal ([`Captures::reads_current`]): the element inside an element body, the
//!   row otherwise.
//! - An index or a method argument that reads the row is evaluated against the **document root**
//!   ([`Captures::root_by_step`]), as is a statement the body holds that reads the row
//!   ([`Captures::root_by_statement`]). A block the body holds reads the current value at the
//!   literal for its own bare fields and `$this`, and is taken over the document root.

use crate::exec::planner::row_scope::{
	RowScopeKind, names_parent, reads_enclosing_row, rescopes_row_params,
};
use crate::expr::Function;
use crate::expr::logical::{
	LogicalClosure, LogicalDestructureField, LogicalExpr, LogicalOp, LogicalRecursePath,
	LogicalSource, SourceRef,
};

/// The projection functions, which read the current value of the row they are
/// called on.
const PROJECTION_FUNCTIONS: &[&str] = &["type::field", "type::fields"];

/// Where a `$name` read inside a closure body takes its value from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NameScope {
	/// An argument of the closure under analysis or of a closure enclosing the read inside it.
	ClosureArgument,
	/// `$this`/`$self` (the current value) or `$parent` (the enclosing row).
	RowVarying(RowScopeKind),
	/// Bound once for everything evaluated in its frame: statement and block `LET`, `DEFINE
	/// PARAM`, bind params, function arguments, `FOR` variables, an outer closure's arguments, and
	/// the write-frame names `$value`, `$before`, `$after`, `$event` and `$input`. Constant across
	/// the elements and rows a body iterates within that frame, except an unshadowed `DEFINE
	/// PARAM`, which is read from the catalog when the body runs and may change in between.
	StatementInvariant,
	/// `$auth`, `$session`, `$token` and `$access`
	/// ([`surrealdb_cnf::PROTECTED_PARAM_NAMES`]): read from the session at invocation.
	Session,
}

impl NameScope {
	/// The scope of the name `name` read inside a closure body whose enclosing closures
	/// declare `enclosing_args`. An argument shadows every other scope of its name.
	pub(crate) fn of(name: &str, enclosing_args: &[&str]) -> Self {
		if enclosing_args.contains(&name) {
			return Self::ClosureArgument;
		}
		if surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name) {
			return Self::Session;
		}
		match RowScopeKind::from_param_name(name) {
			Some(kind) => Self::RowVarying(kind),
			None => Self::StatementInvariant,
		}
	}
}

/// What a closure literal reads from the frame where it is written.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Captures {
	/// The names captured by value: every [`NameScope::StatementInvariant`] name the body reads,
	/// including a name the body also binds itself (a read before the binding, or outside the
	/// block that binds it, reads the captured outer value), and `parent` when it is read outside
	/// a rescoping subquery. Never `this`/`self` (those are [`Self::reads_current`]) and never a
	/// [`NameScope::ClosureArgument`]; a binding of those two is read through [`Self::row_names`].
	/// In the order of the first read, without repeats.
	pub names: Vec<String>,
	/// The `this` and `self` names the body reads at its own level or in a statement it holds, in
	/// the order of the first read, without repeats: the reads that a binding of the name where
	/// the literal is written answers, where one holds it (a block `LET`, a `FOR` variable, a
	/// function parameter, a statement `LET` of a frame with no row). Reads in an element body,
	/// in an index or method argument and in a rescoping subquery are the element's, the step
	/// root's and the subquery row's, which no such binding answers, so they are not listed.
	/// Each is also counted by [`Self::reads_current`], which is what the read is where no
	/// binding holds the name.
	pub row_names: Vec<&'static str>,
	/// The `this` and `self` names that a closure literal written in a rescoping subquery of the
	/// body, or as an index or a method argument outside an element body, lists in its own
	/// [`Self::row_names`], in the order of the first read, without repeats. The literal reads
	/// them where a binding holds the name where it is written, which is the frame this
	/// literal's body runs in: the body must hold the binding for it, whether or not it reads
	/// the name anywhere else. Not counted by [`Self::reads_current`]: the subquery's own
	/// `$this` is its row, and the argument's is the root its step reads.
	pub nested_row_names: Vec<&'static str>,
	/// The body reads the current value at the literal: a bare field, `$this`/`$self`, `@`,
	/// the current value itself, a lookup from it, a projection-function call or a call of
	/// anything but a builtin, whether directly or inside a block it holds.
	pub reads_current: bool,
	/// The body calls a projection function, outside a rescoping subquery, which reads the
	/// current value at the literal.
	pub calls_projection: bool,
	/// The body reads the document root at the literal through an index or a method argument
	/// that reads the row, or through a `$parent` read inside an element filter of the body, which
	/// that filter binds to the document root, or through a call of a function that is not a
	/// builtin with an argument that reads the row.
	pub root_by_step: bool,
	/// The body reads the document root at the literal through a statement: a rescoping subquery
	/// for which [`reads_enclosing_row`] holds, a statement that reads anything it is not handed
	/// (`INFO`, a schema statement, `SLEEP`), or a block, `LET`, `RETURN`, `FOR` or `IF` whose
	/// contents read the row. A block that reads only its own bindings, the closure's arguments
	/// and captured names sets nothing.
	pub root_by_statement: bool,
	/// The body calls a function that is not a builtin, outside a rescoping subquery and outside
	/// an index or a method argument (a root read the document places): a custom function, a
	/// script or a model, at the body's own level or inside an element filter, a broadcast, a
	/// destructure or a recursion it holds. The callee reads the current value of its caller,
	/// which for a literal in an element body is the element where the literal stands, and for the
	/// same call written without a closure in that body is the document root.
	pub calls_custom: bool,
	/// A statement the body holds, not itself inside another, names `$parent`, as a scalar or as
	/// its `FROM` source. The statement reads the document root through it, and what it places
	/// that root at differs between the closure-free statement and the one in a closure body.
	pub parent_in_statement: bool,
	/// The body reads `$parent` inside an element filter it holds, which that filter binds to
	/// the document root. A subset of the reads [`Self::root_by_step`] counts.
	pub parent_in_filter: bool,
	/// The body reads `$parent` inside a block, `LET`, `IF` or `FOR` it holds, outside a
	/// rescoping subquery and outside an element filter. The block reads the row the literal is
	/// written over through it, as it does through `$this`, but it is not a read of the current
	/// value ([`Self::reads_current`]) or a step argument.
	pub parent_in_block: bool,
	/// The body holds, in an element body of its own and outside a rescoping subquery, a closure
	/// literal whose body reads the document root through a statement or reads `$parent` inside
	/// an element filter. The literal may stand anywhere under the element body: at its level,
	/// in an index or a method argument, in a block, or in the body of another closure.
	pub nested_reads_root: bool,
	/// The body holds, in an element body of its own and outside a rescoping subquery, an index
	/// or a method argument that is or holds a closure literal that reads the row
	/// ([`holds_row_reading_closure`]), at any depth under the element body.
	pub nested_step_arg_reads_row: bool,
}

impl Captures {
	/// Whether the body reads the document root at the literal, through a step argument or a
	/// statement. The two are kept apart because a record row that is its own document root places
	/// the root for step arguments only.
	pub(crate) fn reads_root(&self) -> bool {
		self.root_by_step || self.root_by_statement
	}
}

/// What the literal `closure` reads from the frame where it is written.
pub(crate) fn captures_of(closure: &LogicalClosure) -> Captures {
	let mut walker = Walker {
		args: closure.args.iter().map(|(arg, _)| arg.as_str()).collect(),
		captures: Captures::default(),
	};
	walker.expr(
		&closure.body,
		Scope {
			frame: Frame::Body,
			rescoped: false,
			statement: false,
			filter: None,
			filter_args: 0,
			statement_args: 0,
			under_element: false,
		},
	);
	walker.captures
}

/// Whether `expr` reads the value it is evaluated against: a field of it, it itself, or a
/// parameter that names a row. A closure literal is judged by what it captures
/// ([`captures_of`]), never by its body: it reads the row where it reads the current value or
/// the document root, or names `$parent`.
pub(crate) fn reads_row(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => true,
		LogicalExpr::Param(param) => RowScopeKind::from_param_name(param.as_str()).is_some(),
		LogicalExpr::Closure(closure) => {
			let captures = captures_of(closure);
			captures.reads_current
				|| captures.reads_root()
				|| captures.names.iter().any(|name| name == "parent")
		}
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
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
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => {
			let mut reads = false;
			expr.for_each_child(&mut |child| reads = reads || reads_row(child));
			reads
		}
	}
}

/// Whether `expr` is or holds a closure literal that reads the row ([`reads_row`]), outside any
/// subquery: a closure that stands in an index or a method argument, directly, as an invoked
/// literal, or inside an expression the argument holds. A literal nested in the body of another
/// is judged by that body's captures, as [`reads_row`] does.
pub(crate) fn holds_row_reading_closure(expr: &LogicalExpr) -> bool {
	if matches!(expr, LogicalExpr::Closure(_)) {
		return reads_row(expr);
	}
	let mut found = false;
	expr.for_each_child(&mut |child| found = found || holds_row_reading_closure(child));
	found
}

/// Whether `expr`, an index or a method argument, reads the document root it is evaluated
/// against: through the row ([`reads_row`]) or through a call that reads it.
pub(crate) fn step_arg_reads_row(expr: &LogicalExpr) -> bool {
	reads_row(expr) || calls_reading_row(expr)
}

/// Whether `expr` holds a call that reads the row it is evaluated against: a custom or other
/// non-builtin function, which may read `$this`, or a projection function. [`reads_row`] does
/// not count these. A step argument over a closure body does, because the call reads the
/// document root there.
fn calls_reading_row(expr: &LogicalExpr) -> bool {
	// A closure literal is judged by what it captures, which [`reads_row`] already covers.
	if matches!(expr, LogicalExpr::Closure(_)) {
		return false;
	}
	if let LogicalExpr::Call(call) = expr {
		let reads = if let Function::Normal(name) = &call.receiver {
			PROJECTION_FUNCTIONS.contains(&name.as_str())
		} else {
			true
		};
		if reads {
			return true;
		}
	}
	let mut reads = false;
	expr.for_each_child(&mut |child| reads = reads || calls_reading_row(child));
	reads
}

/// What the current value is where a read stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Frame {
	/// The body's own level: the current value is the one at the literal.
	Body,
	/// Inside a construct that binds an element as the current value of its own: an element
	/// filter's predicate, a broadcast, an optional chain, a recursion or a destructure body.
	Element,
	/// Inside an index or a method argument, whose row reads are the document root's and are
	/// recorded where the argument is met.
	Arg,
	/// Inside a statement that runs in the scope of the expression holding it: a block, `LET`,
	/// `RETURN`, `FOR` or `IF`. It is taken over the document root, and its current value is the
	/// one at the literal.
	Statement,
}

#[derive(Debug, Clone, Copy)]
struct Scope {
	frame: Frame,
	/// Inside a rescoping subquery, whose rows are its own: no read is of the literal's frame.
	rescoped: bool,
	/// Inside a statement that runs in the scope of the expression holding it, however deep in
	/// the element bodies it holds: a statement is taken over the document root, so what it reads
	/// of the root is read through the statement.
	statement: bool,
	/// Inside the predicate of an element filter, which binds `$parent` to the row the body runs
	/// over, the document root: the frame the outermost such filter stands in.
	filter: Option<Frame>,
	/// How many closure arguments were in scope where the innermost such filter stands. Only an
	/// argument entered after that, inside the predicate, is nearer to a `$parent` read than the
	/// filter's own binding.
	filter_args: usize,
	/// How many closure arguments were in scope where the innermost statement [`Self::statement`]
	/// counts stands. The statement binds `$this`, `$self` and `$parent` to the document it is
	/// taken over, so only an argument entered after that is nearer to a read of those names.
	statement_args: usize,
	/// Inside a construct that binds an element as the current value of its own, however deep:
	/// the frame may since have become an index or method argument, a block, or the body of a
	/// closure, which keep the element body they stand in. A rescoping subquery ends it.
	under_element: bool,
}

impl Scope {
	fn in_frame(self, frame: Frame) -> Self {
		Self {
			frame,
			..self
		}
	}

	/// The scope of a construct that binds an element as its current value.
	fn in_element(self) -> Self {
		Self {
			frame: Frame::Element,
			under_element: true,
			..self
		}
	}
}

/// The scoped walk behind [`captures_of`]: it carries the argument names of the closures
/// entered and the scope of the read it stands at, and it enters closures, which a flat visit
/// would not tell apart.
struct Walker<'a> {
	args: Vec<&'a str>,
	captures: Captures,
}

impl<'a> Walker<'a> {
	fn name(&mut self, name: &str) {
		if !self.captures.names.iter().any(|n| n == name) {
			self.captures.names.push(name.to_owned());
		}
	}

	fn current_read(&mut self, scope: Scope) {
		if scope.rescoped {
			return;
		}
		match scope.frame {
			Frame::Body => self.captures.reads_current = true,
			// A statement is taken over the document root, and the current value it reads is
			// the one at the literal.
			Frame::Statement => {
				self.captures.reads_current = true;
				self.captures.root_by_statement = true;
			}
			Frame::Element | Frame::Arg => {}
		}
	}

	fn root_read(&mut self, scope: Scope) {
		if scope.rescoped {
			return;
		}
		if scope.statement {
			self.captures.root_by_statement = true;
		} else {
			self.captures.root_by_step = true;
		}
	}

	fn param(&mut self, name: &str, scope: Scope) {
		// An element filter binds `$parent` for its predicate, nearer than a closure argument
		// of that name entered before the filter; one entered inside the predicate is nearer
		// still.
		let filter_binds = name == "parent"
			&& scope.filter.is_some()
			&& !self.args[scope.filter_args.min(self.args.len())..].contains(&name);
		// A statement outside an element body binds the row names to the document it is taken
		// over, nearer than a closure argument of that name entered before the statement.
		let statement_binds = scope.statement
			&& !scope.under_element
			&& !self.args[scope.statement_args.min(self.args.len())..].contains(&name);
		let statement_kind =
			RowScopeKind::from_param_name(name).filter(|_| statement_binds && !scope.rescoped);
		let scope_of = if filter_binds {
			NameScope::RowVarying(RowScopeKind::Parent)
		} else if let Some(kind) = statement_kind {
			NameScope::RowVarying(kind)
		} else {
			NameScope::of(name, &self.args)
		};
		match scope_of {
			NameScope::ClosureArgument | NameScope::Session => {}
			NameScope::RowVarying(kind @ (RowScopeKind::This | RowScopeKind::Self_)) => {
				if !scope.rescoped && matches!(scope.frame, Frame::Body | Frame::Statement) {
					let name = match kind {
						RowScopeKind::This => "this",
						RowScopeKind::Self_ | RowScopeKind::Parent => "self",
					};
					if !self.captures.row_names.contains(&name) {
						self.captures.row_names.push(name);
					}
				}
				self.current_read(scope);
			}
			NameScope::RowVarying(RowScopeKind::Parent) => {
				if !scope.rescoped {
					// An argument of the closure is not a name the literal captures.
					if !self.args.contains(&name) {
						self.name(name);
					}
					// An element filter in the body binds `$parent` to the document root, which
					// the literal captures even where it holds a `$parent` of its own.
					if let Some(frame) = scope.filter {
						self.root_read(scope.in_frame(frame));
						self.captures.parent_in_filter = true;
					} else if scope.statement {
						self.captures.parent_in_block = true;
					}
				}
			}
			NameScope::StatementInvariant => self.name(name),
		}
	}

	/// An index or a method argument: evaluated against the document root.
	fn arg(&mut self, arg: &'a LogicalExpr, scope: Scope) {
		if reads_row(arg) || calls_reading_row(arg) {
			self.root_read(scope);
		}
		if scope.under_element && !scope.rescoped && holds_row_reading_closure(arg) {
			self.captures.nested_step_arg_reads_row = true;
		}
		self.expr(arg, scope.in_frame(Frame::Arg));
	}

	fn destructure(&mut self, fields: &'a [LogicalDestructureField], scope: Scope) {
		for field in fields {
			match field {
				LogicalDestructureField::Aliased(_, body) => {
					self.expr(body, scope.in_element());
				}
				LogicalDestructureField::Destructure(_, nested) => self.destructure(nested, scope),
				LogicalDestructureField::All(_)
				| LogicalDestructureField::Field(_)
				| LogicalDestructureField::Rest => {}
			}
		}
	}

	fn expr(&mut self, expr: &'a LogicalExpr, scope: Scope) {
		match expr {
			LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => {
				self.current_read(scope);
			}
			LogicalExpr::Param(param) => self.param(param.as_str(), scope),
			LogicalExpr::Closure(closure) => {
				if scope.rescoped || (scope.frame == Frame::Arg && !scope.under_element) {
					let inner = captures_of(closure);
					for name in inner.row_names.iter().chain(&inner.nested_row_names) {
						if !self.captures.nested_row_names.contains(name) {
							self.captures.nested_row_names.push(name);
						}
					}
				}
				if scope.under_element && !scope.rescoped {
					let inner = captures_of(closure);
					self.captures.nested_reads_root |=
						inner.root_by_statement || inner.parent_in_filter;
				}
				let depth = self.args.len();
				self.args.extend(closure.args.iter().map(|(arg, _)| arg.as_str()));
				self.expr(&closure.body, scope);
				self.args.truncate(depth);
			}
			LogicalExpr::Subquery(op) => self.op(op, scope),
			LogicalExpr::Call(call) => {
				let custom = !matches!(call.receiver, Function::Normal(_));
				let reads = if let Function::Normal(name) = &call.receiver {
					PROJECTION_FUNCTIONS.contains(&name.as_str())
				} else {
					true
				};
				if reads {
					self.current_read(scope);
				}
				if !scope.rescoped
					&& matches!(&call.receiver, Function::Normal(name)
						if PROJECTION_FUNCTIONS.contains(&name.as_str()))
				{
					self.captures.calls_projection = true;
				}
				if custom && !scope.rescoped && scope.frame != Frame::Arg {
					self.captures.calls_custom = true;
				}
				// A call of a function whose body is one expression is replaced by that body
				// where the closure's unit is bound, with each argument that reads the row
				// substituted where the body names its parameter, which may be an index or a
				// method argument of the body: the document root reads it there.
				if custom && !scope.rescoped && call.args.iter().any(reads_row) {
					self.root_read(scope);
				}
				for arg in &call.args {
					self.expr(arg, scope);
				}
			}
			LogicalExpr::Index {
				base,
				index,
			} => {
				self.expr(base, scope);
				self.arg(index, scope);
			}
			LogicalExpr::Method {
				base,
				args,
				..
			} => {
				self.expr(base, scope);
				for arg in args {
					self.arg(arg, scope);
				}
			}
			LogicalExpr::Filter {
				base,
				predicate,
			} => {
				self.expr(base, scope);
				let inner = Scope {
					filter: scope.filter.or(Some(scope.frame)),
					filter_args: self.args.len(),
					..scope
				};
				self.expr(predicate, inner.in_element());
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			}
			| LogicalExpr::Optional {
				base,
				then: body,
			} => {
				self.expr(base, scope);
				self.expr(body, scope.in_element());
			}
			LogicalExpr::Destructure {
				base,
				fields,
			} => {
				self.expr(base, scope);
				self.destructure(fields, scope);
			}
			LogicalExpr::Recurse(recurse) => {
				self.expr(&recurse.base, scope);
				if let Some(path) = &recurse.path {
					let body = match path {
						LogicalRecursePath::Nested(body) | LogicalRecursePath::Trailing(body) => {
							body
						}
					};
					self.expr(body, scope.in_element());
				}
				if let Some(crate::expr::logical::LogicalRecurseInstruction::Shortest {
					expects,
					..
				}) = &recurse.instruction
				{
					self.expr(expects, scope);
				}
			}
			LogicalExpr::Literal(_)
			| LogicalExpr::Array(_)
			| LogicalExpr::Set(_)
			| LogicalExpr::Object(_)
			| LogicalExpr::RecordId(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Access {
				..
			}
			| LogicalExpr::First(_)
			| LogicalExpr::Last(_)
			| LogicalExpr::Each(_)
			| LogicalExpr::Flatten {
				..
			}
			| LogicalExpr::Lookup {
				..
			}
			| LogicalExpr::RepeatRecurse(_)
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
			| LogicalExpr::If(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue => expr.for_each_child(&mut |child| self.expr(child, scope)),
		}
	}

	/// A statement in scalar position.
	fn op(&mut self, op: &'a LogicalOp, scope: Scope) {
		if matches!(op, LogicalOp::Info(_) | LogicalOp::Ddl(_) | LogicalOp::Sleep(_)) {
			self.root_read_by_statement(scope);
		}
		let inner = if runs_in_enclosing_scope(op) {
			Scope {
				frame: Frame::Statement,
				statement: true,
				statement_args: self.args.len(),
				..scope
			}
		} else {
			if !scope.rescoped && reads_enclosing_row(op) {
				self.captures.root_by_statement = true;
				self.captures.parent_in_statement |= names_parent(op);
			}
			Scope {
				frame: Frame::Body,
				rescoped: true,
				statement: false,
				filter: None,
				filter_args: 0,
				statement_args: 0,
				under_element: false,
			}
		};
		self.op_parts(op, inner);
	}

	fn root_read_by_statement(&mut self, scope: Scope) {
		if !scope.rescoped {
			self.captures.root_by_statement = true;
		}
	}

	/// What a plan reads: a `FROM $param` source, which its scalars do not yield, every
	/// scalar, then its inputs.
	fn op_parts(&mut self, op: &'a LogicalOp, scope: Scope) {
		if let LogicalOp::Source(LogicalSource {
			source: SourceRef::Param(param),
			..
		}) = op
		{
			self.param(param.as_str(), scope);
		}
		for scalar in op.scalars() {
			self.expr(scalar, scope);
		}
		for child in op.children() {
			self.op(child, scope);
		}
	}
}

/// Whether `op`, a plan in scalar position, runs in the scope of the expression holding it
/// rather than over rows of its own: the operators [`rescopes_row_params`] excludes, and the
/// `IF`, `THROW`, `BREAK` and `CONTINUE` statements, whose conditions and values read the
/// enclosing row.
pub(crate) fn runs_in_enclosing_scope(op: &LogicalOp) -> bool {
	!rescopes_row_params(op)
		|| matches!(
			op,
			LogicalOp::If(_) | LogicalOp::Throw(_) | LogicalOp::Break(_) | LogicalOp::Continue(_)
		)
}
