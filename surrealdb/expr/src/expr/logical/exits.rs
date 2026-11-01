//! How control may leave a statement: the exit summary of a [`LogicalOp`].
//!
//! Each flag means *may*. A statement that falls through lets the statement
//! after it in its block run; one that returns, breaks or continues leaves
//! its block with that signal; one that throws raises a written `THROW`.
//! Evaluation errors are always possible and are not part of the summary.
//!
//! The summary is computed from the tree, bottom-up:
//!
//! | Node | Rule |
//! | --- | --- |
//! | a leaf statement | falls through |
//! | `RETURN`, `THROW`, `BREAK`, `CONTINUE` | that signal only |
//! | a block | each statement in order, until the first that cannot fall through; the union of those |
//! | `IF` | the union of its arms; falls through when it has no `ELSE` |
//! | `FOR` | its body's, with `BREAK` and `CONTINUE` absorbed into falling through |
//! | a `LET` value, a `RETURN` value | `RETURN` absorbed into falling through |
//!
//! A statement nested in an expression — a subquery, a block in value
//! position — answers its own signals where it is evaluated: a `RETURN` is
//! the subquery's value, and a `BREAK` or `CONTINUE` leaves the loop the
//! expression is evaluated in. The summary of a statement reports them as the
//! statement's own where they may leave it.

use super::{
	LogicalExpr, LogicalForeach, LogicalIfOp, LogicalLet, LogicalOp, LogicalReturn, LogicalSequence,
};

/// How control may leave a statement; each flag means "may".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Exits {
	/// Control may continue with the statement after it.
	pub falls_through: bool,
	/// A `RETURN` may leave it.
	pub returns: bool,
	/// A `BREAK` may leave it.
	pub breaks: bool,
	/// A `CONTINUE` may leave it.
	pub continues: bool,
	/// A written `THROW` may leave it.
	pub throws: bool,
}

impl Exits {
	/// A statement that only falls through.
	pub const FALLS_THROUGH: Exits = Exits {
		falls_through: true,
		returns: false,
		breaks: false,
		continues: false,
		throws: false,
	};

	/// Either of two summaries.
	pub fn union(self, other: Exits) -> Exits {
		Exits {
			falls_through: self.falls_through || other.falls_through,
			returns: self.returns || other.returns,
			breaks: self.breaks || other.breaks,
			continues: self.continues || other.continues,
			throws: self.throws || other.throws,
		}
	}

	/// This summary with the signals an enclosing construct catches turned
	/// into falling through.
	fn absorbing(self, returns: bool, loops: bool) -> Exits {
		let caught = (returns && self.returns) || (loops && (self.breaks || self.continues));
		Exits {
			falls_through: self.falls_through || caught,
			returns: self.returns && !returns,
			breaks: self.breaks && !loops,
			continues: self.continues && !loops,
			throws: self.throws,
		}
	}

	/// Whether a `BREAK` or `CONTINUE` may leave the statement: at the top of
	/// a request, the signal escapes every loop.
	pub fn escapes_a_loop(self) -> bool {
		self.breaks || self.continues
	}
}

impl LogicalOp {
	/// How control may leave this statement ([`Exits`]).
	pub fn exits(&self) -> Exits {
		match self {
			LogicalOp::Return(LogicalReturn {
				what,
				fetch,
			}) => {
				let value = what.exits().absorbing(true, false);
				let fetched =
					fetch.iter().fold(Exits::default(), |acc, e| acc.union(expr_exits(e)));
				Exits {
					falls_through: false,
					returns: true,
					..value.union(fetched)
				}
			}
			LogicalOp::Throw(throw) => Exits {
				throws: true,
				..expr_exits(&throw.value)
			}
			.without_falling_through(),
			LogicalOp::Break(_) => Exits {
				breaks: true,
				..Exits::default()
			},
			LogicalOp::Continue(_) => Exits {
				continues: true,
				..Exits::default()
			},
			LogicalOp::Sequence(LogicalSequence {
				stmts,
			}) => {
				let mut exits = Exits::FALLS_THROUGH;
				for stmt in stmts {
					let stmt = stmt.exits();
					exits = Exits {
						falls_through: stmt.falls_through,
						..exits.union(stmt)
					};
					if !stmt.falls_through {
						break;
					}
				}
				exits
			}
			LogicalOp::If(ifs) => if_exits(ifs),
			LogicalOp::Foreach(LogicalForeach {
				range,
				body,
				..
			}) => expr_exits(range).union(body.exits().absorbing(false, true)).with_falling_through(),
			LogicalOp::Let(LogicalLet {
				value,
				..
			}) => value.exits().absorbing(true, false),
			other @ (LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Window(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_)) => {
				// A leaf, or a statement whose inputs are pipelines: control
				// leaves it only through a statement nested in its expressions.
				let mut exits = Exits::FALLS_THROUGH;
				other.for_each_scalar(&mut |scalar| exits = exits.union(expr_exits(scalar)));
				for child in other.children() {
					exits = exits.union(child.exits().absorbing(true, false));
				}
				exits
			}
		}
	}
}

impl LogicalSequence {
	/// The statements a block reaches: each in order, up to and including the
	/// first that cannot fall through. Control never gets past that statement,
	/// so the statements after it never run.
	pub fn reachable_prefix(&self) -> &[LogicalOp] {
		let reached = self
			.stmts
			.iter()
			.position(|stmt| !stmt.exits().falls_through)
			.map_or(self.stmts.len(), |at| at + 1);
		&self.stmts[..reached]
	}
}

impl Exits {
	fn with_falling_through(self) -> Exits {
		Exits {
			falls_through: true,
			..self
		}
	}

	fn without_falling_through(self) -> Exits {
		Exits {
			falls_through: false,
			..self
		}
	}
}

/// The summary of a statement `IF`: each condition's, then the union of its
/// arms, falling through where no `ELSE` is written.
fn if_exits(ifs: &LogicalIfOp) -> Exits {
	let mut exits = ifs
		.conditions
		.iter()
		.fold(Exits::FALLS_THROUGH, |acc, condition| acc.union(expr_exits(condition)));
	let arms = ifs.arms.iter().fold(None, |acc: Option<Exits>, arm| {
		Some(acc.map_or_else(|| arm.exits(), |acc| acc.union(arm.exits())))
	});
	let arms = arms.unwrap_or(Exits::FALLS_THROUGH);
	exits = Exits {
		falls_through: arms.falls_through || ifs.otherwise().is_none(),
		..exits.union(arms)
	};
	exits
}

/// The signals a statement nested in `expr` may leave it with: a `BREAK` or
/// `CONTINUE` leaves the loop the expression is evaluated in, and a `THROW`
/// raises; a `RETURN` is the value of the subquery it is in.
fn expr_exits(expr: &LogicalExpr) -> Exits {
	let mut exits = Exits::FALLS_THROUGH;
	let _ = expr.visit(&mut |node| {
		if matches!(node, LogicalExpr::Break) {
			exits.breaks = true;
		} else if matches!(node, LogicalExpr::Continue) {
			exits.continues = true;
		} else if matches!(node, LogicalExpr::Throw(_)) {
			exits.throws = true;
		} else if let LogicalExpr::Subquery(op) = node {
			exits = exits.union(op.exits().absorbing(true, false).with_falling_through());
		}
		std::ops::ControlFlow::<()>::Continue(())
	});
	exits
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::logical::lower;
	use crate::parser::TopLevelExpr;

	fn exits(sql: &str) -> Exits {
		let ast = crate::parser::parse(sql).expect("test statement should parse");
		let top = ast.expressions.into_iter().next().expect("one statement");
		let TopLevelExpr::Expr(expr) = top else {
			panic!("expected a bare expression");
		};
		lower(expr).expect("statement should lower").exits()
	}

	fn of(
		falls_through: bool,
		returns: bool,
		breaks: bool,
		continues: bool,
		throws: bool,
	) -> Exits {
		Exits {
			falls_through,
			returns,
			breaks,
			continues,
			throws,
		}
	}

	#[test]
	fn each_node_kind_has_its_summary() {
		// A leaf statement falls through.
		assert_eq!(exits("SELECT * FROM t"), Exits::FALLS_THROUGH);
		assert_eq!(exits("1 + 1"), Exits::FALLS_THROUGH);
		// The terminators.
		assert_eq!(exits("RETURN 1"), of(false, true, false, false, false));
		assert_eq!(exits("THROW 'x'"), of(false, false, false, false, true));
		assert_eq!(exits("BREAK"), of(false, false, true, false, false));
		assert_eq!(exits("CONTINUE"), of(false, false, false, true, false));
		// A block stops at its first statement that cannot fall through.
		assert_eq!(exits("{ 1; RETURN 2; THROW 3 }"), of(false, true, false, false, false));
		assert_eq!(exits("{ 1; 2 }"), Exits::FALLS_THROUGH);
		// An `IF` is the union of its arms, falling through without an `ELSE`.
		assert_eq!(exits("IF $a { BREAK }"), of(true, false, true, false, false));
		assert_eq!(exits("IF $a { BREAK } ELSE { RETURN 1 }"), of(false, true, true, false, false));
		assert_eq!(exits("IF $a { BREAK } ELSE { 1 }"), of(true, false, true, false, false));
		// A loop absorbs its body's `BREAK` and `CONTINUE`.
		assert_eq!(exits("FOR $x IN [1] { IF $x { BREAK }; CONTINUE }"), Exits::FALLS_THROUGH);
		assert_eq!(
			exits("FOR $x IN [1] { IF $x { THROW 'x' }; RETURN 1 }"),
			of(true, true, false, false, true)
		);
		// A `LET` value absorbs a `RETURN` inside it.
		assert_eq!(exits("LET $v = { RETURN 1 }"), Exits::FALLS_THROUGH);
		assert_eq!(exits("LET $v = IF $a { BREAK }"), of(true, false, true, false, false));
	}

	#[test]
	fn a_block_reaches_its_statements_up_to_the_first_that_exits() {
		fn reached(sql: &str) -> usize {
			let ast = crate::parser::parse(sql).expect("test statement should parse");
			let top = ast.expressions.into_iter().next().expect("one statement");
			let TopLevelExpr::Expr(expr) = top else {
				panic!("expected a bare expression");
			};
			let LogicalOp::Sequence(sequence) = lower(expr).expect("block should lower") else {
				panic!("expected a block");
			};
			sequence.reachable_prefix().len()
		}
		assert_eq!(reached("{ 1; 2 }"), 2);
		assert_eq!(reached("{ 1; RETURN 2; THROW 3 }"), 2);
		assert_eq!(reached("{ RETURN 1; LET $auth = 2 }"), 1);
		assert_eq!(reached("{ THROW 'a'; LET $session = 1 }"), 1);
		assert_eq!(reached("{ IF $a { RETURN 1 }; 2 }"), 2);
	}

	#[test]
	fn a_signal_nested_in_an_expression_leaves_the_statement() {
		// A `BREAK` in a subquery leaves the loop the expression is in.
		assert_eq!(exits("CREATE t SET a = { BREAK }"), of(true, false, true, false, false));
		// A `RETURN` in a subquery is the subquery's value.
		assert_eq!(exits("CREATE t SET a = { RETURN 1 }"), Exits::FALLS_THROUGH);
		assert!(exits("BREAK").escapes_a_loop());
		assert!(!exits("FOR $x IN [1] { BREAK }").escapes_a_loop());
	}
}
