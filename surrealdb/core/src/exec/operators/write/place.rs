//! A place: the address a data clause writes to, compiled.
//!
//! `SET a.b = 1`, `SET tags[WHERE t = 'x'].n += 1`, `SET a[$i] = 2`,
//! `UNSET a.b` — the left-hand side of each is an [`Idiom`], walked into the
//! record and mutated where it lands. A place is not an expression: an
//! expression produces a value, a place names where a value goes, so the
//! [`PlaceStep`]s a path compiles to are what the mutators walk, and a read
//! path — which composes a path into something that *reads* — cannot stand in
//! for them.
//!
//! What a place shares with a read path is the expressions a path embeds:
//! `[WHERE …]` carries a predicate, `[expr]` a subscript. Those are compiled
//! once, when the statement is planned, into [`Clause`]s. The write
//! operator has no other evaluator, so an expression left uncompiled inside a
//! place could not be evaluated at all when the place is walked.
//!
//! # A place also reads
//!
//! A compound assignment — `+=`, `-=`, `+?` — combines its right-hand side
//! with what the place already holds, so the address has to answer a read as
//! well as a write. That read walks these same steps
//! ([`read`](super::apply::read)) rather than compiling the path as an
//! expression: an expression read resolves what a `[…]` subscript embeds
//! against the record, where [`subscript`] resolves it with no record in
//! scope. Walking the steps is what makes `SET a[b] += 1` combine with the
//! value at the key it writes to rather than with the value at the record's
//! `b` field.
//!
//! # Which parts a place can address
//!
//! A place addresses the parts the record mutators descend through, and only
//! those. Each is one [`PlaceStep`]:
//!
//! | Part | As a place |
//! |------|------------|
//! | `Field(name)` | A constant object key. |
//! | `Lookup(l)` | A constant object key, spelled as the lookup renders. |
//! | `Value(expr)` | An object key or an array index, from a per-record subscript. |
//! | `Where(pred)` | The elements of an array the predicate accepts. |
//! | `First` / `Last` | The first or last element of an array. |
//! | `All` | Every element of an array, every value of an object. |
//!
//! Every other part — `Flatten`, `Start`, `Method`, `Destructure`, `Optional`,
//! `Recurse`, `Doc` and `RepeatRecurse` — names no address, and compiles to
//! [`PlaceStep::Unaddressed`]. A walk that reaches it has landed on something it
//! does not address, so the assignment writes nothing and a removal removes
//! nothing; the steps before it are walked as usual, and the steps after it are
//! never reached. Dropping such a part instead would widen the place to its
//! prefix and write over a part of the record the statement never named.
//!
//! # The steps are an address, not a walk
//!
//! Nothing here descends into a record. The three walks over these steps
//! disagree at every one of them — an assignment creates the objects an absent
//! path needs and writes at the end of it, a removal stops where the path stops
//! and removes what it lands on, a read yields what is there and `NONE` where
//! nothing is — so a shared walk would be parameterised by the difference at
//! each step until it was three walks in one body. What they do share is this
//! address, and the frames its embedded expressions are evaluated in:
//! [`subscript`] and [`accepts`].

use crate::exec::{BoxFut, Clause};
use crate::expr::ControlFlow;
use crate::val::Value;

/// One step of a place: one part of the path, with whatever it embeds
/// compiled.
///
/// `E` is how an embedded expression is held: a compiled [`Clause`], or the
/// index of the pipeline of a lowered program that computes it. The mutators walk either through
/// the [`PlaceEval`] that knows how to evaluate that form.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PlaceStep<E = Clause> {
	/// `.name` — an object key fixed by the statement.
	Field(String),
	/// `->edge`, `<-edge` and their kin. A place addresses a lookup as the
	/// constant object key it renders to, never as a graph traversal: the
	/// record being written is a value, and a lookup written into one of its
	/// paths is a key like any other.
	Lookup(String),
	/// `[expr]` — an object key or an array index, evaluated per record by
	/// [`subscript`]. Which of the two it is depends on what the step lands on
	/// and on the value the subscript produces, so it is one step rather than
	/// two.
	Subscript {
		/// The subscript, evaluated per record.
		expr: E,
		/// Whether the statement wrote a literal number here rather than an
		/// expression. A record mutator reads it only to decide how a step
		/// *following* a [`PlaceStep::Where`] counts, which is the one place
		/// the two disagree: an assignment takes the special reading for a
		/// literal index alone, a removal for any subscript.
		literal_index: bool,
	},
	/// `[WHERE …]` — the elements of an array the predicate accepts, judged
	/// one element at a time by [`accepts`].
	///
	/// The step selects a *set* of elements, so what follows it applies to each
	/// of them, and an index that follows it counts within the selection rather
	/// than within the array: `tags[WHERE n > 1][0]` is the first accepted
	/// element, not element zero.
	Where(E),
	/// The first element of an array, which a path renders as `[0]`. Written
	/// text never produces it: `a[0]` parses as a literal subscript and
	/// compiles to [`PlaceStep::Subscript`], so this step reaches a place only
	/// from a path built without the parser.
	First,
	/// `[$]` — the last element of an array.
	Last,
	/// `[*]` — every element of an array, and every value of an object. As with
	/// [`PlaceStep::Where`] it selects a set, and what follows applies to each
	/// member.
	All,
	/// A part that names no address — a method call, a destructure, a
	/// recursion. A walk that reaches it lands on something it does not
	/// address and stops there: nothing is written, nothing is removed, and a
	/// read finds `NONE`.
	Unaddressed,
}

/// How the mutators evaluate what a place embeds, for one form of
/// [`PlaceStep`]. Both questions are answered with no record in scope but the
/// element a `[WHERE …]` judges, and a `RETURN` inside the expression is its
/// value.
pub(crate) trait PlaceEval: Send {
	/// How an embedded expression is held in the steps this evaluates.
	type Expr: Send + Sync;

	/// The value of a [`PlaceStep::Subscript`]'s expression.
	fn subscript<'a>(&'a mut self, expr: &'a Self::Expr) -> BoxFut<'a, Result<Value, ControlFlow>>;

	/// Whether a [`PlaceStep::Where`]'s predicate accepts `element`.
	fn accepts<'a>(
		&'a mut self,
		expr: &'a Self::Expr,
		element: &'a Value,
	) -> BoxFut<'a, Result<bool, ControlFlow>>;
}
