//! `sql` -> `expr` conversions for [`crate::sql::part`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::part::*;

impl From<Part> for crate::expr::Part {
	fn from(v: Part) -> Self {
		match v {
			Part::All => Self::All,
			Part::Flatten => Self::Flatten,
			Part::Last => Self::Last,
			Part::First => Self::First,
			Part::Field(ident) => Self::Field(ident),
			Part::Where(value) => Self::Where(value.into()),
			Part::Graph(graph) => Self::Lookup(Box::new((*graph).into())),
			Part::Value(value) => Self::Value(value.into()),
			Part::Start(value) => Self::Start(value.into()),
			Part::Method(method, values) => {
				Self::Method(method, values.into_iter().map(Into::into).collect())
			}
			Part::Destructure(parts) => {
				Self::Destructure(parts.into_iter().map(Into::into).collect())
			}
			Part::Optional => Self::Optional,
			Part::Recurse(recurse, idiom, instructions) => {
				let idiom = idiom.map(|idiom| idiom.into());
				let instructions = instructions.map(Into::into);
				crate::expr::Part::Recurse(recurse.into(), idiom, instructions)
			}
			Part::Doc => crate::expr::Part::Doc,
			Part::RepeatRecurse => crate::expr::Part::RepeatRecurse,
		}
	}
}

impl From<crate::expr::Part> for Part {
	fn from(v: crate::expr::Part) -> Self {
		match v {
			crate::expr::Part::All => Self::All,
			crate::expr::Part::Flatten => Self::Flatten,
			crate::expr::Part::Last => Self::Last,
			crate::expr::Part::First => Self::First,
			crate::expr::Part::Field(ident) => Self::Field(ident),
			crate::expr::Part::Where(value) => Self::Where(value.into()),
			crate::expr::Part::Lookup(graph) => Self::Graph(Box::new((*graph).into())),
			crate::expr::Part::Value(value) => Self::Value(value.into()),
			crate::expr::Part::Start(value) => Self::Start(value.into()),
			crate::expr::Part::Method(method, values) => {
				Self::Method(method, values.into_iter().map(Into::into).collect())
			}
			crate::expr::Part::Destructure(parts) => {
				Self::Destructure(parts.into_iter().map(Into::<DestructurePart>::into).collect())
			}
			crate::expr::Part::Optional => Self::Optional,
			crate::expr::Part::Recurse(recurse, idiom, instructions) => Self::Recurse(
				recurse.into(),
				idiom.map(|idiom| idiom.into()),
				instructions.map(Into::into),
			),
			crate::expr::Part::Doc => Self::Doc,
			crate::expr::Part::RepeatRecurse => Self::RepeatRecurse,
		}
	}
}

impl From<DestructurePart> for crate::expr::part::DestructurePart {
	fn from(v: DestructurePart) -> Self {
		match v {
			DestructurePart::All(v) => Self::All(v),
			DestructurePart::Field(v) => Self::Field(v),
			DestructurePart::Aliased(v, idiom) => Self::Aliased(v, idiom.into()),
			DestructurePart::Destructure(v, d) => {
				Self::Destructure(v, d.into_iter().map(Into::into).collect())
			}
		}
	}
}

impl From<crate::expr::part::DestructurePart> for DestructurePart {
	fn from(v: crate::expr::part::DestructurePart) -> Self {
		match v {
			crate::expr::part::DestructurePart::All(v) => Self::All(v),
			crate::expr::part::DestructurePart::Field(v) => Self::Field(v),
			crate::expr::part::DestructurePart::Aliased(v, idiom) => Self::Aliased(v, idiom.into()),
			crate::expr::part::DestructurePart::Destructure(v, d) => {
				Self::Destructure(v, d.into_iter().map(Into::<DestructurePart>::into).collect())
			}
		}
	}
}

impl From<Recurse> for crate::expr::part::Recurse {
	fn from(v: Recurse) -> Self {
		match v {
			Recurse::Fixed(v) => Self::Fixed(v),
			Recurse::Range(min, max) => Self::Range(min, max),
		}
	}
}

impl From<crate::expr::part::Recurse> for Recurse {
	fn from(v: crate::expr::part::Recurse) -> Self {
		match v {
			crate::expr::part::Recurse::Fixed(v) => Self::Fixed(v),
			crate::expr::part::Recurse::Range(min, max) => Self::Range(min, max),
		}
	}
}

impl From<RecurseInstruction> for crate::expr::part::RecurseInstruction {
	fn from(v: RecurseInstruction) -> Self {
		match v {
			RecurseInstruction::Path {
				inclusive,
			} => Self::Path {
				inclusive,
			},
			RecurseInstruction::Collect {
				inclusive,
			} => Self::Collect {
				inclusive,
			},
			RecurseInstruction::Shortest {
				expects,
				inclusive,
			} => Self::Shortest {
				expects: expects.into(),
				inclusive,
			},
		}
	}
}

impl From<crate::expr::part::RecurseInstruction> for RecurseInstruction {
	fn from(v: crate::expr::part::RecurseInstruction) -> Self {
		match v {
			crate::expr::part::RecurseInstruction::Path {
				inclusive,
			} => Self::Path {
				inclusive,
			},
			crate::expr::part::RecurseInstruction::Collect {
				inclusive,
			} => Self::Collect {
				inclusive,
			},
			crate::expr::part::RecurseInstruction::Shortest {
				expects,
				inclusive,
			} => Self::Shortest {
				expects: expects.into(),
				inclusive,
			},
		}
	}
}
