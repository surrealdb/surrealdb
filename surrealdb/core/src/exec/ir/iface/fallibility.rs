//! What an op's failure can depend on.
//!
//! Every op declares a fallibility class beside its effects. There is no default: a type that
//! holds a [`Fallibility`] names its bits at every construction, a dialect answers by a match that
//! names every op variant with no wildcard arm (`clippy::wildcard_enum_match_arm` is denied in
//! those files), and a builtin function states its class through
//! [`declare_fallibility!`](crate::declare_fallibility), which fails the build unless the
//! `FALLIBILITY` row agrees. An op or builtin that never declared a class does not compile, and a
//! stub that cannot answer names [`Fallibility::TOP`], the weakest assumption a consumer can be
//! handed.

use ::core::fmt;
use ::core::ops::BitOr;
use surrealdb_strand::Strand;

/// The failure dependencies of an op, as a set: an opaque body is `CONTEXT | DATA`.
///
/// An empty set is [`Fallibility::INFALLIBLE`]: the op raises for no input, context or stored
/// state. `Resource` failures (depth, fuel, memory, timeout, cancellation) are ambient on every
/// op: no set holds them, and no consumer may assume their absence.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Fallibility(u8);

impl Fallibility {
	/// Raises for no input, context or stored state.
	pub(crate) const INFALLIBLE: Self = Self(0);
	/// Raises as a function of its inputs only: overflow, a cast or type mismatch, a certain raise.
	pub(crate) const VALUE: Self = Self(1);
	/// Raises as a function of the principal, the session or the server configuration.
	pub(crate) const CONTEXT: Self = Self(1 << 1);
	/// Raises as a function of stored records or of the catalog at run time.
	pub(crate) const DATA: Self = Self(1 << 2);
	/// Every dependency: the answer of an op nothing is known about.
	pub(crate) const TOP: Self = Self::VALUE.union(Self::CONTEXT).union(Self::DATA);

	/// The dependencies of either operand.
	pub(crate) const fn union(self, other: Self) -> Self {
		Self(self.0 | other.0)
	}

	/// Whether every dependency of `other` is a dependency of `self`.
	pub(crate) const fn contains(self, other: Self) -> bool {
		self.0 & other.0 == other.0
	}

	/// Whether both sets hold the same dependencies; `==` is not usable in a `const` context.
	pub(crate) const fn is_exactly(self, other: Self) -> bool {
		self.0 == other.0
	}

	/// Whether the set is empty.
	pub(crate) const fn is_infallible(self) -> bool {
		self.0 == 0
	}

	/// The strongest member, which is what a consumer's side condition is stated over. Never
	/// [`FallibilityClass::Resource`], which no set holds.
	pub(crate) const fn class(self) -> FallibilityClass {
		if self.contains(Self::DATA) {
			FallibilityClass::Data
		} else if self.contains(Self::CONTEXT) {
			FallibilityClass::Context
		} else if self.contains(Self::VALUE) {
			FallibilityClass::Value
		} else {
			FallibilityClass::Infallible
		}
	}
}

impl BitOr for Fallibility {
	type Output = Self;

	fn bitor(self, other: Self) -> Self {
		self.union(other)
	}
}

impl fmt::Debug for Fallibility {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if self.is_infallible() {
			return f.write_str("INFALLIBLE");
		}
		let mut first = true;
		for (flag, name) in
			[(Self::VALUE, "VALUE"), (Self::CONTEXT, "CONTEXT"), (Self::DATA, "DATA")]
		{
			if self.contains(flag) {
				if !first {
					f.write_str(" | ")?;
				}
				f.write_str(name)?;
				first = false;
			}
		}
		Ok(())
	}
}

/// The classes a consumer states its side condition over, weakest first.
///
/// `Resource` is ambient on every op: no op declares it, so [`Fallibility::class`] never answers
/// it, and a consumer that needs "cannot fail at all" must not assume it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FallibilityClass {
	Infallible,
	Value,
	Context,
	Data,
	Resource,
}

/// Each dependency a `CONTEXT` failure rests on.
///
/// A fold or cache entry that relies on the op not failing records each dependency as a premise,
/// or does not rely on it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ContextDep {
	/// Covered by the build identity's capabilities.
	Capabilities,
	/// Covered by the frame class: the principal class, the `AUTH LIMIT` chain included.
	PrincipalClass,
	/// The role level (viewer, editor, owner). Never a premise.
	PrincipalRole,
	/// The principal's values (`$auth` fields and the like). Never a premise, and never folds.
	PrincipalValue,
	/// Covered by the program key's namespace and database scope.
	SessionDb,
	/// A function's `PERMISSIONS` clause: a premise only when it is `FULL` or `NONE`.
	FunctionPermission {
		name: Strand,
	},
	/// A parameter's `PERMISSIONS` clause: a premise only when it is `FULL` or `NONE`.
	ParamPermission {
		name: Strand,
	},
	/// A server configuration value.
	ServerConfig {
		key: &'static str,
	},
	/// A backend capability, such as versioned reads on a backend without versions.
	Backend {
		capability: &'static str,
	},
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn top_holds_every_dependency_and_infallible_none() {
		assert!(Fallibility::TOP.contains(Fallibility::VALUE));
		assert!(Fallibility::TOP.contains(Fallibility::CONTEXT));
		assert!(Fallibility::TOP.contains(Fallibility::DATA));
		assert!(Fallibility::INFALLIBLE.is_infallible());
		assert!(!Fallibility::TOP.is_infallible());
		assert_eq!(Fallibility::VALUE | Fallibility::CONTEXT | Fallibility::DATA, Fallibility::TOP);
	}

	#[test]
	fn class_is_the_strongest_member() {
		assert_eq!(Fallibility::INFALLIBLE.class(), FallibilityClass::Infallible);
		assert_eq!(Fallibility::VALUE.class(), FallibilityClass::Value);
		assert_eq!((Fallibility::VALUE | Fallibility::CONTEXT).class(), FallibilityClass::Context);
		assert_eq!((Fallibility::VALUE | Fallibility::DATA).class(), FallibilityClass::Data);
		assert_eq!(Fallibility::TOP.class(), FallibilityClass::Data);
	}

	#[test]
	fn debug_names_the_members() {
		assert_eq!(format!("{:?}", Fallibility::INFALLIBLE), "INFALLIBLE");
		assert_eq!(format!("{:?}", Fallibility::VALUE), "VALUE");
		assert_eq!(format!("{:?}", Fallibility::CONTEXT | Fallibility::DATA), "CONTEXT | DATA");
		assert_eq!(format!("{:?}", Fallibility::TOP), "VALUE | CONTEXT | DATA");
	}
}
