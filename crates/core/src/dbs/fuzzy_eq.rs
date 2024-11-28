/// FuzzyEq trait is used to compare objects while ignoring values that are non-deterministic.
/// Non detereministic values include:
/// - Timestamps
/// - UUIDs
pub trait FuzzyEq<Rhs: ?Sized = Self> {
	/// Use this when comparing objects that you do not want to compare properties that are
	/// non-deterministic
	fn fuzzy_eq(&self, other: &Rhs) -> bool;
}
