/// The direction of a scan over a key range.
///
/// Lives in its own module rather than inside the cursor or scanner code
/// because every layer (backends, exec operators, indices, doc machinery)
/// needs to name it and they shouldn't all depend on the storage backend
/// or stream-scanner modules to do so.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Direction {
	/// Iterate from `range.start` toward `range.end` (lex-ascending).
	Forward,
	/// Iterate from `range.end - 1` toward `range.start` (lex-descending).
	Backward,
}
