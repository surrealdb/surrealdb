//! Cardinality hints for execution operators.
//!
//! Provides a [`CardinalityHint`] enum that operators use to declare how many
//! rows they expect to produce. This information is consumed by
//! [`super::buffer::buffer_stream`] to choose an appropriate buffering strategy:
//!
//! - **`AtMostOne`**: No buffering — spawning a task + channel for a single value is pure overhead.
//! - **`Unbounded`**: full buffering according to [`super::access_mode::AccessMode`].

/// Cardinality hint for an execution operator's output stream.
///
/// This is a *static* estimate known at plan-construction time. It describes
/// an upper bound on the number of rows the operator will produce, which
/// callers use to right-size their buffering strategy.
///
/// The default for [`super::ExecOperator`] is `Unbounded` (conservative),
/// so only operators with known small cardinality need to override.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CardinalityHint {
	/// At most one row (point lookups, scalar expressions).
	AtMostOne,
	/// Unknown or potentially large.
	Unbounded,
}
