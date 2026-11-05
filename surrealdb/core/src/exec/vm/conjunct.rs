//! The conjuncts of a filter's predicate: their cost classes, which of them
//! may move, and the rewrites that reorder or drop them.
//!
//! A filter keeps a row when its predicate is truthy, and `AND` evaluates
//! its operands left to right, stopping at the first falsy one. Moving a
//! conjunct past another changes which of the two is evaluated for a row the
//! first rejects, so a conjunct moves only past conjuncts that are *total*:
//! they raise no error, have no effect, and read nothing beyond the row,
//! the parameters and the literals they name. Between two total conjuncts
//! the order decides only how much work a rejected row costs, never what the
//! statement raises, writes or returns. A conjunct that is not total stays
//! where it is written, and no conjunct crosses it.
