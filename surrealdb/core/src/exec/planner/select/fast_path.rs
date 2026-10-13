//! The pipelines that compile to a single scan.
//!
//! A fast path is a claim about a pipeline's *shape*: that the operators it
//! replaces are not in the tree, so one scan can answer the statement. Each
//! matcher here writes that shape out over the clauses the fold collected and
//! the sources it stopped at, checks it, and hands back the parts the shape
//! guarantees — so the compiler that follows never re-derives them and never
//! needs a guard for a case the match already excluded.
//!
//! Every matcher destructures [`PipelineClauses`] with no `..`. Adding a
//! clause to the collection therefore fails to compile here until each fast
//! path states its position on it, which is the whole point: a clause nobody
//! placed would otherwise be dropped silently by a path that skips the
//! operator carrying it.
//!
//! `EXPLAIN` never reaches a matcher: the fold applies it to whatever operator
//! the pipeline compiles to.
