//! Which user-defined function bodies a call may inline.
//!
//! A call runs its body as a subroutine: a program of its own, run one
//! re-entry deeper in a child of the caller's context that is a call
//! boundary ([`crate::ctx::Context::lexical_value`]), with the arguments bound
//! as parameters. The body is lexically scoped: it reads its arguments, the
//! row names, the session's own values and the request's parameters, and no
//! binding its caller made with `LET` or `FOR`. Inlining replaces only the
//! subroutine: the body's instructions run in the caller's program, reading
//! the bound arguments from registers. The call's boundary stays whole around
//! them: the entry (capability, definition, `AUTH LIMIT`, `PERMISSIONS`), the
//! arguments evaluated in the frame the entry opens, their count and coercion,
//! the re-entry the call consumes, and the coercion of what the body returns.
//!
//! So the body inlined must observe nothing the call's own context would have
//! answered differently: it reads no parameter but its arguments, no row, no
//! record, and no context, and it calls nothing but builtins that are pure
//! and synchronous. Such a body is a function of its arguments alone, and so
//! has the one value wherever it is evaluated.
