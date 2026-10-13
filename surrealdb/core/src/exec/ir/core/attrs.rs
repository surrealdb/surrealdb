//! Attributes of an op.
//!
//! An op's attributes are the typed payload of its [`OpKind`](super::op::OpKind) variant, plus the
//! two identity-bearing fields of [`OpData`](super::op::OpData): its frame and its snapshot. There
//! is no string-keyed attribute dictionary. A pattern that reads an attribute names the variant
//! that holds it, so a missing attribute is a compile error, not a run-time lookup that fails.
//!
//! Placement is not an attribute. It lives in the container's placement table, so it never enters
//! an op's identity.
