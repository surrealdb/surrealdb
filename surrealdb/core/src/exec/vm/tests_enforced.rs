//! Sources the VM covers although their table's declarations are enforced at
//! run time rather than bound as nodes of the plan, and the semi and anti
//! joins the binder lifts out of a filter or a table's permission. Each
//! statement answers with the rows, in their order, and the errors the
//! language gives, for the owner and for a record user whose reads are
//! checked, and none is declined.
