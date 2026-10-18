/**
[test]
reason = "Same token-verification gap as 7362_bearer_for_record_jwt_is_usable (reported against the `DEFINE ACCESS ... TYPE BEARER` documentation): presenting a database-level bearer-signin JWT to `iam::verify::token` hits the `(ns, db, ac, id)` record-access arm in `surrealdb/core/src/iam/verify.rs` and is rejected with `AccessMethodMismatch` (or a record-id parse failure for `BEARER FOR USER`). Not expressible in SurrealQL."
issue = 7498
run = false
*/

RETURN NONE;
