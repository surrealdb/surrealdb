/**
[test]
reason = "Token-verification: `signin_bearer` in `surrealdb/core/src/iam/signin.rs` populates `ns`, `db`, `ac` and `id` in `Claims`, which routes a database-level bearer JWT to the record-access arm in `surrealdb/core/src/iam/verify.rs` that only accepts `AccessType::Record` and rejects `AccessType::Bearer` with `AccessMethodMismatch` (pinned by `context_is_accepted_on_bearer_for_record` in `signin.rs`). Not expressible in SurrealQL because token verification is transport-level."
issue = 7362
run = false
*/

RETURN NONE;
