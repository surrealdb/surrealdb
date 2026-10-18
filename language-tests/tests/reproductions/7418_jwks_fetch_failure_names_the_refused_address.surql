/**
[test]
reason = "Diagnostics: a JWKS fetch refused because the host resolves to a special-use address must say so in the server log. Not expressible in SurrealQL; covered by test_fetch_failure_names_a_refused_address in surrealdb/core/src/iam/jwks.rs."
issue = 7418
run = false
*/

RETURN NONE;
