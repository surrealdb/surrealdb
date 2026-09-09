/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "File-backed modules require an explicit module name"
issue = 7462

[test.results]
parsing-error = true
*/

-- Run with --features surrealdb-core/surrealism to exercise the missing-name check.
-- Without Surrealism, DEFINE MODULE is rejected as unsupported instead.
DEFINE MODULE f"bucket:/file.surli";
