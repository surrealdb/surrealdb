/**
[env]
imports = ["reproductions/7361_bucket_permission_reads_auth_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bob" }

[env.capabilities]
allow-experimental = ["files"]

[test]
reason = "A top-level file function in a record session evaluates a bucket predicate that dereferences `$auth` to a clean permission refusal, as the same call from a function does, and never to an internal error about a missing database context."
issue = 7361

[[test.results]]
error = "You don't have permission to get this file in the `b` bucket"

[[test.results]]
error = "You don't have permission to exists this file in the `b` bucket"

[[test.results]]
error = "You don't have permission to get this file in the `b` bucket"

[[test.results]]
error = "You don't have permission to list this file in the `b` bucket"
*/

RETURN <string>file::get(f"b:/x.md");
RETURN file::exists(f"b:/x.md");
RETURN fn::read(f"b:/x.md");
RETURN file::list("b");
