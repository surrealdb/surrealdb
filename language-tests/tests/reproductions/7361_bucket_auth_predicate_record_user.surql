/**
[env]
imports = ["reproductions/7361_bucket_auth_predicate_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bob" }

[env.capabilities]
allow-experimental = ["files"]

[test]
reason = "Issue 7361: top-level file functions must evaluate bucket predicates that dereference $auth with database context and return clean permission denials."
issue = 7361

[[test.results]]
error = "You don't have permission to get this file in the `auth_gate` bucket"

[[test.results]]
error = "You don't have permission to exists this file in the `auth_gate` bucket"

[[test.results]]
error = "You don't have permission to put this file in the `auth_gate` bucket"

[[test.results]]
error = "You don't have permission to get this file in the `auth_gate` bucket"

[[test.results]]
error = "You don't have permission to list this file in the `auth_gate` bucket"
*/

file::get(f"auth_gate:/secret.txt").?.to_string();
file::exists(f"auth_gate:/secret.txt");
file::put(f"auth_gate:/new.txt", "NEW");
fn::read_auth_gate(f"auth_gate:/secret.txt");
file::list("auth_gate");
