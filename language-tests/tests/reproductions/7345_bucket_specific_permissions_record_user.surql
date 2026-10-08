/**
[env]
imports = ["reproductions/7345_bucket_specific_permissions_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bob" }

[env.capabilities]
allow-experimental = ["files"]

[test]
reason = "Issue 7345: record users must not bypass DEFINE BUCKET PERMISSIONS WHERE in the file executor."
issue = 7345

[[test.results]]
error = "You don't have permission to get this file in the `deny_where` bucket"

[[test.results]]
value = '"ALLOWED"'

[[test.results]]
error = "You don't have permission to get this file in the `file_gate` bucket"

[[test.results]]
value = "NONE"

[[test.results]]
error = "You don't have permission to copy this file in the `target_gate` bucket"
*/

file::get(f"deny_where:/secret.txt").?.to_string();
file::get(f"file_gate:/allowed.txt").?.to_string();
file::get(f"file_gate:/denied.txt").?.to_string();
file::copy(f"target_gate:/source.txt", "allowed-copy.txt");
file::copy(f"target_gate:/source.txt", "denied-copy.txt");
