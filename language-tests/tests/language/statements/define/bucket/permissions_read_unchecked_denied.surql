/**
[env]
namespace = true
database = true
imports = ["language/statements/define/bucket/permissions_read_unchecked_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:denied" }

[test]
reason = """
	A bucket's PERMISSIONS clause is judged with permissions off: it reads the
	whole of a grant table the record user cannot read, so it admits exactly
	the users the table names.
"""

[[test.results]]
error = "You don't have permission to get this file in the `granted` bucket"

[[test.results]]
value = "[]"
*/

file::get(f"granted:/file.txt").?.to_string();
SELECT * FROM grant;
