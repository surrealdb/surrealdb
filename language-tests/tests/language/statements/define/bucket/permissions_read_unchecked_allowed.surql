/**
[env]
namespace = true
database = true
imports = ["language/statements/define/bucket/permissions_read_unchecked_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:allowed" }

[env.capabilities]
allow-experimental = ["files"]

[test]
reason = """
	A bucket's PERMISSIONS clause is judged with permissions off: it reads the
	whole of a grant table the record user cannot read, so it admits exactly
	the users the table names.
"""

[[test.results]]
value = '"GRANTED"'

[[test.results]]
value = "[]"
*/

file::get(f"granted:/file.txt").?.to_string();
SELECT * FROM grant;
