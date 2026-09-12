/**
[env]
imports = ["reproductions/udf_frame_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A user-defined function called from a table `select` guard runs in the
	permission frame: the reads inside its body are not themselves
	permission-checked, so a field the caller may not read directly still
	resolves inside the guard. `fn::is_member` reads `membership.active`,
	whose `select` permission is `NONE`, and admits the member.
"""

# 0: the member reads the document.
[[test.results]]
value = "[{ id: doc:1, title: 'd' }]"

# 1: outside a guard the same field is not readable by the record user.
[[test.results]]
value = "[]"
*/

-- 0: the guard's function body reads a NONE-guarded field and admits the row.
SELECT * FROM doc;

-- 1: the table itself stays closed to the record user.
SELECT * FROM membership;
