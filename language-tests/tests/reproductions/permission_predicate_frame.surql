/**
[env]
imports = ["reproductions/permission_predicate_frame_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:reader" }

[test]
reason = """
	A table's `PERMISSIONS FOR select` predicate runs in the definer's frame:
	a function or parameter it reads is not permission-checked against the
	caller, so a record user is admitted by a predicate that calls a
	`PERMISSIONS NONE` function or reads a `PERMISSIONS NONE` parameter.
	The predicate's own `PERMISSIONS NONE` reads would fail for this caller
	anywhere outside the predicate.
"""

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
value = "[{ id: doc2:1 }]"

[[test.results]]
error = "You don't have permission to run the fn::is_open function"

[[test.results]]
error = "You don't have permission to view the $open parameter"
*/

SELECT id FROM doc;
SELECT id FROM doc2;
RETURN fn::is_open();
RETURN $open;
