/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
record::id over a field some rows lack, inside WHERE. A function rejects NONE
where it requires a record, in WHERE as anywhere else, so a row without the
field fails the statement unless the call is guarded: by a condition before
it, or by a default in place of NONE.
"""
issue = 4761

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function record::id(). Argument 1 was the wrong type. Expected `record` but found `NONE`"

[[test.results]]
error = "There was a problem running the id() function. no such method found for the none type"

[[test.results]]
value = "[A:1]"

[[test.results]]
value = "[A:1]"

[[test.results]]
value = "[A:2]"
*/
CREATE A:1 SET b = (CREATE ONLY B:1) RETURN NONE;
CREATE A:2 SET b = (CREATE ONLY B:2) RETURN NONE;
CREATE A:3 RETURN NONE;
SELECT * FROM A WHERE record::id(b.id) == 1;
SELECT VALUE id FROM A WHERE b.id.id() == 1;
SELECT VALUE id FROM A WHERE b.id && record::id(b.id) == 1;
SELECT VALUE id FROM A WHERE record::id(b.id ?? B:0) == 1;
SELECT VALUE id FROM A WHERE (b.id ?? B:0).id() == 2;
