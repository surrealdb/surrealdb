/**
[env]
namespace = true
database = true
imports = ["./a_stored_guard_judges_every_row_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:probe" }

[test]
reason = """
	A stored `PERMISSIONS` clause is judged against the row the read or the
	call is made on, so a projection reading a parameter or calling a function
	judges it once per row. The clause is *compiled* once for the definition
	the evaluation found — compiling it per evaluation is plan-time work in a
	row loop — but what the compiled predicate answers is the row's, and a
	guard that admits one row and refuses another must still refuse.
"""

[[test.results]]
value = "['yes', 'yes', 'yes']"

[[test.results]]
value = "['yes', 'yes', 'yes']"

[[test.results]]
error = "You don't have permission to view the $some parameter"

[[test.results]]
error = "You don't have permission to run the fn::some function"
*/

-- Admitted on every row.
SELECT VALUE $always FROM t;
SELECT VALUE fn::always() FROM t;

-- Refused on the row the clause does not admit, whatever earlier rows answered.
SELECT VALUE $some FROM t;
SELECT VALUE fn::some() FROM t;
