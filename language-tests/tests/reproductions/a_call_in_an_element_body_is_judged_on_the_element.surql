/**
[env]
namespace = true
database = true
imports = ["./a_call_in_an_element_body_is_judged_on_the_element_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:probe" }

[test]
reason = """
	A function called in a body evaluated against an element — a `[WHERE …]`
	predicate, a destructure body — runs against that element, and its
	`PERMISSIONS` clause is judged against that element too, not against the
	row the body is evaluated within: a clause admitting the elements and
	refusing the row admits the call, and one admitting the row and refusing
	the elements refuses it. Called on the row itself, the clause is judged
	against the row.
"""

[[test.results]]
value = "[[{ x: 1 }]]"

[[test.results]]
value = "[[{ x: 1 }]]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[2]"

[[test.results]]
error = "You don't have permission to run the fn::big function"

[[test.results]]
error = "You don't have permission to run the fn::big_call function"

[[test.results]]
value = "[9]"

[[test.results]]
value = "[9]"

[[test.results]]
error = "You don't have permission to run the fn::small function"

[[test.results]]
error = "You don't have permission to run the fn::small_call function"
*/

-- Admitted on the elements, though the row is refused.
SELECT VALUE items[WHERE fn::small() = 1] FROM s;
SELECT VALUE items[WHERE fn::small_call() = 1] FROM s;
SELECT VALUE items[1].{ r: fn::small() }.r FROM s;
SELECT VALUE items[1].{ r: fn::small_call() }.r FROM s;

-- Refused on the elements, though the row is admitted.
SELECT VALUE items[WHERE fn::big() = 1] FROM s;
SELECT VALUE items[WHERE fn::big_call() = 1] FROM s;

-- On the row itself, the row is judged.
SELECT VALUE fn::big() FROM s;
SELECT VALUE fn::big_call() FROM s;
SELECT VALUE fn::small() FROM s;
SELECT VALUE fn::small_call() FROM s;
