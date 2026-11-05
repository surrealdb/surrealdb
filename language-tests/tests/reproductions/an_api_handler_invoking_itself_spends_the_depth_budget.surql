/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An API handler that invokes its own endpoint spends the computation-depth
	budget level by level as the handler's expressions are evaluated, and the
	innermost request answers with the depth error, rather than recursing
	without bound: as many requests nest as the budget allows, from a statement,
	a function and an event alike.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "'Reached excessive computation depth due to functions, subqueries, or computed values'"

[[test.results]]
value = "500"

[[test.results]]
value = "NONE"

[[test.results]]
value = "31"

[[test.results]]
value = "NONE"

[[test.results]]
value = "31"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

DEFINE API "/r" FOR get THEN { api::invoke("/r") };
RETURN api::invoke("/r").body;
RETURN api::invoke("/r").status;
DEFINE API "/s" FOR get THEN { RETURN { status: 200, body: api::invoke("/s") } };
RETURN array::len(string::split(<string> api::invoke("/s"), 'status'));
DEFINE FUNCTION fn::call() { RETURN api::invoke("/s") };
RETURN array::len(string::split(<string> fn::call(), 'status'));
DEFINE EVENT e ON t THEN { api::invoke("/s") };
CREATE t:1 RETURN NONE;
