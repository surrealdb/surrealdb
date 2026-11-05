/**
[test]
reason = "An event that writes back to its own table fires again for the record it writes: a chain with a base case runs to it, and one without spends the computation-depth budget and ends in the computation-depth error."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Error while processing event again: Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "[0]"

*/

DEFINE TABLE chain SCHEMALESS;
DEFINE EVENT down ON chain WHEN $event = "CREATE" AND $after.n > 0 THEN { CREATE chain SET n = $after.n - 1 };
CREATE chain:start SET n = 5 RETURN NONE;
SELECT VALUE n FROM chain ORDER BY n;

DEFINE TABLE spin SCHEMALESS;
DEFINE EVENT again ON spin WHEN $event = "CREATE" THEN { CREATE spin };
CREATE spin:start RETURN NONE;
SELECT VALUE count() FROM spin GROUP ALL;
