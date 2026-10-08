/**
[test]
reason = """
A function whose script queries it again, eight at a time, recurses until the
computation-depth budget runs out, and the error nests once per level that
reached it. A level consumes one unit per call and per planning level, as it
does for a function body or an `eval` query, so a call costs the same however
its query is written.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = """
Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Reached excessive computation depth due to functions, subqueries, or computed values



















"""
*/

define function fn::deep_parallel() {
	return function() {
		await Promise.all([
			surrealdb.query("return fn::deep_parallel()"),
			surrealdb.query("return fn::deep_parallel()"),
			surrealdb.query("return fn::deep_parallel()"),
			surrealdb.query("return fn::deep_parallel()"),
			surrealdb.query("return fn::deep_parallel()"),
			surrealdb.query("return fn::deep_parallel()"),
			surrealdb.query("return fn::deep_parallel()"),
			surrealdb.query("return fn::deep_parallel()"),
		])
	};
};
return fn::deep_parallel();
