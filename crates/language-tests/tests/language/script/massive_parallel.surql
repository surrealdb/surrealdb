/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = """
Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Problem with embedded script function. An exception occurred: Reached excessive computation depth due to functions, subqueries, or futures














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
