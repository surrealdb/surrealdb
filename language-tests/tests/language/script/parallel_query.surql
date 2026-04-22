/**
[test]

[[test.results]]
value = "[1f, 1f]"

*/
RETURN function() {
	return await Promise.all([
		surrealdb.query("1"),
		surrealdb.query("1")
	])
}
