/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "'The daily news'"

*/
LET $test = CREATE ONLY article:test SET name = "The daily news", issue_number = 3;
RETURN function() {
	return await surrealdb.value(`$test.name`)
}
