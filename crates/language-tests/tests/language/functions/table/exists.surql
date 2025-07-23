/**
[test]

[[test.results]]
value = "false"

[[test.results]]
value = "none"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "none"

[[test.results]]
value = "true"

*/
table::exists("person");
DEFINE TABLE person;
table::exists("person");

RETURN function() { return surrealdb.functions.table.exists("company"); };
DEFINE TABLE company;
RETURN function() { return surrealdb.functions.table.exists("company"); };
