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
schema::table::exists("person");
DEFINE TABLE person;
schema::table::exists("person");

RETURN function() { return surrealdb.functions.schema.table.exists("company"); };
DEFINE TABLE company;
RETURN function() { return surrealdb.functions.schema.table.exists("company"); };
