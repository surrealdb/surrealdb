/**
# A SCHEMAFULL table whose fields are computed from the id a record is given, for the benches whose
# subject is the compute half of a `CREATE` over a range of ids: a `CREATE` evaluates its `SET` against
# no id, so what varies from record to record is the table's own, and it is deterministic. `run = false`
# keeps it out of the regular suite.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE record SCHEMAFULL;
	DEFINE FIELD n ON record TYPE int VALUE record::id(id);
	DEFINE FIELD name ON record TYPE string VALUE 'n' + <string> record::id(id);
	DEFINE FIELD score ON record TYPE float VALUE record::id(id) * 0.5;
	DEFINE FIELD tags ON record TYPE array<string>;
	RETURN 'OK';
}
