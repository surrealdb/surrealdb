/**
# An empty `record` table whose select permission carries a row-level predicate,
# and the record user (`user:bench`) the bench session runs as. Create is `FULL`,
# so a bench that creates records as that user judges the select clause against
# each record it creates, unless the stages can prove the clause inert. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE user SCHEMALESS;
	CREATE user:bench SET name = 'bench';

	DEFINE ACCESS user ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM type::record('user', $id) )
		DURATION FOR TOKEN 1h, FOR SESSION 1h;

	DEFINE TABLE record SCHEMALESS PERMISSIONS FOR select WHERE age < 200 FOR create, update, delete FULL;

	RETURN 'OK';
}
