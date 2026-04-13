/**

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:test }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNUP ( CREATE type::record('user', $id) )
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE OVERWRITE user SCHEMAFULL PERMISSIONS FULL;

CREATE user:test;

DEFINE FUNCTION OVERWRITE fn::select($record: record<a>) {
	RETURN $record.id != NONE;
};

DEFINE TABLE OVERWRITE a SCHEMAFULL
	PERMISSIONS
		FOR create, update, delete FULL
		FOR select WHERE fn::select(id);
