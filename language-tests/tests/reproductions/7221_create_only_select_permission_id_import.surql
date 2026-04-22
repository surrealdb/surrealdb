/**

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:test }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/

DEFINE TABLE OVERWRITE user SCHEMAFULL PERMISSIONS FULL;

CREATE user:test;

DEFINE FUNCTION OVERWRITE fn::select($record: record<a>) {
	RETURN $record.is_record();
};

DEFINE TABLE OVERWRITE a SCHEMAFULL
	PERMISSIONS
		FOR create, update, delete FULL
		FOR select WHERE fn::select(id);
