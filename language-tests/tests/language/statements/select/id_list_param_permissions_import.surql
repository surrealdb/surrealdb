/**
[test]
reason = "Setup for id_list_param_permissions.surql: a table whose select permission hides every odd record, and a computed field."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:bob }]"

[[test.results]]
value = "NONE"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('person', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE author SCHEMALESS PERMISSIONS FOR select WHERE n % 2 = 0, FOR update WHERE n % 4 = 0, FOR delete NONE;
DEFINE FIELD double ON author COMPUTED n * 2;
CREATE person:bob;
FOR $i IN 0..1300 {
	CREATE type::record('author', $i) SET n = $i RETURN NONE;
};
