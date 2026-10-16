/**
[test]
reason = "Setup for rows_ahead_permissions.surql: a table whose select permission is conditional, linking to a table whose select permission hides every odd record."

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

[[test.results]]
value = "NONE"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('person', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;
DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE author SCHEMALESS PERMISSIONS FOR select WHERE n % 2 = 0;
DEFINE TABLE post SCHEMALESS PERMISSIONS FOR select WHERE shown = true;
CREATE person:bob;
FOR $i IN 0..40 {
	CREATE type::record('author', $i) SET n = $i RETURN NONE;
};
FOR $i IN 0..40 {
	CREATE type::record('post', $i) SET shown = $i % 3 != 0, author = type::record('author', $i) RETURN NONE;
};
