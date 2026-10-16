/**
[test]
reason = "Setup: people, edges and referencing records gated by table-level and field-level SELECT permissions, some vertices with inline caps, and a record access method, for the traversals over many sources and rows that the record user reads"
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS INLINE EDGES 4 INLINE REFERENCES 4 PERMISSIONS FOR select WHERE hidden != true;
DEFINE FIELD secret ON person PERMISSIONS FOR select WHERE id = $auth.id;
DEFINE TABLE knows SCHEMALESS TYPE RELATION FROM person TO person
	PERMISSIONS FOR select WHERE public = true;
DEFINE TABLE comment SCHEMALESS PERMISSIONS FOR select WHERE public = true;
DEFINE FIELD author ON comment TYPE record<person> REFERENCE;

FOR $i IN 1..=24 {
	CREATE type::record('person', $i) SET n = $i, secret = 's' + <string> $i, hidden = ($i % 5 = 0);
};
FOR $i IN 1..=24 {
	FOR $k IN 0..($i % 4 + 1) {
		LET $edge = type::record('knows', $i * 10 + $k);
		RELATE (type::record('person', $i))->$edge->(type::record('person', ($i * 5 + $k) % 24 + 1))
			SET public = (($i + $k) % 2 = 0);
	};
};
FOR $i IN 1..=24 {
	FOR $k IN 0..($i % 3 + 1) {
		CREATE type::record('comment', $i * 10 + $k)
			SET author = type::record('person', $i), public = (($i + $k) % 2 = 1);
	};
};
