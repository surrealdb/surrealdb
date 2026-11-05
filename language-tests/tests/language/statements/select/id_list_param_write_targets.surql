/**
[test]
reason = """
	An UPDATE or DELETE of a parameter holding record ids names the tables of
	those ids as the same list written as the target does, so a synchronous
	event of the table that redefines a field is seen by a read later in the
	same block, whether it names the table by the parameter or by a literal.
	The same parameter read as a whole source beside the write reads the
	records as the write left them.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: 2, id: person:1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[200]"

[[test.results]]
value = "[300]"

[[test.results]]
value = "[4]"

[[test.results]]
value = "[4]"
*/

DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD n ON person TYPE int;
DEFINE FIELD b ON person COMPUTED n * 2;
DEFINE EVENT ev ON person WHEN $event = 'UPDATE' THEN {
	DEFINE FIELD OVERWRITE b ON person COMPUTED n * 100;
};
CREATE person:1 SET n = 1;
LET $ids = [person:1];
{ UPDATE $ids SET n += 1; RETURN (SELECT VALUE b FROM person); };
{ UPDATE [person:1] SET n += 1; RETURN (SELECT VALUE b FROM person); };
{ UPDATE $ids SET n += 1; RETURN (SELECT VALUE n FROM $ids); };
UPDATE $ids SET n = (SELECT VALUE math::sum(n) FROM $ids GROUP ALL)[0] RETURN VALUE n;
