/**
[test]
reason = """A FROM $param source is resolved from the binding in effect where the SELECT \
runs: a LET for the same name inside the statement's own block shadows the value the \
enclosing context holds at plan time, for a table value and for a record id, and a FOR \
loop variable is bound per iteration. A table-valued parameter bound at runtime scans \
the table, and a NONE-valued one yields no rows."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, kind: 'person' }]"

[[test.results]]
value = "[{ id: company:1, kind: 'company' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['company']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['company']"

[[test.results]]
value = "['person']"

[[test.results]]
value = "['person']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['company']"

[[test.results]]
value = "[]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE company SCHEMALESS;
CREATE person:1 SET kind = 'person';
CREATE company:1 SET kind = 'company';

LET $tb = type::table('person');

RETURN { LET $tb = type::table('company'); SELECT VALUE kind FROM $tb };

LET $r = person:1;

RETURN { LET $r = company:1; SELECT VALUE kind FROM $r };

-- The enclosing binding is untouched by the inner LET.
SELECT VALUE kind FROM $tb;

SELECT VALUE kind FROM $r;

-- A FOR loop variable used as a source is bound per iteration.
LET $loop = type::table('person');

RETURN { FOR $loop IN [type::table('company')] { RETURN SELECT VALUE kind FROM $loop }; RETURN 'unreached' };

-- A parameter bound to NONE where the SELECT runs is no rows, as a literal NONE source is.
RETURN { LET $n = NONE; SELECT * FROM $n };
