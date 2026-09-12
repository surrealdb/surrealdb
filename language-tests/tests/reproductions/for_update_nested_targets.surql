/**
[test]
reason = """A SELECT ... FOR UPDATE over a target that is not a specific record is refused \
wherever the statement appears — at the top level, nested in RETURN or LET, or as a \
subquery source — and whether the target is written as a literal value, a table, or a \
parameter that resolves to a value or an array, at plan time or when the statement runs. \
A specific record id is the one target the clause locks."""

[[test.results]]
value = "[{ id: person:1 }]"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
value = "[{ id: person:1 }]"
*/

CREATE person:1;

-- A table, nested.
RETURN (SELECT * FROM person FOR UPDATE);
LET $p = SELECT * FROM person FOR UPDATE;

-- A parameter that resolves to a value, and to an array of record ids.
LET $p = [1];
SELECT * FROM $p FOR UPDATE;
LET $q = [person:1];
SELECT * FROM $q FOR UPDATE;

-- A parameter bound where the statement runs.
RETURN { LET $r = [1]; SELECT * FROM $r FOR UPDATE };

-- Literal values, nested.
RETURN (SELECT * FROM [1] FOR UPDATE);
SELECT * FROM (SELECT * FROM [1] FOR UPDATE);
RETURN (SELECT * FROM 'x' FOR UPDATE);

-- A record id is the one target the clause locks.
RETURN (SELECT * FROM person:1 FOR UPDATE);
