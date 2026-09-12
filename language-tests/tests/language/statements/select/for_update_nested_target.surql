/**
[test]
reason = "A nested SELECT as a FOR UPDATE target is rejected with the targets error wherever the statement appears: at the top level, in scalar position, and as a subquery source"

[[test.results]]
value = "[{ id: person:1 }]"

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
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

*/

CREATE person:1;

-- At the top level, and with a record alongside the nested statement.
SELECT * FROM (SELECT * FROM person:1) FOR UPDATE;
SELECT * FROM person:1, (SELECT * FROM person:1) FOR UPDATE;

-- In scalar position, where only the plan carries the clause.
RETURN (SELECT * FROM (SELECT * FROM person:1) FOR UPDATE);
LET $locked = (SELECT * FROM (SELECT * FROM person:1) FOR UPDATE);
SELECT * FROM person:1 WHERE (SELECT * FROM (SELECT * FROM person:1) FOR UPDATE);

-- As a subquery source.
SELECT * FROM (SELECT * FROM (SELECT * FROM person:1) FOR UPDATE);
