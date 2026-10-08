/**
[test]
reason = "Setup: one record and one edge to update, so both halves of every dispatcher — the create attempt and the update — can be reached with the same data clause."
run = false
*/

DEFINE TABLE t SCHEMALESS;
DEFINE TABLE e TYPE RELATION IN t OUT t SCHEMALESS;

CREATE t:1 SET n = 1;
RELATE t:1->e:x1->t:1;
