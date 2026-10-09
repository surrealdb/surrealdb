/**
[test]
reason = "Import: a table readable by record users, with a field whose SELECT permission hides it from non-owners, plus an index over that field. Used to verify ORDER BY on the restricted field does not leak the hidden values' ordering."
run = false
*/

-- The table itself is fully selectable by record users, but the `code` field is
-- only visible to its owner. An index covers `code` so the planner could be
-- tempted to satisfy `ORDER BY code` via an index walk (true value order).
DEFINE TABLE secret SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD code ON secret TYPE string PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD owner ON secret TYPE record;
DEFINE INDEX code_idx ON secret FIELDS code;

-- Rows owned by the victim. Hidden-value order (a < c < m) deliberately differs
-- from record-id order (secret:1 < secret:2 < secret:3).
CREATE secret:1 SET code = 'm', owner = users:victim;
CREATE secret:2 SET code = 'a', owner = users:victim;
CREATE secret:3 SET code = 'c', owner = users:victim;
