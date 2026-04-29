/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "After a unique-index violation inside BEGIN, COMMIT must not leave duplicate values (issue #7207 SDK pattern in SurrealQL)"
issue = 7207

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u7207:r1, name: 'Alex1' }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `u7207_name` already contains 'Alex1', with record `u7207:r1`"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "[{ c: 1, name: 'Alex1' }]"

*/

DEFINE TABLE OVERWRITE u7207 TYPE NORMAL SCHEMAFULL PERMISSIONS NONE;
DEFINE FIELD OVERWRITE name ON u7207 TYPE string PERMISSIONS FULL;
DEFINE INDEX OVERWRITE u7207_name ON u7207 FIELDS name UNIQUE;

-- Seed: one row with unique name (matches issue: first create succeeds)
CREATE u7207:r1 SET name = 'Alex1';

-- Transaction attempts a second row with the same unique `name` (should fail the statement)
BEGIN;
CREATE u7207:r2 SET name = 'Alex1';
COMMIT;

-- Invariant: at most one row for name 'Alex1'
SELECT name, count() AS c FROM u7207 GROUP BY name;
