/**
[env]
imports = ["language/trace/graph_import.surql"]
trace = ["relations", "variables"]

[test]
reason = "A trace stands or falls with its statement: a failed statement, and every statement of a cancelled or failed block, sends none"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
error = "The query was not executed due to a cancelled transaction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "An error occurred: no"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
trace = "{ variables: { z: 1 } }"

[[test.results]]
value = "[[person:b, person:c]]"
trace = "{ relations: [[person:a, knows:ab, person:b], [person:a, knows:ac, person:c]] }"

[[test.results]]
value = "NONE"
*/

-- 0: a statement that fails sends no trace
SELECT VALUE ->knows->person FROM person:a WHERE <int> 'x';

-- 1..4: a cancelled block retracts every statement's trace
BEGIN;
LET $x = person:a->knows->person;
SELECT VALUE ->knows->person FROM person:a;
CANCEL;

-- 5..9: a block that fails retracts every statement's trace
BEGIN;
LET $y = 1;
SELECT VALUE ->knows->person FROM person:a;
THROW 'no';
COMMIT;

-- 10..13: a committed block keeps every statement's trace
BEGIN;
LET $z = 1;
SELECT VALUE ->knows->person FROM person:a;
COMMIT;
