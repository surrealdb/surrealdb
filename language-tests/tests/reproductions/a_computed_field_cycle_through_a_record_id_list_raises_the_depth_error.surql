/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A COMPUTED field whose body selects from a list of record ids (`FROM $f`) derives, on each record the list names, its own COMPUTED field, which selects from the list again. Reading such a record spends a unit of the computation-depth budget, as a field read through a link does, so a cycle of links through the field ends in the computation-depth error whether the statement reads one record, every record or a list of ids, and never exhausts the stack. A chain of a hundred links stays within the budget and is read in full.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"
*/

DEFINE FIELD friends ON crowd TYPE array<record<crowd>> DEFAULT [];
DEFINE FIELD docs ON crowd COMPUTED { LET $f = friends; RETURN (SELECT * FROM $f); };
CREATE crowd:1 SET friends = [crowd:2, crowd:2, crowd:2] RETURN NONE;
CREATE crowd:2 SET friends = [crowd:1, crowd:1, crowd:1] RETURN NONE;
SELECT id, docs FROM crowd:1;
SELECT id, docs FROM crowd;
LET $ids = [crowd:1, crowd:2];
SELECT id, docs FROM $ids;

DEFINE FIELD next ON hop TYPE option<record<hop>>;
DEFINE FIELD docs ON hop COMPUTED { LET $n = next; RETURN (SELECT * FROM $n); };
FOR $i IN 1..=100 { CREATE type::record('hop', $i) SET next = IF $i < 100 { type::record('hop', $i + 1) } ELSE { NONE } RETURN NONE; };
RETURN count(SELECT id, docs FROM hop:1);
