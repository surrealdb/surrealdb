/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A COMPUTED field whose body `FETCH`es the links of its record derives, on each record the fetch reads, its own COMPUTED field, which fetches again. Each such derivation spends a unit of the computation-depth budget, so a cycle of links through the field ends in the computation-depth error and never exhausts the stack.
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
*/

DEFINE FIELD friends ON wide TYPE array<record<wide>> DEFAULT [];
DEFINE FIELD docs ON wide COMPUTED (SELECT VALUE friends FROM ONLY $this FETCH friends);
CREATE wide:1 SET friends = [wide:2] RETURN NONE;
CREATE wide:2 SET friends = [wide:1] RETURN NONE;
SELECT id, docs FROM wide:1;
