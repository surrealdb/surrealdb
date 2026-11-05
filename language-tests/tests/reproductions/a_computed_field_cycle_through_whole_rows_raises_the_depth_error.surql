/**
[test]
reason = "A COMPUTED field whose body reads every field of its own table's records derives itself on each record it reads, which derives it again: the cycle spends the computation-depth budget and ends in the computation-depth error rather than exhausting the stack. A read that names no computed field derives none and ends."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[ring:1, ring:2]"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

*/

DEFINE TABLE ring SCHEMALESS;
DEFINE FIELD members ON ring COMPUTED count(SELECT * FROM ring);

CREATE ring:1 RETURN NONE;
CREATE ring:2 RETURN NONE;

SELECT VALUE id FROM ring;
SELECT * FROM ring;
