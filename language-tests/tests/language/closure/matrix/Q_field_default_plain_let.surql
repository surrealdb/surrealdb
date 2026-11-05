/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w DEFAULT $m`. A `LET` of the statement that defines or runs a stored clause is not visible in the clause's own frame, so the clause reads the name as unbound."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: w:1 }]"

[[test.results]]
value = "[NONE]"
*/
LET $m = 8;
DEFINE FIELD f ON w DEFAULT $m;
CREATE w:1;
SELECT VALUE f FROM w;
