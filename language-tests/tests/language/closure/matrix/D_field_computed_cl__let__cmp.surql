/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w COMPUTED array::filter([1, 5, 10, 15, 20], |$e| $e >= $m)`. A `LET` of the statement that defines or runs a stored clause is not visible in the clause's own frame, so the clause reads the name as unbound."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: [1, 5, 10, 15, 20], id: w:1 }]"

[[test.results]]
value = "[[1, 5, 10, 15, 20]]"
*/
LET $m = 8;
DEFINE FIELD f ON w COMPUTED array::filter([1, 5, 10, 15, 20], |$e| $e >= $m);
CREATE w:1;
SELECT VALUE f FROM w;
