/**
[test]
reason = "The body is written in `DEFINE FIELD s ON w ASSERT array::all($value, |$e| $e >= 6)`. The statement raises the error the closure or the clause it stands in gives."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found [1, 5, 10, 15, 20] for field `s`, with record `w:1`, but field must conform to: array::all($value, |$e: any| $e >= 6)"

[[test.results]]
value = "[]"
*/
DEFINE FIELD s ON w ASSERT array::all($value, |$e| $e >= 6);
CREATE w:1 SET s = [1, 5, 10, 15, 20];
SELECT VALUE s FROM w;
