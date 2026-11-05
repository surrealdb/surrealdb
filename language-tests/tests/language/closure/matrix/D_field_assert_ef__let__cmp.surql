/**
[test]
reason = "The body is written in `DEFINE FIELD f ON w ASSERT [1, 5, 10, 15, 20][WHERE $this >= $m] != []`. Each name and row it reads takes the value of the frame where the body is written."

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
DEFINE FIELD f ON w ASSERT [1, 5, 10, 15, 20][WHERE $this >= $m] != [];
CREATE w:1;
SELECT VALUE f FROM w;
