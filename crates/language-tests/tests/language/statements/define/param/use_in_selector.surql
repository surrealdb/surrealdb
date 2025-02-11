/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 1, b: 2 }"

*/
let $a = 1;
DEFINE PARAM $b VALUE 2;

SELECT $a,$b FROM ONLY "";
