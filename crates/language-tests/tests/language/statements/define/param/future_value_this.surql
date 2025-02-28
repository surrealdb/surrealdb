/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: { parent: NONE, this: { a: { parent: NONE, this: 'this' } } } }"

*/

DEFINE PARAM $a VALUE <future> { { this: $this, parent: $parent } };

SELECT $a, * FROM ONLY (SELECT $a FROM ONLY "this");
