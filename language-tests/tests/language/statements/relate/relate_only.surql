/**
[test]

[[test.results]]
value = "{ id: edge:1, in: a:1, out: a:2 }"

[[test.results]]
value = "{ id: edge:1, in: a:1, name: 'A', out: a:2 }"

*/

RELATE ONLY a:1->edge:1->a:2;
RELATE ONLY a:1->edge:1->a:2 SET name = 'A';
