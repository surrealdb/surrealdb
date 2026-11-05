/**
[test]
reason = "Record ids whose table name is a keyword (e.g. sleep) are valid RELATE targets"

[[test.results]]
value = "{ id: edge:1, in: a:1, out: sleep:b }"

[[test.results]]
value = "{ id: sleep:1, in: sleep:a, out: sleep:b }"
*/

RELATE ONLY a:1->edge:1->sleep:b;
RELATE ONLY sleep:a->sleep:1->sleep:b;
