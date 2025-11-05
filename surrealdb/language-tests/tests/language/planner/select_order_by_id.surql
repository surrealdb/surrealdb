/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: p:9 }]"

*/

CREATE |p:1..10| RETURN NONE;

SELECT id FROM p ORDER BY id DESC LIMIT 1;
