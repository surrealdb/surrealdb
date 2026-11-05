/**
[test]
reason = "START/LIMIT over a mock target skip and cap correctly: the mock is one lazy iterable, so the skip applies identically regardless of how many ids the mock names"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: mtgt:2 }]"
*/

CREATE |mtgt:1..=3| RETURN NONE;
SELECT * FROM mtgt START 1 LIMIT 1;
