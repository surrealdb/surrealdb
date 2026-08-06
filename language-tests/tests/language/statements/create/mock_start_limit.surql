/**
[test]
reason = "START/LIMIT over a mock target still skip and cap correctly now that the mock is one lazy iterable instead of one entry per id — the per-record skip (Collector::collect, which checks Iterator::skippable() before even preparing the document) applies identically regardless of how many entries stage the scan"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: mtgt:2 }]"
*/

CREATE |mtgt:1..=3| RETURN NONE;
SELECT * FROM mtgt START 1 LIMIT 1;
