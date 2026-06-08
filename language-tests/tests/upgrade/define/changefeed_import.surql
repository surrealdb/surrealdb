/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: cf_src:1, v: 1 }]"

[[test.results]]
value = "[{ id: cf_src:1, v: 2 }]"

[[test.results]]
value = "[{ id: cf_src:1, v: 2 }]"

*/
DEFINE TABLE cf_src SCHEMALESS CHANGEFEED 1h;
CREATE cf_src:1 SET v = 1;
UPDATE cf_src:1 SET v = 2;
DELETE cf_src:1 RETURN BEFORE;
