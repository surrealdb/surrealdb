/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ agr: 1, day: 1 }]"

[[test.results]]
value = "[{ agr: 1, day: 1 }]"

*/
UPSERT wallet:1 CONTENT { value: 20.0dec, day: 1 } RETURN NONE;
UPSERT wallet:2 CONTENT { value: 5.0dec, day: 1 } RETURN NONE;
DEFINE TABLE wallet_agr AS SELECT count() as agr, day FROM wallet WHERE value >= 5 GROUP BY day;
UPSERT wallet:2 SET value = 3.0dec RETURN NONE;
SELECT count() as agr, day FROM wallet WHERE value >= 5 GROUP BY day;
SELECT agr, day FROM wallet_agr;
