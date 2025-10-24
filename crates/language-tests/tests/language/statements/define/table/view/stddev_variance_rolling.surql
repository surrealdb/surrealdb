/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: { person_stats: 'DEFINE TABLE person_stats TYPE ANY SCHEMALESS AS SELECT count(), age, math::stddev(score) AS score_stddev, math::variance(score) AS score_variance FROM person GROUP BY age PERMISSIONS NONE' } }"

[[test.results]]
value = "[{ age: 25, id: person:one, score: 85 }]"

[[test.results]]
value = "[{ age: 25, count: 1, id: person_stats:[25], score_stddev: 0f, score_variance: 0f }]"

[[test.results]]
value = "[{ age: 25, id: person:two, score: 95 }]"

[[test.results]]
value = "[{ age: 25, count: 2, id: person_stats:[25], score_stddev: 7.071067811865475244008443621dec, score_variance: 50dec }]"

[[test.results]]
value = "[{ age: 25, id: person:three, score: 75 }]"

[[test.results]]
value = "[{ age: 25, count: 3, id: person_stats:[25], score_stddev: 10.000000000000000000000000000dec, score_variance: 100dec }]"

[[test.results]]
value = "[{ age: 30, id: person:four, score: 88 }]"

[[test.results]]
value = "[{ age: 25, count: 3, id: person_stats:[25], score_stddev: 10.000000000000000000000000000dec, score_variance: 100dec }, { age: 30, count: 1, id: person_stats:[30], score_stddev: 0f, score_variance: 0f }]"

[[test.results]]
value = "[{ age: 30, id: person:five, score: 92 }]"

[[test.results]]
value = "[{ age: 25, count: 3, id: person_stats:[25], score_stddev: 10.000000000000000000000000000dec, score_variance: 100dec }, { age: 30, count: 2, id: person_stats:[30], score_stddev: 2.8284271247461900976033774484dec, score_variance: 8dec }]"

[[test.results]]
value = "[{ age: 25, id: person:three, score: 80 }]"

[[test.results]]
value = "[{ age: 25, count: 3, id: person_stats:[25], score_stddev: 7.6376261582597333443134119675dec, score_variance: 58.333333333333333333333333dec }, { age: 30, count: 2, id: person_stats:[30], score_stddev: 2.8284271247461900976033774484dec, score_variance: 8dec }]"
*/

DEFINE TABLE person SCHEMALESS;
DEFINE TABLE person_stats AS
	SELECT
		count(),
		age,
		math::stddev(score) AS score_stddev,
		math::variance(score) AS score_variance
	FROM person
	GROUP BY age
;
INFO FOR TABLE person;
UPSERT person:one SET age = 25, score = 85;
SELECT * FROM person_stats;
UPSERT person:two SET age = 25, score = 95;
SELECT * FROM person_stats;
UPSERT person:three SET age = 25, score = 75;
SELECT * FROM person_stats;
UPSERT person:four SET age = 30, score = 88;
SELECT * FROM person_stats ORDER BY age;
UPSERT person:five SET age = 30, score = 92;
SELECT * FROM person_stats ORDER BY age;
UPSERT person:three SET age = 25, score = 80;
SELECT * FROM person_stats ORDER BY age;
