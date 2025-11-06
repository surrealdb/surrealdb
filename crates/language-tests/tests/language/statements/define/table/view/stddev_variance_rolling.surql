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
value = "[{ age: 25, count: 2, id: person_stats:[25], score_stddev: 7.0710678118654755f, score_variance: 50f }]"

[[test.results]]
value = "[{ age: 25, id: person:three, score: 75 }]"

[[test.results]]
value = "[{ age: 25, count: 3, id: person_stats:[25], score_stddev: 10f, score_variance: 100f }]"

[[test.results]]
value = "[{ age: 30, id: person:four, score: 88 }]"

[[test.results]]
value = "[{ age: 25, count: 3, id: person_stats:[25], score_stddev: 10f, score_variance: 100f }, { age: 30, count: 1, id: person_stats:[30], score_stddev: 0f, score_variance: 0f }]"

[[test.results]]
value = "[{ age: 30, id: person:five, score: 92 }]"

[[test.results]]
value = "[{ age: 25, count: 3, id: person_stats:[25], score_stddev: 10f, score_variance: 100f }, { age: 30, count: 2, id: person_stats:[30], score_stddev: 2.8284271247461903f, score_variance: 8f }]"

[[test.results]]
value = "[{ age: 25, id: person:three, score: 80 }]"

[[test.results]]
value = "[{ age: 25, count: 3, id: person_stats:[25], score_stddev: 7.637626158259654f, score_variance: 58.33333333333212f }, { age: 30, count: 2, id: person_stats:[30], score_stddev: 2.8284271247461903f, score_variance: 8f }]"
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
