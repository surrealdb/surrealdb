/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ age: 25, count: 3, id: person_stats:[25], score_stddev: 10f, score_variance: 100f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 25, count: 2, id: person_stats:[25], score_stddev: 7.0710678118654755f, score_variance: 50f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 25, count: 1, id: person_stats:[25], score_stddev: 0f, score_variance: 0f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 30, id: person:d1, score: 100 }]"

[[test.results]]
value = "[{ age: 30, id: person:d2, score: 110 }]"

[[test.results]]
value = "[{ age: 30, id: person:d3, score: 120 }]"

[[test.results]]
value = "[{ age: 30, id: person:d4, score: 130 }]"

[[test.results]]
value = "[{ age: 30, id: person:d5, score: 140 }]"

[[test.results]]
value = "[{ age: 30, count: 5, id: person_stats:[30], score_stddev: 15.811388300841896f, score_variance: 250f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 30, count: 4, id: person_stats:[30], score_stddev: 17.07825127659933f, score_variance: 291.6666666666667f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 30, count: 3, id: person_stats:[30], score_stddev: 20f, score_variance: 400f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 30, count: 2, id: person_stats:[30], score_stddev: 14.142135623730951f, score_variance: 200f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ age: 40, id: person:mover, score: 200 }]"

[[test.results]]
value = "[{ age: 40, count: 1, id: person_stats:[40], score_stddev: 0f, score_variance: 0f }]"

[[test.results]]
value = "[{ age: 50, id: person:mover, score: 200 }]"

[[test.results]]
value = "[{ age: 50, count: 1, id: person_stats:[50], score_stddev: 0f, score_variance: 0f }]"

[[test.results]]
value = "[{ age: 60, id: person:updater, score: 300 }]"

[[test.results]]
value = "[{ age: 60, id: person:updater2, score: 310 }]"

[[test.results]]
value = "[{ age: 60, count: 2, id: person_stats:[60], score_stddev: 7.0710678118654755f, score_variance: 50f }]"

[[test.results]]
value = "[{ age: 60, id: person:updater, score: 350 }]"

[[test.results]]
value = "[{ age: 60, count: 2, id: person_stats:[60], score_stddev: 28.284271247461902f, score_variance: 800f }]"
*/
{
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

	-- Setup: Add multiple records to establish statistics
	UPSERT person:alice SET age = 25, score = 80;
	UPSERT person:bob SET age = 25, score = 90;
	UPSERT person:charlie SET age = 25, score = 70;
	"OK";
};

SELECT * FROM person_stats WHERE age = 25;

-- Test 1: Delete one record from middle of dataset
DELETE person:bob;
SELECT * FROM person_stats WHERE age = 25;

-- Test 2: Delete another record
DELETE person:charlie;
SELECT * FROM person_stats WHERE age = 25;

-- Test 3: Delete last record (group should be removed)
DELETE person:alice;
SELECT * FROM person_stats WHERE age = 25;

-- Test 4: Multi-record group with sequential deletions
UPSERT person:d1 SET age = 30, score = 100;
UPSERT person:d2 SET age = 30, score = 110;
UPSERT person:d3 SET age = 30, score = 120;
UPSERT person:d4 SET age = 30, score = 130;
UPSERT person:d5 SET age = 30, score = 140;
SELECT * FROM person_stats WHERE age = 30;

-- Delete multiple records to test rolling updates
DELETE person:d2;
SELECT * FROM person_stats WHERE age = 30;
DELETE person:d4;
SELECT * FROM person_stats WHERE age = 30;
DELETE person:d1;
SELECT * FROM person_stats WHERE age = 30;

-- Test 5: Mass deletion
DELETE person WHERE age = 30;
SELECT * FROM person_stats WHERE age = 30;

-- Test 6: Group membership changes (record moves between groups)
UPSERT person:mover SET age = 40, score = 200;
SELECT * FROM person_stats WHERE age = 40;
UPDATE person:mover SET age = 50;  -- Move to different group
SELECT * FROM person_stats ORDER BY age;

-- Test 7: Update that changes value but not group
UPSERT person:updater SET age = 60, score = 300;
UPSERT person:updater2 SET age = 60, score = 310;
SELECT * FROM person_stats WHERE age = 60;
UPDATE person:updater SET score = 350;  -- Change score, same group
SELECT * FROM person_stats WHERE age = 60;
