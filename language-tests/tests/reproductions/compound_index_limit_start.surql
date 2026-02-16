/**
[test]
reason = "Compound index scan with LIMIT/START must return the correct number of rows"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ category: 'A', id: item:3, score: 30 }, { category: 'A', id: item:4, score: 40 }]"

[[test.results]]
value = "[{ category: 'A', id: item:3, score: 30 }, { category: 'A', id: item:4, score: 40 }, { category: 'A', id: item:5, score: 50 }]"

[[test.results]]
value = "[{ category: 'A', id: item:2, score: 20 }, { category: 'A', id: item:3, score: 30 }]"

[[test.results]]
value = "[{ category: 'A', id: item:1, score: 10 }]"

[[test.results]]
value = "[{ category: 'A', id: item:4, score: 40 }, { category: 'A', id: item:5, score: 50 }]"

[[test.results]]
value = "[]"
*/

-- Setup: compound index on (category, score)
{
	DEFINE INDEX idx_cat_score ON TABLE item COLUMNS category, score;
	CREATE item:1 SET category = 'A', score = 10;
	CREATE item:2 SET category = 'A', score = 20;
	CREATE item:3 SET category = 'A', score = 30;
	CREATE item:4 SET category = 'A', score = 40;
	CREATE item:5 SET category = 'A', score = 50;
	CREATE item:6 SET category = 'B', score = 60;
	RETURN "OK";
};

-- Basic LIMIT with compound index equality prefix + range
SELECT * FROM item WHERE category = 'A' AND score > 20 LIMIT 2;

-- LIMIT larger than result set
SELECT * FROM item WHERE category = 'A' AND score > 20 LIMIT 10;

-- START + LIMIT with compound index
SELECT * FROM item WHERE category = 'A' LIMIT 2 START 1;

-- LIMIT 1 (minimal)
SELECT * FROM item WHERE category = 'A' LIMIT 1;

-- Large START that skips most results
SELECT * FROM item WHERE category = 'A' LIMIT 2 START 3;

-- START beyond result set
SELECT * FROM item WHERE category = 'A' LIMIT 2 START 100;
