/**
[test]
reason = "Compound index scan with LIMIT/START must return the correct number of rows and use the expected plan"

[[test.results]]
value = "'OK'"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx_cat_score, access: [1] MoreThan 20, direction: Forward, limit: 2]
''''

[[test.results]]
value = "[{ category: 1, id: item:3, score: 30 }, { category: 1, id: item:4, score: 40 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx_cat_score, access: [1], direction: Forward, limit: 2, offset: 1]
''''

[[test.results]]
value = "[{ category: 1, id: item:2, score: 20 }, { category: 1, id: item:3, score: 30 }]"

[[test.results]]
value = "[{ category: 1, id: item:4, score: 40 }, { category: 1, id: item:5, score: 50 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx_cat_score, access: [1] MoreThan 20, direction: Forward, limit: 2, offset: 1]
''''

[[test.results]]
value = "[{ category: 1, id: item:4, score: 40 }, { category: 1, id: item:5, score: 50 }]"
*/

-- Setup: compound index on (category, score) using numeric category
{
	DEFINE INDEX idx_cat_score ON TABLE item COLUMNS category, score;
	CREATE item:1 SET category = 1, score = 10;
	CREATE item:2 SET category = 1, score = 20;
	CREATE item:3 SET category = 1, score = 30;
	CREATE item:4 SET category = 1, score = 40;
	CREATE item:5 SET category = 1, score = 50;
	CREATE item:6 SET category = 2, score = 60;
	RETURN "OK";
};

-- Verify plan: compound prefix + range with LIMIT uses IndexScan with limit pushed
EXPLAIN SELECT * FROM item WHERE category = 1 AND score > 20 LIMIT 2;

-- Verify results
SELECT * FROM item WHERE category = 1 AND score > 20 LIMIT 2;

-- Verify plan: compound prefix with START+LIMIT uses IndexScan with offset pushed
EXPLAIN SELECT * FROM item WHERE category = 1 LIMIT 2 START 1;

-- Verify results
SELECT * FROM item WHERE category = 1 LIMIT 2 START 1;

-- Large START that skips most results
SELECT * FROM item WHERE category = 1 LIMIT 2 START 3;

-- START beyond result set
SELECT * FROM item WHERE category = 1 LIMIT 2 START 100;

-- Verify plan: compound prefix + range + START + LIMIT all pushed
EXPLAIN SELECT * FROM item WHERE category = 1 AND score > 20 LIMIT 2 START 1;

-- Verify results: compound range + START + LIMIT returns correct rows
SELECT * FROM item WHERE category = 1 AND score > 20 LIMIT 2 START 1;
