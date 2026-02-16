/**
[test]
reason = "Large START offsets must correctly skip rows in table and index scans"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: item:8, score: 80 }, { id: item:9, score: 90 }, { id: item:10, score: 100 }]"

[[test.results]]
value = "[{ id: item:10, score: 100 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: item:6, score: 60 }, { id: item:7, score: 70 }]"

[[test.results]]
value = "[]"
*/

-- Setup: 10 rows with an index
{
	DEFINE INDEX idx_score ON TABLE item COLUMNS score;
	CREATE item:1 SET score = 10;
	CREATE item:2 SET score = 20;
	CREATE item:3 SET score = 30;
	CREATE item:4 SET score = 40;
	CREATE item:5 SET score = 50;
	CREATE item:6 SET score = 60;
	CREATE item:7 SET score = 70;
	CREATE item:8 SET score = 80;
	CREATE item:9 SET score = 90;
	CREATE item:10 SET score = 100;
	RETURN "OK";
};

-- Table scan: START 7 skips first 7 rows, LIMIT 3 returns remaining 3
SELECT * FROM item LIMIT 3 START 7;

-- Table scan: START 9 skips first 9 rows, LIMIT 5 returns remaining 1
SELECT * FROM item LIMIT 5 START 9;

-- Table scan: START beyond row count
SELECT * FROM item LIMIT 5 START 100;

-- Index scan: START + LIMIT through index
SELECT * FROM item WHERE score > 0 LIMIT 2 START 5;

-- Index scan: START beyond matching rows
SELECT * FROM item WHERE score > 0 LIMIT 2 START 100;
