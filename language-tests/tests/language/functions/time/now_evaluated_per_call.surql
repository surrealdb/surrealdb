/**
[test]
reason = "`time::now()` is evaluated per call in a projection and in a data clause, so a statement over many rows reads the clock once per row. Only a condition folds it to one value for the statement, which cannot be told apart from per-row evaluation and is what lets an index range answer it. Two rows can still read the same instant, so each check asserts more than one distinct value, which folding to one value per statement can never produce."

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "50"

*/
RETURN count(array::distinct((CREATE |clk:50| SET at = time::now() RETURN VALUE at))) > 1;
RETURN count(array::distinct((SELECT VALUE time::now() FROM clk))) > 1;
RETURN count(array::distinct((UPDATE clk SET at = time::now() RETURN VALUE at))) > 1;
RETURN count(array::distinct((INSERT INTO ins (SELECT VALUE { at: time::now() } FROM clk) RETURN VALUE at))) > 1;
RETURN count(SELECT * FROM clk WHERE at <= time::now());
