/**
[test]
reason = "Every clock read answers one instant per statement (register row #7), whatever the position: a projection, a data clause and an insert source over many rows each read the clock once for the statement, so the rows of one statement all hold the same value, and a later statement reads a later instant, which every record the earlier statements stamped lies at or before."

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
RETURN count(array::distinct((CREATE |clk:50| SET at = time::now() RETURN VALUE at))) = 1;
RETURN count(array::distinct((SELECT VALUE time::now() FROM clk))) = 1;
RETURN count(array::distinct((UPDATE clk SET at = time::now() RETURN VALUE at))) = 1;
RETURN count(array::distinct((INSERT INTO ins (SELECT VALUE { at: time::now() } FROM clk) RETURN VALUE at))) = 1;
RETURN count(SELECT * FROM clk WHERE at <= time::now());
