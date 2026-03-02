/**
[test]
reason = "Test WHERE clause with boolean field comparisons (new executor)"
*/

CREATE test:1 SET active = false;
CREATE test:2 SET active = false;
CREATE test:3 SET active = true;
SELECT * FROM test WHERE active = false;
SELECT * FROM test WHERE active != true;
SELECT * FROM test WHERE active = true;
