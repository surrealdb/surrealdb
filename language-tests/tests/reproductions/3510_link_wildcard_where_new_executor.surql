/**
[test]
reason = "Ensure link.* works correctly with and without WHERE clause (new executor)"
issue = 3510
*/

CREATE a:1;
CREATE b:1 SET link = a:1, num = 1;
SELECT link.* FROM b;
SELECT link.* FROM b WHERE num = 1;
