/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
error = """
Invalid statement: Invalid query: Found a `$this` parameter refering to the document of a group by select statement
Select statements with a group by currently have no defined document to refer to"""

[[test.results]]
error = """
Invalid statement: Invalid query: Found a `$this` parameter refering to the document of a group by select statement
Select statements with a group by currently have no defined document to refer to"""

[[test.results]]
error = """
Invalid statement: Invalid query: Found a `$this` parameter refering to the document of a group by select statement
Select statements with a group by currently have no defined document to refer to"""

*/

{
    CREATE a:1 SET v = 1, g = 1;
    CREATE a:2 SET v = 2, g = 1;
    CREATE a:3 SET v = 3, g = 2;
    CREATE a:4 SET v = 4, g = 2;
    RETURN "OK";
};

SELECT $this,g,math::mean(v) FROM a GROUP BY g;
SELECT { $this },g,math::mean(v) FROM a GROUP BY g;
SELECT g,math::mean($this.v) FROM a GROUP BY g;
