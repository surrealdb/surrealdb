/**
[test]

[[test.results]]
value = "[{ g: 1, id: a:1, v: 1 }]"

[[test.results]]
value = "[{ g: 1, id: a:2, v: 2 }]"

[[test.results]]
value = "[{ g: 2, id: a:3, v: 3 }]"

[[test.results]]
value = "[{ g: 2, id: a:4, v: 4 }]"

[[test.results]]
error = """
Invalid query: Found a `$this` parameter refering to the document of a group by select statement
Select statements with a group by currently have no defined document to refer to"""

[[test.results]]
error = """
Invalid query: Found a `$this` parameter refering to the document of a group by select statement
Select statements with a group by currently have no defined document to refer to"""

*/

create a:1 set v = 1, g = 1;
create a:2 set v = 2, g = 1;
create a:3 set v = 3, g = 2;
create a:4 set v = 4, g = 2;

SELECT $this,g,math::mean(v) FROM a GROUP BY g;
SELECT g,math::mean($this.v) FROM a GROUP BY g;

