/**
[test]

# 0: Setup
[[test.results]]
value = "'OK'"

# 1: Array literal source - simple values
[[test.results]]
value = "[1, 2, 3]"

# 2: Array literal source with projection
[[test.results]]
value = "[1, 2, 3]"

# 3: Array literal source with filter
[[test.results]]
value = "[2, 3]"

# 4: Null source - should return empty array
[[test.results]]
value = "[]"

# 5: None source - should return empty array
[[test.results]]
value = "[]"

# 6: String source - should return single value
[[test.results]]
value = "['bob']"

# 7: Object source - should return single value
[[test.results]]
value = "[{ name: 'test' }]"

# 8: Subquery source
[[test.results]]
value = "[{ id: person:1, name: 'John' }, { id: person:2, name: 'Jane' }]"

# 9: Subquery source with outer projection
[[test.results]]
value = "[{ name: 'John' }, { name: 'Jane' }]"

# 10: Subquery source with outer filter
[[test.results]]
value = "[{ id: person:2, name: 'Jane' }]"

# 11: Parameter - array value (LET returns NONE)
[[test.results]]
value = "NONE"

# 11: Parameter - array value
[[test.results]]
value = "[10, 20, 30]"

# 12: Parameter - array value with filter (LET returns NONE)
[[test.results]]
value = "NONE"

# 12: Parameter - array value with filter
[[test.results]]
value = "[20, 30]"

# 13: Parameter - table name value (LET returns NONE)
[[test.results]]
value = "NONE"

# 13: Parameter - table name value
[[test.results]]
value = "[{ id: person:1, name: 'John' }, { id: person:2, name: 'Jane' }]"

# 14: Nested subquery
[[test.results]]
value = "[{ name: 'John' }, { name: 'Jane' }]"

# 15: Array of objects
[[test.results]]
value = "[{ x: 1 }, { x: 2 }]"
*/

# 0: Setup
{
    CREATE person:1 SET name = 'John';
    CREATE person:2 SET name = 'Jane';
    RETURN "OK";
};

# 1: Array literal source - simple values
SELECT $this FROM [1, 2, 3];

# 2: Array literal source with projection
SELECT VALUE $this FROM [1, 2, 3];

# 3: Array literal source with filter
SELECT VALUE $this FROM [1, 2, 3] WHERE $this > 1;

# 4: Null source - should return empty array
SELECT $this FROM NULL;

# 5: None source - should return empty array
SELECT $this FROM NONE;

# 6: String source - should return single value
SELECT VALUE $this FROM "bob";

# 7: Object source - should return single value
SELECT VALUE $this FROM { name: 'test' };

# 8: Subquery source
SELECT * FROM (SELECT * FROM person);

# 9: Subquery source with outer projection
SELECT name FROM (SELECT * FROM person);

# 10: Subquery source with outer filter
SELECT * FROM (SELECT * FROM person) WHERE name = 'Jane';

# 11: Parameter - array value
LET $arr = [10, 20, 30];
SELECT VALUE $this FROM $arr;

# 12: Parameter - array value with filter
LET $arr2 = [10, 20, 30];
SELECT VALUE $this FROM $arr2 WHERE $this > 10;

# 13: Parameter - table name value
LET $tbl = type::table("person");
SELECT * FROM $tbl;

# 14: Nested subquery
SELECT name FROM (SELECT name FROM (SELECT * FROM person));

# 15: Array of objects
SELECT VALUE $this FROM [{ x: 1 }, { x: 2 }];
