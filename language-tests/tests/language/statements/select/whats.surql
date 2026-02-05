/**
[test]

# 0
[[test.results]]
value = "'OK'"

# 1
[[test.results]]
value = "1"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
value = "1"

# 4
[[test.results]]
value = "[1]"

# 5
[[test.results]]
value = "[1]"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
value = "[{ id: person:1, name: 'John' }, { id: person:2, name: 'Jane' }]"

# 8
[[test.results]]
value = "[1, 2, 3]"

# 9
[[test.results]]
value = "[{ id: person:1, name: 'John' }, { id: person:2, name: 'Jane' }]"

# 10
[[test.results]]
value = "[{ id: person:2, name: 'Jane' }]"

# 11
[[test.results]]
value = "[]"

# 12
[[test.results]]
value = "[{ id: person:1, name: 'John' }, { id: person:2, name: 'Jane' }]"

# 13
[[test.results]]
value = "[{ id: person:1, name: 'John' }, { id: person:2, name: 'Jane' }]"

# 14
[[test.results]]
value = "[{ id: person:1, name: 'John' }, { id: person:2, name: 'Jane' }]"

# 15
[[test.results]]
value = "[{ id: person:1, name: 'John' }]"

# 16
[[test.results]]
value = "[{ id: person:1}]"

# 17
[[test.results]]
value = "[2, 3]"

# 18
[[test.results]]
value = "['bob']"

# 19
[[test.results]]
value = "[NONE]"

# 20
[[test.results]]
value = "[NULL]"

# 21
[[test.results]]
value = "NONE"

# 22
[[test.results]]
value = "NULL"
*/

# 0: Setup
{
    CREATE a:1 SET enabled = false;
    CREATE a:2 SET enabled = true;
    CREATE person:1 SET name = 'John';
    CREATE person:2 SET name = 'Jane';
    RELATE person:1->knows->person:2;
    CREATE test:1 SET id = 1;
    RETURN "OK";
};

# 1
SELECT * FROM ONLY 1;
# 2
SELECT * FROM ONLY NONE;
# 3
SELECT VALUE 1 FROM ONLY "";
# 4
SELECT VALUE 1 FROM "";
# 5
SELECT VALUE 1 FROM "asdf";
# 6
SELECT VALUE enabled FROM ONLY { id: a:1, enalbed: false };
# 7: Stream data from subquery which streams data from person table
SELECT * FROM (SELECT VALUE id FROM person);
# 8
SELECT * FROM [1,2,3];
# 9
SELECT * FROM person:1..4;
# 10
SELECT * FROM person:1->knows->person;
# 11
SELECT * FROM |person:10..12|;
# 12
SELECT * FROM [person:1, person:2];
# 13
[person:1, person:2].*;
# 14: Stream data from person table
SELECT * FROM person;
# 15
SELECT * FROM person:1;
# 16
SELECT id FROM [person:1] WHERE name = 'John';
# 17
SELECT $this FROM [1, 2, 3] WHERE $this > 1;
# 18
SELECT $this FROM "bob";
# 19
SELECT $this FROM NONE;
# 20
SELECT $this FROM NULL;
# 21
SELECT $this FROM ONLY NONE;
# 22
SELECT $this FROM ONLY NULL;
