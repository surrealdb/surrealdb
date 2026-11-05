/**
[env]
namespace = true
database = true

[test]
reason = """
Rows that are record ids, from a statement in `FROM` that selects `VALUE id`,
read the records they name: `SELECT *` returns the records, and so does `*`
beside other fields, whose fields are written onto the record. A sort over such
rows hands the record ids on, and `SELECT *` over the sorted rows still returns
the records. A `SPLIT` on an alias splits the projected rows when a `FETCH`
follows the projection.
"""

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ age: 25, city: city:2, id: user:1, name: 'Bob', x: 1 }, { age: 30, city: city:1, id: user:2, name: 'Alice', x: 1 }]"

[[test.results]]
value = "[{ age: 25, city: city:2, id: user:1, name: 'Bob' }, { age: 30, city: city:1, id: user:2, name: 'Alice' }]"

[[test.results]]
value = "[{ age: 25, city: city:2, id: user:1, name: 'Bob' }, { age: 30, city: city:1, id: user:2, name: 'Alice' }]"

[[test.results]]
value = "[{ c: { country: 'USA', id: city:1, name: 'San Francisco' }, t: 1 }, { c: { country: 'USA', id: city:1, name: 'San Francisco' }, t: 2 }, { c: { country: 'USA', id: city:2, name: 'New York' }, t: 3 }]"

[[test.results]]
value = "[{ c: { country: 'USA', id: city:1, name: 'San Francisco' }, tags: 1 }, { c: { country: 'USA', id: city:1, name: 'San Francisco' }, tags: 2 }, { c: { country: 'USA', id: city:2, name: 'New York' }, tags: 3 }]"
*/

{
	CREATE user:1 SET name = 'Bob', age = 25, city = city:2;
	CREATE user:2 SET name = 'Alice', age = 30, city = city:1;
	CREATE city:1 SET name = 'San Francisco', country = 'USA';
	CREATE city:2 SET name = 'New York', country = 'USA';
	CREATE s:1 SET tags = [1, 2], c = city:1;
	CREATE s:2 SET tags = [3], c = city:2;
	RETURN "OK";
};

SELECT *, 1 AS x FROM (SELECT VALUE id FROM user);
SELECT * FROM (SELECT VALUE id FROM user) ORDER BY age;
SELECT * FROM (SELECT VALUE id FROM user) ORDER BY id;
SELECT tags AS t, c FROM s SPLIT t FETCH c;
SELECT tags, c FROM s SPLIT tags FETCH c;
