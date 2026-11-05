/**
[env]
namespace = true
database = true

[test]
reason = """
An index over an array field lists each element, so an equality looked up on
it finds a record whose array holds the value. Whether the record is returned
is the predicate's to answer, and `[3, 4] = 3` is false. The answer must not
depend on whether another conjunct stands beside the equality.
"""

[[test.results]]
value = "[{ age: 4, id: t:3, name: 'c' }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ id: t:1 }]"

[[test.results]]
value = "[{ id: t:1 }]"
*/

{
	DEFINE INDEX by_age ON t FIELDS age;
	CREATE t:1 SET age = 3, name = 'a';
	CREATE t:2 SET age = [3, 4], name = 'b';
	CREATE t:3 SET age = 4, name = 'c';
};

SELECT id FROM t WHERE age = 3;
SELECT id FROM t WHERE age = 3 AND name != 'z';
SELECT id FROM t WITH NOINDEX WHERE age = 3 AND name != 'z';
