/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "An uncorrelated sub-SELECT with ORDER BY on a field other than id and LIMIT, read in a WHERE IN or in a projection, has the value it has when bound once with LET, and that value is the one the data gives, with and without indexes on either table"
issue = 7470

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[post:1, post:4, post:7, post:10, post:13, post:16, post:19, post:22, post:25, post:28]"

[[test.results]]
value = "[[person:b], [person:b, person:a], [person:a]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[post:1, post:4, post:7, post:10, post:13, post:16, post:19, post:22, post:25, post:28]"

*/

CREATE person:a SET age = 36 RETURN NONE;
CREATE person:b SET age = 50 RETURN NONE;
CREATE person:c SET age = 20 RETURN NONE;
FOR $i IN 0..30 { CREATE type::record('post', $i) SET author = [person:a, person:b, person:c][$i % 3] RETURN NONE; };
LET $oldest = (SELECT id, age FROM person ORDER BY age DESC LIMIT 1).id;
LET $two_oldest = SELECT VALUE id FROM person ORDER BY age DESC LIMIT 2;
LET $second = SELECT VALUE id FROM person ORDER BY age DESC LIMIT 1 START 1;

-- The issue's statement: the ten posts of the oldest person.
SELECT VALUE id FROM post WHERE author IN (SELECT id, age FROM person ORDER BY age DESC LIMIT 1).id ORDER BY id;

-- The bound values are the ones the data gives.
RETURN [$oldest, $two_oldest, $second];

-- Every read against the subquery bound once, with no index, then an index on
-- the outer table, then one on the inner table too.
DEFINE FUNCTION fn::mismatches($first: array<record<person>>, $two: array<record<person>>, $second: array<record<person>>) {
	RETURN [
		{ q: 'in projected ids', ok: (SELECT VALUE id FROM post WHERE author IN (SELECT id, age FROM person ORDER BY age DESC LIMIT 1).id ORDER BY id) == (SELECT VALUE id FROM post WHERE author IN $first ORDER BY id) },
		{ q: 'in values', ok: (SELECT VALUE id FROM post WHERE author IN (SELECT VALUE id FROM person ORDER BY age DESC LIMIT 1) ORDER BY id) == (SELECT VALUE id FROM post WHERE author IN $first ORDER BY id) },
		{ q: 'in two', ok: (SELECT VALUE id FROM post WHERE author IN (SELECT VALUE id FROM person ORDER BY age DESC LIMIT 2) ORDER BY id) == (SELECT VALUE id FROM post WHERE author IN $two ORDER BY id) },
		{ q: 'in after start', ok: (SELECT VALUE id FROM post WHERE author IN (SELECT VALUE id FROM person ORDER BY age DESC LIMIT 1 START 1) ORDER BY id) == (SELECT VALUE id FROM post WHERE author IN $second ORDER BY id) },
		{ q: 'equal first', ok: (SELECT VALUE id FROM post WHERE author = (SELECT VALUE id FROM person ORDER BY age DESC LIMIT 1)[0] ORDER BY id) == (SELECT VALUE id FROM post WHERE author IN $first ORDER BY id) },
		{ q: 'projection', ok: (SELECT VALUE (SELECT VALUE id FROM person ORDER BY age DESC LIMIT 1) FROM post) == (SELECT VALUE $first FROM post) },
		{ q: 'projection after start', ok: (SELECT VALUE (SELECT VALUE id FROM person ORDER BY age DESC LIMIT 1 START 1) FROM post) == (SELECT VALUE $second FROM post) }
	].filter(|$c| !$c.ok).map(|$c| $c.q);
};
RETURN fn::mismatches($oldest, $two_oldest, $second);
DEFINE INDEX post_author ON post FIELDS author;
RETURN fn::mismatches($oldest, $two_oldest, $second);
DEFINE INDEX person_age ON person FIELDS age;
RETURN fn::mismatches($oldest, $two_oldest, $second);

-- A write reads the subquery's value too.
UPDATE post SET hit = true WHERE author IN (SELECT id, age FROM person ORDER BY age DESC LIMIT 1).id RETURN VALUE id;
