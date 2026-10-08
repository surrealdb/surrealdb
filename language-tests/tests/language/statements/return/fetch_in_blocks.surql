/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "RETURN … FETCH fetches into the value an expression yields, and into each row a statement yields, wherever the RETURN stands: at the top level, in a block, in a loop body and in an IF branch"

[[test.results]]
value = "[{ friend: person:b, id: person:a, name: 'a', tags: [person:b, person:c] }]"

[[test.results]]
value = "[{ friend: person:c, id: person:b, name: 'b' }]"

[[test.results]]
value = "[{ id: person:c, name: 'c' }]"

[[test.results]]
value = "{ many: [{ friend: person:c, id: person:b, name: 'b' }, { id: person:c, name: 'c' }], one: { friend: person:b, id: person:a, name: 'a', tags: [person:b, person:c] } }"

[[test.results]]
value = "{ tags: [{ friend: person:c, id: person:b, name: 'b' }, person:c] }"

[[test.results]]
value = "[{ p: { friend: person:b, id: person:a, name: 'a', tags: [person:b, person:c] } }, { p: { friend: person:c, id: person:b, name: 'b' } }]"

[[test.results]]
value = "{ who: { friend: person:b, id: person:a, name: 'a', tags: [person:b, person:c] } }"

[[test.results]]
value = "{ who: { friend: person:b, id: person:a, name: 'a', tags: [person:b, person:c] } }"

[[test.results]]
value = "{ first: { friend: person:b, id: person:a, name: 'a', tags: [person:b, person:c] } }"

[[test.results]]
value = "[{ friend: { friend: person:c, id: person:b, name: 'b' }, id: person:a }, { friend: { id: person:c, name: 'c' }, id: person:b }, { friend: NONE, id: person:c }]"

[[test.results]]
value = "[{ tags: [{ friend: person:c, id: person:b, name: 'b' }, person:c] }]"

[[test.results]]
value = "[{ friend: { friend: { id: person:c, name: 'c' }, id: person:b, name: 'b' } }]"

[[test.results]]
value = "{ friend: { friend: person:c, id: person:b, name: 'b' }, id: person:a, name: 'a', tags: [person:b, person:c] }"

[[test.results]]
value = "[{ friend: { id: person:c, name: 'c' } }]"

[[test.results]]
value = "{ friend: { friend: person:b, id: person:a, name: 'a', tags: [person:b, person:c] }, id: person:d }"

[[test.results]]
value = "[{ friend: { friend: person:c, id: person:b, name: 'b', seen: true }, id: person:a, tags: [{ friend: person:c, id: person:b, name: 'b', seen: true }, person:c] }, { friend: { id: person:c, name: 'c' }, id: person:b, tags: NONE }]"

[[test.results]]
value = "[{ friend: { id: person:c, name: 'c' } }]"

[[test.results]]
value = "{ gone: NONE }"

*/

CREATE person:a SET name = 'a', friend = person:b, tags = [person:b, person:c];
CREATE person:b SET name = 'b', friend = person:c;
CREATE person:c SET name = 'c';

-- An expression's value.
RETURN { one: person:a, many: [person:b, person:c] } FETCH one, many;
RETURN { tags: [person:b, person:c] } FETCH tags[0];
RETURN [{ p: person:a }, { p: person:b }] FETCH p;

-- In a block, reading a value the block binds.
{
	LET $p = { who: person:a };
	RETURN $p FETCH who;
};

-- In a loop body: the first RETURN answers the block.
FOR $p IN [person:a, person:b] {
	RETURN { who: $p } FETCH who;
};

-- In an IF branch.
IF true {
	RETURN { first: person:a } FETCH first;
} ELSE {
	RETURN NONE;
};

-- A statement's rows: each row is fetched, under the statement's own FETCH.
RETURN SELECT id, friend FROM person ORDER BY id FETCH friend;
RETURN SELECT tags FROM person:a FETCH tags[0];
RETURN (SELECT friend FROM person:a FETCH friend) FETCH friend.friend;
RETURN SELECT * FROM ONLY person:a FETCH friend;
{
	LET $who = person:b;
	RETURN SELECT friend FROM person WHERE id = $who FETCH friend;
};

-- A write's rows.
RETURN CREATE ONLY person:d SET friend = person:a FETCH friend;
RETURN UPDATE person:a, person:b SET seen = true RETURN id, friend, tags FETCH friend, tags[0];
{
	LET $who = person:c;
	RETURN UPSERT person:e SET friend = $who RETURN friend FETCH friend;
};

-- A record that does not exist fetches to NONE.
RETURN { gone: person:zz } FETCH gone;
