/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A `FETCH` path is walked up to its first part the walk does not go through, and
a recursion -- `.@`, `.{2}`, `.{1..3}` -- is one: a link the walk stands on when
it reaches the recursion is fetched, and the path ends there. An array the walk
stands on there is not walked into, so its links stay as they are. A `.@` in a
`FETCH` path is no error where no recursion encloses it.
"""

[[test.results]]
value = "[{ friend: person:2, id: person:1, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:2, name: 'Jaime' }]"

[[test.results]]
value = "[{ author: person:1, id: post:1, title: 'hello' }]"

[[test.results]]
value = "[{ author: { friend: person:2, id: person:1, name: 'Tobie' }, id: post:1, title: 'hello' }]"

[[test.results]]
value = "[{ author: { friend: person:2, id: person:1, name: 'Tobie' }, id: post:1, title: 'hello' }]"

[[test.results]]
value = "[{ author: { friend: { id: person:2, name: 'Jaime' }, id: person:1, name: 'Tobie' }, id: post:1, title: 'hello' }]"

[[test.results]]
value = "[{ author: { friend: { id: person:2, name: 'Jaime' }, id: person:1, name: 'Tobie' }, id: post:1, title: 'hello' }]"

[[test.results]]
value = "[{ author: { friend: person:2, id: person:1, name: 'Tobie' }, id: post:1, title: 'hello' }]"

[[test.results]]
value = "[{ author: { friend: person:2, id: person:1, name: 'Tobie' }, id: post:1, title: 'hello' }]"

[[test.results]]
value = "[{ author: { friend: person:2, id: person:1, name: 'Tobie' }, id: post:1, title: 'hello' }]"

[[test.results]]
value = "[{ author: { friend: person:2, id: person:1, name: 'Tobie' }, id: post:1, title: 'hello' }]"

[[test.results]]
value = "1"

[[test.results]]
value = "{ a: { id: person:2, name: 'Jaime' } }"

[[test.results]]
value = "[{ a: { id: person:2, name: 'Jaime' } }]"

[[test.results]]
value = "{ a: [person:2] }"

[[test.results]]
value = "{ a: [{ id: person:2, name: 'Jaime' }] }"

[[test.results]]
value = "{ a: { id: person:2, name: 'Jaime' } }"
*/

CREATE person:1 SET name = 'Tobie', friend = person:2;
CREATE person:2 SET name = 'Jaime';
CREATE post:1 SET author = person:1, title = 'hello';

SELECT * FROM post:1 FETCH author.@;
SELECT * FROM post:1 FETCH author.@.name;
SELECT * FROM post:1 FETCH author.friend.@;
SELECT * FROM post:1 FETCH author.@, author.friend;
SELECT * FROM post:1 FETCH author.{2};
SELECT * FROM post:1 FETCH author.{1..3};
SELECT * FROM post:1 FETCH author.{2}.name;
SELECT * FROM post:1 FETCH author.{1..3}(.friend.@);

RETURN 1 FETCH a.@;
RETURN { a: person:2 } FETCH a.@;
RETURN [{ a: person:2 }] FETCH a.@;
RETURN { a: [person:2] } FETCH a.@;
RETURN { a: [person:2] } FETCH a.*.@;
RETURN { a: person:2 } FETCH a.?.@;
