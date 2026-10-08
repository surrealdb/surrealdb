/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A `FETCH` path is followed along its whole length, fetching each link on the
way, so a target names what it says: `FETCH link.deep.deeper` fetches `link`,
`link.deep` and `link.deep.deeper`, and naming every prefix --
`FETCH link, link.deep, link.deep.deeper` -- fetches the same three in any
order.
"""

[[test.results]]
value = "[{ id: tag:3, name: 'c' }]"

[[test.results]]
value = "[{ deeper: tag:3, id: tag:2, name: 'b' }]"

[[test.results]]
value = "[{ deep: tag:2, id: tag:1, name: 'a' }]"

[[test.results]]
value = "[{ id: t:1, link: tag:1 }]"

[[test.results]]
value = "[{ id: u:1, list: [tag:1] }]"

[[test.results]]
value = "[{ id: t:1, link: { deep: tag:2, id: tag:1, name: 'a' } }]"

[[test.results]]
value = "[{ id: t:1, link: { deep: { deeper: tag:3, id: tag:2, name: 'b' }, id: tag:1, name: 'a' } }]"

[[test.results]]
value = "[{ id: t:1, link: { deep: { deeper: tag:3, id: tag:2, name: 'b' }, id: tag:1, name: 'a' } }]"

[[test.results]]
value = "[{ id: t:1, link: { deep: { deeper: { id: tag:3, name: 'c' }, id: tag:2, name: 'b' }, id: tag:1, name: 'a' } }]"

[[test.results]]
value = "[{ id: t:1, link: { deep: { deeper: tag:3, id: tag:2, name: 'b' }, id: tag:1, name: 'a' } }]"

[[test.results]]
value = "[{ id: t:1, link: { deep: { deeper: { id: tag:3, name: 'c' }, id: tag:2, name: 'b' }, id: tag:1, name: 'a' } }]"

[[test.results]]
value = "[{ id: u:1, list: [{ deep: { deeper: tag:3, id: tag:2, name: 'b' }, id: tag:1, name: 'a' }] }]"

[[test.results]]
value = "[{ id: u:1, list: [{ deep: { deeper: tag:3, id: tag:2, name: 'b' }, id: tag:1, name: 'a' }] }]"

[[test.results]]
value = "[{ id: v:1, nested: { inner: tag:1 } }]"

[[test.results]]
value = "[{ id: v:1, nested: { inner: { deep: tag:2, id: tag:1, name: 'a' } } }]"
*/

CREATE tag:3 SET name = 'c';
CREATE tag:2 SET name = 'b', deeper = tag:3;
CREATE tag:1 SET name = 'a', deep = tag:2;
CREATE t:1 SET link = tag:1;
CREATE u:1 SET list = [tag:1];

-- One link deep: it is fetched.
SELECT * FROM t FETCH link;

-- Every prefix named: the whole chain is fetched, whatever the order the
-- targets are written in.
SELECT * FROM t FETCH link, link.deep;
SELECT * FROM t FETCH link.deep, link;
SELECT * FROM t FETCH link, link.deep, link.deep.deeper;

-- Only the deepest named: the path is followed, or dropped after one link.
SELECT * FROM t FETCH link.deep;
SELECT * FROM t FETCH link.deep.deeper;

-- The same holds where the path crosses an array of links.
SELECT * FROM u FETCH list.deep;
SELECT * FROM u FETCH list, list.deep;

-- A path through a plain object reaches on either engine: nothing is
-- crossed that has to be fetched first.
CREATE v:1 SET nested = { inner: tag:1 };
SELECT * FROM v FETCH nested.inner;
