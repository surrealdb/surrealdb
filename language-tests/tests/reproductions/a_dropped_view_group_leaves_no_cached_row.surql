/**
[env]
namespace = true
database = true

[test]
reason = "An aggregated view's group row is a record, so removing it has to go through the record API rather than by key. A key write alone leaves the transaction's record cache holding the row, and the cache answers point reads only — so the same transaction then disagrees with itself: a read of the group by its record id is served the row that was just removed, while a scan of the view table correctly omits it. The read before the delete is load-bearing rather than cosmetic, since it is what puts the row in the cache; without it the group is never read by id and there is nothing stale to serve."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, tag: 'a' }]"

[[test.results]]
value = "[{ id: item:2, tag: 'b' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: totals:['a'], tag: 'a', total: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: totals:['b'], tag: 'b', total: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/

DEFINE TABLE item SCHEMALESS;
DEFINE TABLE totals AS SELECT tag, count() AS total FROM item GROUP BY tag;

CREATE item:1 SET tag = 'a';
CREATE item:2 SET tag = 'b';

BEGIN;
-- Reading the group row by id is what puts it in the record cache.
SELECT * FROM totals:['a'];
-- The only row of group 'a' leaves, so the group row is removed.
DELETE item:1;
-- The point read must agree with the scan below: the group is gone.
SELECT * FROM totals:['a'];
SELECT * FROM totals;
COMMIT;

SELECT * FROM totals:['a'];
