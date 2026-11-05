/**
[env]
namespace = true
database = true

[test]
reason = "The record an UPSERT under a WHERE guarantees when it matched nothing fails the statement with whatever fails its write, an error its data clause raises, or a cast, and is not written. Main discards these failures and answers no rows; this is an intentional difference."

[[test.results]]
value = "[{ id: ok:1, v: 1 }]"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
error = "Could not cast into `int` using input `'zz'`"

[[test.results]]
value = "[3]"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
value = "[{ count: 2 }]"

*/
CREATE ok:1 SET v = 1;
UPSERT ok SET x = THROW 'boom' WHERE v = 9;
UPSERT ok SET x = <int>'zz' WHERE v = 9;
UPSERT ok SET x = 3 WHERE v = 9 RETURN VALUE x;
UPSERT ok:5 SET x = THROW 'boom' WHERE v = 9;
UPSERT newt SET x = THROW 'boom' WHERE true;
SELECT count() FROM ok GROUP ALL;
