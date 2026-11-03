/**
[env]
namespace = true
database = true

[test]
reason = "A closure that is a method argument and calls a projection function (`type::field`, `type::fields`) beside a read of the document root, as a method argument that reads a field does, reads the document of the frame that invokes it: the record where the statement writes one (an `UPDATE` or `CREATE` `SET`, which answers as the same body does without the projection call), and none in a frame with no row (a `RETURN`, an `INSERT` value), where both reads are `NONE`. The projection function reads the same value as `$this` does. A closure written in the body of another closure reads the document that closure runs with. A block of the body that binds with `LET` reads the same document, and is the value of its array element."

[[test.results]]
value = "[{ id: t:1, k: 1, n: 30 }]"

[[test.results]]
value = "[[[1, 20]]]"

[[test.results]]
value = "[[[1, 20]]]"

[[test.results]]
value = "[[[1, 20]]]"

[[test.results]]
value = "[[[1, 30]]]"

[[test.results]]
value = "[[[1, 20]]]"

[[test.results]]
value = "[[[NONE, NONE]]]"

[[test.results]]
value = "[[[NONE, NONE]]]"

[[test.results]]
value = "[[NONE, NONE]]"

[[test.results]]
value = "[[[NONE, NONE]]]"

[[test.results]]
value = "[[[1, 20]]]"

[[test.results]]
value = "[[[[1, 20]]]]"

[[test.results]]
value = "[[[[1, 20]]]]"

[[test.results]]
value = "[[[1, 30]]]"

[[test.results]]
value = "[[[[1, 20]]]]"
*/

CREATE t:1 SET k = 1, n = 30;
UPDATE t:1 SET a = [5].map(|$e| [type::field('k'), [10, 20][k]]) RETURN VALUE a;
UPDATE t:1 SET a = [{ k: 7 }].map(|$e| [type::field('k'), [10, 20][k]]) RETURN VALUE a;
UPDATE t:1 SET a = [5].map(|$e| [type::fields(['k']), [10, 20][k]]) RETURN VALUE a;
UPDATE t:1 SET a = [5].map(|$e| [type::field('k'), { LET $v = n; $v }]) RETURN VALUE a;
UPDATE t:1 SET a = [5].map(|$e| [$this.k, [10, 20][k]]) RETURN VALUE a;
CREATE t:9 SET a = [{ k: 1 }].map(|$e| [type::field('k'), [10, 20][k]]) RETURN VALUE a;
INSERT INTO t { id: t:8, a: [{ k: 1 }].map(|$e| [type::field('k'), [10, 20][k]]) } RETURN VALUE a;
RETURN [{ k: 1 }].map(|$e| [type::field('k'), [10, 20][k]]);
RETURN [{ k: 1 }].map(|$e| { LET $x = [{ k: 1 }]; RETURN $x.map(|$f| [type::field('k'), [10, 20][k]]) });
SELECT VALUE [{ k: 7 }].map(|$e| [type::field('k'), [10, 20][k]]) FROM t:1;
UPDATE t:1 SET a = [1].map(|$a| array::map([5], |$e| [type::field('k'), [10, 20][k]])) RETURN VALUE a;
UPDATE t:1 SET a = array::map([1], |$a| array::map([5], |$e| [type::fields(['k']), [10, 20][k]])) RETURN VALUE a;
UPDATE t:1 SET a = [1].map(|$a| (|$e| [type::field('k'), { LET $v = n; $v }])(5)) RETURN VALUE a;
SELECT VALUE [1].map(|$a| array::map([5], |$e| [type::field('k'), [10, 20][k]])) FROM t:1;
