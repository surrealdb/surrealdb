/**
[env]
namespace = true
database = true

[test]
reason = "A statement or a block that a method argument's closure holds reads the current value of the step through `$this` and `$parent`, which in a frame with no row is the method's receiver, as the same statement written without a closure does: the answer is the same whether or not the body also reads the current value elsewhere. An element filter in the body that reads `$parent` reads the row of the frame the literal is written in, which is none in a frame with no row, whether or not the body reads the current value elsewhere, and whether the filter is at the body's own level or in a block, a `LET` or an `IF` the body holds; in a frame with a row it reads that row. A closure nested in such a block, with an element filter that reads `$parent`, reads the same row. The closure-free twins (`concat`, a literal array) are the control."

[[test.results]]
value = "[[1, 2], [1, 2]]"

[[test.results]]
value = "[[1, 2], [1, 2]]"

[[test.results]]
value = "[{ lim: 1 }, { lim: 2 }, [1, 2]]"

[[test.results]]
value = "[[1, 2], [1, 2]]"

[[test.results]]
value = "[[1, 2], [1, 2]]"

[[test.results]]
value = "[[{ lim: 1 }, { lim: 2 }], [{ lim: 1 }, { lim: 2 }]]"

[[test.results]]
value = "[[{ lim: 1 }, { lim: 2 }], [{ lim: 1 }, { lim: 2 }]]"

[[test.results]]
value = "[{ lim: 1 }, { lim: 2 }, [{ lim: 1 }, { lim: 2 }]]"

[[test.results]]
value = "[2, 2]"

[[test.results]]
value = "[2, 2]"

[[test.results]]
value = "[1, 5, 10]"

[[test.results]]
value = "[1, 5, 10]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "1"

[[test.results]]
value = "[[3]]"

[[test.results]]
value = "[[3]]"

[[test.results]]
value = "[[3]]"

[[test.results]]
value = "[[3]]"

[[test.results]]
value = "[[3]]"

[[test.results]]
value = "[[3]]"

[[test.results]]
value = "[[3, [{ lim: 2 }]]]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[{ id: rb:1, lim: 1 }]"

[[test.results]]
value = "[{ id: rb:2, lim: 8 }]"

[[test.results]]
value = "[[[3]], [[]]]"

[[test.results]]
value = "[[[3]], [[]]]"

[[test.results]]
value = "[[[3]], [[]]]"

[[test.results]]
value = "[rb:1]"

[[test.results]]
value = "[[[3]], [[]]]"

[[test.results]]
value = "[[[]]]"

[[test.results]]
value = "[[[]]]"

[[test.results]]
value = "[[[[]]], [[[3]]]]"

[[test.results]]
value = "[[[[]]], [[[3]]]]"

[[test.results]]
value = "[[[]]]"

[[test.results]]
value = "[[[]]]"

[[test.results]]
value = "[[[[]]], [[[3]]]]"

[[test.results]]
value = "[[[[]]], [[[3]]]]"

[[test.results]]
value = "[[[]]]"

[[test.results]]
value = "[[[]]]"

[[test.results]]
value = "[[[[]]], [[[3]]]]"

[[test.results]]
value = "[[[[]]], [[[3]]]]"
*/

RETURN [{ lim: 1 }, { lim: 2 }].map(|$e| (SELECT VALUE lim FROM $this));
RETURN [{ lim: 1 }, { lim: 2 }].map(|$e| [$this, (SELECT VALUE lim FROM $this)][1]);
RETURN [{ lim: 1 }, { lim: 2 }].concat([(SELECT VALUE lim FROM $this)]);
RETURN [{ lim: 1 }, { lim: 2 }].map(|$e| (SELECT VALUE lim FROM $parent));
RETURN [{ lim: 1 }, { lim: 2 }].map(|$e| [$this, (SELECT VALUE lim FROM $parent)][1]);
RETURN [{ lim: 1 }, { lim: 2 }].map(|$e| { LET $z = $parent; $z });
RETURN [{ lim: 1 }, { lim: 2 }].map(|$e| [$this, { LET $z = $parent; $z }][1]);
RETURN [{ lim: 1 }, { lim: 2 }].concat([{ LET $z = $parent; $z }]);
RETURN [{ lim: 1 }, { lim: 2 }].map(|$e| count(SELECT * FROM $this));
RETURN [{ lim: 1 }, { lim: 2 }].map(|$e| [$this, count(SELECT * FROM $this)][1]);
RETURN [1, 5, 10].filter(|$e| (SELECT VALUE 1 FROM $this)[0] = 1);
RETURN [1, 5, 10].filter(|$e| [$this, (SELECT VALUE 1 FROM $this)][1][0] = 1);
RETURN [3].map(|$e| [$this, [$e][WHERE $this >= $parent.lim].len()][1]);
RETURN [3].map(|$e| [$e][WHERE $this >= $parent.lim].len());
RETURN [{ lim: 2 }].map(|$e| [$this, [$e][WHERE $this >= $parent.lim].len()][1]);
RETURN [3, [3][WHERE $this >= $parent.lim].len()][1];
RETURN [{ lim: 2 }].map(|$e| [{ [3][WHERE $this >= $parent.lim] }, $this][0]);
RETURN [{ lim: 2 }].map(|$e| { [3][WHERE $this >= $parent.lim] });
RETURN [{ lim: 2 }].map(|$e| [{ LET $z = [3][WHERE $this >= $parent.lim]; $z }, $this][0]);
RETURN [{ lim: 2 }].map(|$e| { LET $z = [3][WHERE $this >= $parent.lim]; $z });
RETURN [{ lim: 2 }].map(|$e| [IF true { [3][WHERE $this >= $parent.lim] }, $this][0]);
RETURN [{ lim: 2 }].map(|$e| IF true { [3][WHERE $this >= $parent.lim] });
RETURN [{ lim: 2 }].map(|$e| { [3][WHERE $this >= $parent.lim] } + [$this]);
RETURN [3, { [3][WHERE $this >= $parent.lim] }][1];
RETURN [3, { LET $z = [3][WHERE $this >= $parent.lim]; $z }][1];
RETURN [3, IF true { [3][WHERE $this >= $parent.lim] }][1];
CREATE rb:1 SET lim = 1;
CREATE rb:2 SET lim = 8;
SELECT VALUE [{ lim: 2 }].map(|$e| [{ [3][WHERE $this >= $parent.lim] }, $this][0]) FROM rb ORDER BY id;
SELECT VALUE [{ lim: 2 }].map(|$e| { [3][WHERE $this >= $parent.lim] }) FROM rb ORDER BY id;
SELECT VALUE [{ lim: 2 }].map(|$e| [IF true { [3][WHERE $this >= $parent.lim] }, $this][0]) FROM rb ORDER BY id;
SELECT VALUE id FROM rb WHERE [{ lim: 2 }].map(|$e| [{ LET $z = [3][WHERE $this >= $parent.lim]; $z }, $this][0]) = [[3]];
UPDATE rb SET z = [{ lim: 2 }].map(|$e| [{ [3][WHERE $this >= $parent.lim] }, $this][0]) RETURN VALUE z;
RETURN [{ lim: 2 }].map(|$e| [{ [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]) }, $this][0]);
RETURN [{ lim: 2 }].map(|$e| { [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]) });
SELECT VALUE [{ lim: 2 }].map(|$e| [{ [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]) }, $this][0]) FROM rb ORDER BY id;
SELECT VALUE [{ lim: 2 }].map(|$e| { [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]) }) FROM rb ORDER BY id;
RETURN [{ lim: 2 }].map(|$e| [{ LET $z = [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]); $z }, $this][0]);
RETURN [{ lim: 2 }].map(|$e| { LET $z = [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]); $z });
SELECT VALUE [{ lim: 2 }].map(|$e| [{ LET $z = [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]); $z }, $this][0]) FROM rb ORDER BY id;
SELECT VALUE [{ lim: 2 }].map(|$e| { LET $z = [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]); $z }) FROM rb ORDER BY id;
RETURN [{ lim: 2 }].map(|$e| [IF true { [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]) }, $this][0]);
RETURN [{ lim: 2 }].map(|$e| IF true { [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]) });
SELECT VALUE [{ lim: 2 }].map(|$e| [IF true { [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]) }, $this][0]) FROM rb ORDER BY id;
SELECT VALUE [{ lim: 2 }].map(|$e| IF true { [{ lim: 1 }].map(|$f| [3][WHERE $this < $parent.lim]) }) FROM rb ORDER BY id;
