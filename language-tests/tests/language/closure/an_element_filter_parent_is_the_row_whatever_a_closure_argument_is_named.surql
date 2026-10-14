/**
[env]
namespace = true
database = true

[test]
reason = "An element filter in a closure body binds `$parent` to the row of the frame the closure is written in, as it does outside a closure, nearer than a closure argument of that name entered before the filter: in a `SELECT` projection, a `WHERE`, an `UPDATE` `SET` and a `DELETE` `WHERE` the filter reads the record, and in a frame with no row it reads none. A `LET $parent` is refused wherever it is written, as `$parent` is a name the system binds, so each statement that holds one raises the refusal."

[[test.results]]
value = "[{ id: ap:1, lim: 1 }]"

[[test.results]]
value = "[{ id: ap:2, lim: 8 }]"

[[test.results]]
value = "[[[]], [[]]]"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
value = "[[[5, 10]], [[10]]]"

[[test.results]]
value = "[[5, 10], [10]]"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
value = "[ap:2]"

[[test.results]]
error = "'parent' is a protected variable and cannot be set"

[[test.results]]
value = "[[[]], [[]]]"

[[test.results]]
value = "[[5, 10]]"

[[test.results]]
value = "[5, 10]"

[[test.results]]
value = "[ap:2]"
*/
CREATE ap:1 SET lim = 1;
CREATE ap:2 SET lim = 8;
SELECT VALUE [1].map(|$parent| [5, 10][WHERE $this > $parent]) FROM ap ORDER BY id;
SELECT VALUE { LET $parent = 1; [1].map(|$e| [5, 10][WHERE $this > $parent]) } FROM ap ORDER BY id;
SELECT VALUE [{ lim: 6 }].map(|$parent| [5, 10][WHERE $this > $parent.lim]) FROM ap ORDER BY id;
SELECT VALUE (|$parent| [5, 10][WHERE $this > $parent.lim])({ lim: 6 }) FROM ap ORDER BY id;
SELECT VALUE { LET $parent = { lim: 6 }; (|$e| [5, 10][WHERE $this > $parent.lim])(0) } FROM ap ORDER BY id;
SELECT VALUE id FROM ap WHERE (|$parent| [5, 10][WHERE $this > $parent.lim])({ lim: 6 }) = [10];
SELECT VALUE id FROM ap WHERE { LET $parent = { lim: 6 }; [5, 10][WHERE $this > $parent.lim] } = [10];
UPDATE ap SET z = [1].map(|$parent| [5, 10][WHERE $this > $parent]) RETURN VALUE z;
RETURN [1].map(|$parent| [5, 10][WHERE $this > $parent]);
RETURN (|$parent| [5, 10][WHERE $this > $parent.lim])({ lim: 6 });
DELETE ap WHERE (|$parent| [5, 10][WHERE $this > $parent.lim])({ lim: 6 }) = [10] RETURN VALUE $before.id;
