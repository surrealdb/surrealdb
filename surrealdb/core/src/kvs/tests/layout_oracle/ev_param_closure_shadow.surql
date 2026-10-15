-- Parameters and closures in a migration. A transform reads only its `$value`: a transform that
-- names a parameter is refused when it is defined. A closure's own argument may shadow `$value`.
-- A stored VALUE may read a parameter, also from inside a closure, and is judged at each write
-- with the value the parameter has then; the rows the job rewrites are not written again.
DEFINE PARAM $scale VALUE 2;
DEFINE PARAM $dbl VALUE 2;
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD xs ON tb TYPE array<string>;
DEFINE FIELD ys ON tb TYPE array<string>;
DEFINE FIELD k ON tb TYPE int VALUE $scale * 5;
DEFINE FIELD c ON tb TYPE array<int> VALUE array::map([1, 2], |$x: int| $x * $scale);
CREATE tb:1 SET xs = ['1', '2'], ys = ['3'];
CREATE tb:2 SET xs = ['x', '4'], ys = ['5'];
-- @upgrade
ALTER FIELD xs ON tb TYPE array<int> MIGRATE EXISTING ROWS AS m0 USING array::map($value, |$v: string| <int>$v * $scale) FALLBACK [];
ALTER FIELD xs ON tb TYPE array<int> MIGRATE EXISTING ROWS AS m0 USING array::map($value, |$v: string| <int>$v * $dbl) FALLBACK [];
ALTER FIELD xs ON tb TYPE array<int> MIGRATE EXISTING ROWS AS m1 USING array::map($value, |$v: string| <int>$v * 2) FALLBACK [];
ALTER FIELD ys ON tb TYPE array<int> MIGRATE EXISTING ROWS AS m2 USING array::map($value, |$value: string| <int>$value + 1) FALLBACK [];
SELECT * FROM tb ORDER BY id;
SELECT VALUE k FROM tb ORDER BY id;
DEFINE PARAM OVERWRITE $scale VALUE 3;
DEFINE PARAM OVERWRITE $dbl VALUE 3;
CREATE tb:3 SET xs = [7], ys = [8];
UPDATE tb:1 SET ys = [9];
SELECT * FROM tb ORDER BY id;
RETURN $dbl * 7;
-- @drain
SELECT * FROM tb ORDER BY id;
REMOVE PARAM $scale;
CREATE tb:4 SET xs = [1], ys = [2];
SELECT * FROM tb ORDER BY id;
-- @check
