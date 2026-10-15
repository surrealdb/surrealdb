-- Builtin functions, closures, IF/ELSE and casts in transforms. A transform that fails on one row
-- (a cast or a builtin that errors) yields the FALLBACK for that row alone, and a closure that
-- fails on one element fails the whole transform for its row. A result that is NONE removes the
-- field from its row. No row reads as a value its declared TYPE refuses.
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD s ON tb TYPE string;
DEFINE FIELD f ON tb TYPE any;
DEFINE FIELD xs ON tb TYPE array<string>;
DEFINE FIELD m ON tb TYPE any;
CREATE tb:1 SET s = '  hello ', f = '-4', xs = ['1', '2'], m = 9;
CREATE tb:2 SET s = 'Mixed', f = 2.5, xs = ['3', 'x'], m = 'str';
CREATE tb:3 SET s = '', f = 'abc', xs = [], m = [1];
-- @upgrade
ALTER FIELD s ON tb TYPE string MIGRATE EXISTING ROWS AS up USING string::uppercase(string::trim($value)) FALLBACK '';
ALTER FIELD f ON tb TYPE float MIGRATE EXISTING ROWS AS fl USING IF <float>$value < 0 THEN math::abs(<float>$value) ELSE math::sqrt(<float>$value) END FALLBACK -1f;
ALTER FIELD xs ON tb TYPE array<int> MIGRATE EXISTING ROWS AS ints USING array::map($value, |$v: string| <int>$v * 10) FALLBACK [];
ALTER FIELD m ON tb TYPE option<int> MIGRATE EXISTING ROWS AS mi USING IF type::is_int($value) THEN $value ELSE NONE END FALLBACK 0;
SELECT * FROM tb ORDER BY id;
-- @drain
SELECT * FROM tb ORDER BY id;
SELECT VALUE type::is_string(s) FROM tb ORDER BY id;
SELECT VALUE type::is_float(f) FROM tb ORDER BY id;
SELECT VALUE xs FROM tb ORDER BY id;
CREATE tb:4 SET s = ' new ', f = -2, xs = [5], m = 3;
UPDATE tb:2 SET s = 'again';
SELECT * FROM tb ORDER BY id;
-- @check
