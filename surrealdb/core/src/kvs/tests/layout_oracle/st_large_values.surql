-- Very large values through stacked migrations: the largest and smallest integers,
-- a float at the top of its range, an overflowing transform, a decimal too big
-- for an integer, a 60000-character string and a 3000-element array. Each row
-- takes the FALLBACK where its value fails the transform, and the value of the
-- next step is the value the previous step left.
DEFINE TABLE big SCHEMAFULL;
DEFINE FIELD n ON big TYPE any;
DEFINE FIELD w ON big TYPE any;
DEFINE FIELD x ON big TYPE any;
DEFINE FIELD s ON big TYPE string;
DEFINE FIELD arr ON big TYPE any;
CREATE big:1 SET n = 9223372036854775807, w = 4611686018427387904, x = 1.7976931348623157e308, s = 'a' RETURN NONE;
CREATE big:2 SET n = -9223372036854775807, w = 4611686018427387903, x = 1e30, s = 'b' RETURN NONE;
CREATE big:3 SET n = 1e308, w = 1, x = -1.5e300, s = 'c' RETURN NONE;
CREATE big:4 SET n = 79228162514264337593543950335dec, w = 2, x = 2.5, s = 'd' RETURN NONE;
CREATE big:5 SET n = 9223372036854775806, w = 3, x = 0, s = string::repeat('ab', 30000) RETURN NONE;
CREATE big:6 SET n = 0, w = 0, x = 3.5e38f, s = 'e', arr = array::repeat(7, 3000) RETURN NONE;
-- @upgrade
ALTER FIELD n ON big TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK 0;
ALTER FIELD n ON big TYPE float MIGRATE EXISTING ROWS AS n_float USING <float>$value * 2 FALLBACK 0f;
SELECT id, n FROM big ORDER BY id;
SELECT VALUE id FROM big WHERE n > 1e18 ORDER BY id;
-- @check
-- Doubling the largest integer overflows: the transform fails, and the FALLBACK
-- is taken for that row only.
ALTER FIELD w ON big TYPE int MIGRATE EXISTING ROWS AS w_dbl USING $value * 2 FALLBACK -1;
ALTER FIELD w ON big TYPE float MIGRATE EXISTING ROWS AS w_half USING <float>$value / 2 FALLBACK -0.5f;
SELECT id, w FROM big ORDER BY id;
-- A float that overflows to infinity under a transform with a FALLBACK.
ALTER FIELD x ON big TYPE float MIGRATE EXISTING ROWS AS x_ten USING $value * 10 FALLBACK 0f;
SELECT id, x FROM big ORDER BY id;
-- A long string, a chain string to string to int.
ALTER FIELD s ON big TYPE string MIGRATE EXISTING ROWS AS s_up USING string::uppercase($value) FALLBACK '';
ALTER FIELD s ON big TYPE int MIGRATE EXISTING ROWS AS s_len USING string::len($value) FALLBACK -1;
SELECT id, s FROM big ORDER BY id;
-- A large array, mapped by a transform with a closure, then retyped.
ALTER FIELD arr ON big TYPE array<int> MIGRATE EXISTING ROWS AS arr_int USING array::map($value, |$v| <int>$v * 2) FALLBACK [];
ALTER FIELD arr ON big TYPE array<float> MIGRATE EXISTING ROWS AS arr_float USING array::map($value, |$v| <float>$v + 0.5) FALLBACK [];
SELECT id, array::len(arr) AS len, arr[0] AS first FROM big ORDER BY id;
-- @drain
SELECT id, n, w, x, s, array::len(arr) AS len FROM big ORDER BY id;
(INFO FOR TABLE big).migrations;
-- @check
-- A row written after the job runs with the largest values its types admit.
CREATE big:7 SET n = 9223372036854775807, w = 1, x = 1.7976931348623157e308, s = 1, arr = [] RETURN NONE;
SELECT id, n, w, x, s, array::len(arr) AS len FROM big ORDER BY id;
SELECT VALUE n FROM big ORDER BY id;
SELECT VALUE s FROM big ORDER BY id;
SELECT VALUE array::len(arr) FROM big ORDER BY id;
-- @drain
-- @check
