/**
[test]
reason = "Stacked migrations: a row applies exactly the steps it missed, in order, so a row written between two steps is migrated once by the second only."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[246f, 468f]"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "[{ field: 246f }]"

[[test.results]]
value = "[tb:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[246f, 468f]"

[[test.results]]
error = "Cannot migrate field `field` of table `tb`: the table has already run a different migration named `double_field`"

[[test.results]]
value = "[246f, 468f]"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD field ON tb TYPE string;
UPSERT tb:1 SET field = "123" RETURN NONE;
ALTER FIELD field ON tb TYPE int
    MIGRATE EXISTING ROWS AS field_to_int USING <int>$value FALLBACK 0;
UPSERT tb:2 SET field = 234 RETURN NONE;
ALTER FIELD field ON tb TYPE float
    MIGRATE EXISTING ROWS AS double_field USING $value * 2 FALLBACK 7;
SELECT VALUE field FROM tb ORDER BY id;
SELECT VALUE type::is_float(field) FROM tb ORDER BY id;
SELECT field FROM tb:1;
SELECT VALUE id FROM tb WHERE field > 300;
-- Re-running the whole script is a no-op.
DEFINE TABLE IF NOT EXISTS tb SCHEMAFULL;
ALTER FIELD field ON tb TYPE int
    MIGRATE EXISTING ROWS AS field_to_int USING <int>$value FALLBACK 0;
ALTER FIELD field ON tb TYPE float
    MIGRATE EXISTING ROWS AS double_field USING $value * 2 FALLBACK 7;
SELECT VALUE field FROM tb ORDER BY id;
-- Reusing a name with a different transform fails.
ALTER FIELD field ON tb TYPE float
    MIGRATE EXISTING ROWS AS double_field USING $value * 3 FALLBACK 7;
SELECT VALUE field FROM tb ORDER BY id;
