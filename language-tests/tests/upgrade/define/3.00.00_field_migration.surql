/**
[env]
compatibility = 3
imports = ["upgrade/define/3.00.00_field_migration_import.surql"]

[test]
upgrade = true
reason = "Field definitions a released binary wrote read as Unchecked on the current build, and a migration over the released binary's rows, which carry no generation, applies to every one of them, before and after rows of the new build sit beside them."
# Migrations need the stored grammar of the current checkout.
version = ">=3.4.0-a"

[[test.results]]
value = "{ events: {  }, fields: { price: 'DEFINE FIELD price ON item TYPE int | string PERMISSIONS FULL', qty: 'DEFINE FIELD qty ON item TYPE int PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ price: '10', qty: 1 }, { price: 'n/a', qty: 2 }, { price: 30, qty: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ price: 10, qty: 1 }, { price: 0, qty: 2 }, { price: 30, qty: 3 }]"

[[test.results]]
value = "[true, true, true]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ price: 20f, qty: 1 }, { price: 0f, qty: 20 }, { price: 60f, qty: 3 }, { price: 80f, qty: 4 }]"

[[test.results]]
value = "[item:3, item:4]"

[[test.results]]
value = "['price_to_int', 'price_to_float']"
*/
-- The drifted definition reads back unchanged and has no migrations section.
INFO FOR TABLE item;
SELECT price, qty FROM item ORDER BY id;
-- The server runs the migration job beside the test, and a statement that
-- queues work for a table, or writes one of its rows, while the job writes that
-- table can lose a retryable conflict to it. Every such statement runs in this
-- transaction, so the job starts only once it has committed.
BEGIN;
ALTER FIELD price ON item TYPE int MIGRATE EXISTING ROWS AS price_to_int USING <int>$value FALLBACK 0;
SELECT price, qty FROM item ORDER BY id;
SELECT VALUE type::is_int(price) FROM item ORDER BY id;
CREATE item:4 SET price = 40, qty = 4 RETURN NONE;
UPDATE item:2 SET qty = 20 RETURN NONE;
ALTER FIELD price ON item TYPE float MIGRATE EXISTING ROWS AS price_to_float USING $value * 2 FALLBACK -1f;
COMMIT;
SELECT price, qty FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE price >= 60 ORDER BY id;
(INFO FOR TABLE item).migrations.migrations.*.name;
