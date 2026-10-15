/**
[env]
compatibility = 3

[test]
reason = "Each rule a transform, a fallback and a migrated field must satisfy is refused when the statement runs."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the transform must have no effects: it may not read data, the catalog, the session, the clock or randomness"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the transform must have no effects: it may not read data, the catalog, the session, the clock or randomness"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the transform must have no effects: it may not read data, the catalog, the session, the clock or randomness"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the transform must have no effects: it may not read data, the catalog, the session, the clock or randomness"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the transform reads only `$value`, not `$p`"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the transform may not hold a statement"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the transform may not call a user-defined function, a model, a script or a module"

[[test.results]]
error = "An error occurred: A migration's FALLBACK must be a constant"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: the FALLBACK does not pass the field's definition"

[[test.results]]
error = "Cannot migrate field `a` of table `tb`: FALLBACK may be left out only when the transform cannot fail and its result always passes the field's definition"

[[test.results]]
error = "Cannot migrate field `b` of table `tb`: FALLBACK may be left out only when the transform cannot fail and its result always passes the field's definition"

[[test.results]]
error = "Cannot migrate field `c` of table `tb`: a COMPUTED field is never stored"

[[test.results]]
error = "Cannot migrate field `i` of table `tb`: the index `by_i` reads it: remove the index, migrate, then define it again"

[[test.results]]
error = "Cannot migrate field `r` of table `tb`: a REFERENCE field's backlinks hold its stored values"

[[test.results]]
error = "Cannot migrate field `id` of table `tb`: `id` is fixed by the record's key"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot migrate field `a` of table `src`: the view `v` selects from the table"

[[test.results]]
value = "[{ a: '1' }]"

[[test.results]]
value = "{ events: {  }, fields: { a: 'DEFINE FIELD a ON `tb` TYPE string PERMISSIONS FULL', c: 'DEFINE FIELD c ON `tb` COMPUTED 1 PERMISSIONS FULL', i: 'DEFINE FIELD i ON `tb` TYPE string PERMISSIONS FULL', r: 'DEFINE FIELD r ON `tb` TYPE none | record<other> REFERENCE ON DELETE IGNORE PERMISSIONS FULL' }, indexes: { by_i: 'DEFINE INDEX by_i ON `tb` FIELDS i' }, lives: {  }, tables: {  } }"
*/
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD a ON tb TYPE string;
DEFINE FIELD c ON tb COMPUTED 1;
DEFINE FIELD r ON tb TYPE option<record<other>> REFERENCE;
DEFINE FIELD i ON tb TYPE string;
DEFINE INDEX by_i ON tb FIELDS i;
DEFINE FUNCTION fn::conv($v: any) { RETURN <int>$v };
CREATE tb:1 SET a = '1', i = 'x' RETURN NONE;
LET $p = 3;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m1 USING time::now() FALLBACK 0;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m2 USING rand::int() FALLBACK 0;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m3 USING <int>session::id() FALLBACK 0;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m4 USING <int>record::exists(tb:1) FALLBACK 0;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m5 USING $p FALLBACK 0;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m6 USING (SELECT VALUE 1 FROM ONLY tb:1) FALLBACK 0;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m7 USING fn::conv($value) FALLBACK 0;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m8 USING <int>$value FALLBACK $value;
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m9 USING <int>$value FALLBACK 'zero';
ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS m10 USING <int>$value;
DEFINE FIELD b ON tb TYPE string MIGRATE EXISTING ROWS AS m11 USING $value ?? 'USD';
DEFINE FIELD OVERWRITE c ON tb COMPUTED 2 MIGRATE EXISTING ROWS AS m12 USING 1 FALLBACK 1;
ALTER FIELD i ON tb TYPE string MIGRATE EXISTING ROWS AS m13 USING string::uppercase($value) FALLBACK '';
ALTER FIELD r ON tb MIGRATE EXISTING ROWS AS m14 USING NONE;
DEFINE FIELD OVERWRITE id ON tb TYPE int MIGRATE EXISTING ROWS AS m15 USING 1 FALLBACK 1;
-- A view source.
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE string;
DEFINE TABLE v AS SELECT a FROM src;
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS m16 USING <int>$value FALLBACK 0;
-- Nothing was migrated.
SELECT a FROM tb:1;
INFO FOR TABLE tb;
