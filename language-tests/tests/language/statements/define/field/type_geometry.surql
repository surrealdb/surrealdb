/**
[test]
reason = "Test DEFINE FIELD with geometry subtypes"

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
value = "{ events: {  }, fields: { area: 'DEFINE FIELD area ON geo_test TYPE geometry<polygon> PERMISSIONS FULL', coll: 'DEFINE FIELD coll ON geo_test TYPE geometry<collection> PERMISSIONS FULL', line: 'DEFINE FIELD line ON geo_test TYPE geometry<line> PERMISSIONS FULL', loc: 'DEFINE FIELD loc ON geo_test TYPE geometry<point> PERMISSIONS FULL', multi: 'DEFINE FIELD multi ON geo_test TYPE geometry<multipoint> PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
error = "Couldn't coerce value for field `area` of `geo_test:1`: Expected `geometry` but found `NONE`"

*/

DEFINE TABLE geo_test;
DEFINE FIELD loc ON geo_test TYPE geometry<point>;
DEFINE FIELD area ON geo_test TYPE geometry<polygon>;
DEFINE FIELD line ON geo_test TYPE geometry<line>;
DEFINE FIELD multi ON geo_test TYPE geometry<multipoint>;
DEFINE FIELD coll ON geo_test TYPE geometry<collection>;
INFO FOR TABLE geo_test;
CREATE geo_test:1 SET loc = (1.0, 2.0);
