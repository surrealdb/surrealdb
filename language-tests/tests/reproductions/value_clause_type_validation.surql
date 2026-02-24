/**
[test]
reason = "VALUE clause output must conform to the field TYPE constraint"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: layout:one }]"

[[test.results]]
match = "$error = /Couldn't coerce value for field `layout` of `layoutBuild:.*`: Expected `record<layout>` but found `'layout:one'`/"
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:tngjb45s2x6nq9xq8056, name: 'HELLO' }]"
skip-record-id-key = true

[[test.results]]
value = "[{ id: item:tngjb45s2x6nq9xq8056, name: 'HELLO' }]"
skip-record-id-key = true

*/

-- A VALUE clause that returns a type incompatible with TYPE should error at write time
DEFINE TABLE layoutBuild SCHEMAFULL;
DEFINE TABLE layout SCHEMAFULL;
DEFINE FIELD layout ON TABLE layoutBuild TYPE record<layout> VALUE type::string($value);
CREATE layout:one;
CREATE layoutBuild SET layout = layout:one;

-- A valid VALUE clause that preserves the correct type should succeed
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD name ON TABLE item TYPE string VALUE string::uppercase($value);
CREATE item SET name = "hello";
SELECT * FROM item;
