/**
[test]
reason = "A DEFAULT that does not coerce to its field's TYPE is refused by DEFINE FIELD, so the field is not stored and later writes are not held to it. A write whose value does not coerce to the TYPE fails as a validation error, not an internal one."

[[test.results]]
value = "NONE"

[[test.results]]
match = "$error = /its DEFAULT does not coerce to `set<string>`/"
error = true

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:2, n: 1 }]"

[[test.results]]
match = "$error = /Couldn't coerce value for field `n` of `item:1`: Expected `int` but found `'x'`/"
error = true
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD ops ON item TYPE set<string> DEFAULT {};
DEFINE FIELD n ON item TYPE int;
CREATE item:2 SET n = 1;
CREATE item:1 SET n = 'x';
