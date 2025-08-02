/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `obj[*]` of `test:1`: Expected `number` but found `'a'`"

*/


DEFINE FIELD obj ON test TYPE object;
DEFINE FIELD obj.* ON test TYPE number;
CREATE test:1 SET obj.a = 'a';
