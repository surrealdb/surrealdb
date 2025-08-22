/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: typed:a, typed: 'a' }]"

[[test.results]]
error = "Couldn't coerce value for field `typed` of `typed:1`: Expected `string` but found `1`"

*/
DEFINE FIELD typed ON typed TYPE string COMPUTED id.id();

CREATE typed:a;
CREATE typed:1;