/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `bar` of `foo:1`: Expected `string` but found `0`"

*/

DEFINE TABLE foo;
DEFINE FIELD bar ON foo TYPE string DEFAULT 0;
CREATE foo:1;
