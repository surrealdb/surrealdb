/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 0 for field `bar`, with record `foo:1`, but expected a string"

*/

DEFINE TABLE foo;
DEFINE FIELD bar ON foo TYPE string DEFAULT 0;
CREATE foo:1;
