/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 0 for field `bar`, with record `foo:1`, but field must conform to: $value != 0"


*/

DEFINE TABLE foo;
DEFINE FIELD bar ON foo ASSERT $value != 0 DEFAULT 0;
CREATE foo:1;
