/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define nested field `a.a` as parent field `a` is a `COMPUTED` field."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot define field `b` as `COMPUTED` since a nested field `b.b` already exists."

[[test.results]]
error = "Cannot define field `c.c` as `COMPUTED` fields must be top-level."

*/
DEFINE FIELD a ON a COMPUTED 1;
DEFINE FIELD a.a ON a;

DEFINE FIELD b.b ON b;
DEFINE FIELD b ON b COMPUTED 1;

DEFINE FIELD c.c ON c COMPUTED 1;