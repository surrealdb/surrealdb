/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found { id: true } for field `a`, with record `a:cqjtfv5izprjud4end9f`, but expected a { id: number } whilst validating against array<{ id: number }>"

*/
DEFINE FIELD a ON a TYPE array<{ id: number }>;
CREATE a SET a = [ { id: true } ];