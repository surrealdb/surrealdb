/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found { id: true } for field `fd[*]`, with record `a:1`, but expected a { id: number }"

*/
DEFINE FIELD fd ON a TYPE array<{ id: number }>;
CREATE a:1 SET fd = [ { id: true } ];