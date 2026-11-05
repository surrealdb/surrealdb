/**
[test]
reason = "Values a block draws from a sequence and the clock are drawn in statement order"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "true"

[env]
namespace = "test"
database = "test"

*/

DEFINE SEQUENCE s;
{ LET $x = sequence::nextval('s'); CREATE g:1 SET v = $x; LET $y = sequence::nextval('s'); [$x < $y, (SELECT VALUE v FROM ONLY g:1) = $x] };
{ LET $a = time::now(); SLEEP 1ms; LET $b = time::now(); $a < $b };
