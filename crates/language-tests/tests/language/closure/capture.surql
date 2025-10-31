/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

*/

let $a = 1;
(||{ $a; })();
$a;
