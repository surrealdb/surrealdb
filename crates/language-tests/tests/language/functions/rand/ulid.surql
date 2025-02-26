/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/
let $a = (<array> 0..1000).map(|$_| rand::ulid());
$a.all(|$x| $x = /[0-9A-HJKM-Z]*/);
$a.distinct().len() == 1000;
