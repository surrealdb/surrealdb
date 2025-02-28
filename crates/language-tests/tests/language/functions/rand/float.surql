/**
[env]
timeout = 5000

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/
let $array = (<array> 0..1000).map(|$x| rand::float());
array::min($array) >= 0;
array::max($array) < 1;
$array.all(|$x| type::is::float($x));

let $array = (<array> 0..1000).map(|$x| rand::float(5,10));
array::min($array) >= 5;
array::max($array) < 10;
$array.all(|$x| type::is::float($x));

