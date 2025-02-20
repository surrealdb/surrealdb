/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

*/
let $array = (<array> 0..1000).map(|$x| rand());
array::min($array) >= 0;
array::max($array) < 1;
$array.all(|$x| type::is::float($x))
