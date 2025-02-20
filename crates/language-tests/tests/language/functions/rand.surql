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
let $array = (<array> 0..10000).map(|$x| rand());
array::min($array) >= 0;
array::max($array) < 1;
$array.filter(|$x| type::is::float($x)).len() == 10000
