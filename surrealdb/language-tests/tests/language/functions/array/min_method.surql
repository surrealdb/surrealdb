/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "1"


*/
let $array = [1,2,3];
$array.min();
$array[0..1].min();
array::min($array[0..1]);
