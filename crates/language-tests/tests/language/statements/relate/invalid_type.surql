/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Can not execute RELATE statement where property 'in' is: 1"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Can not execute RELATE statement where property 'id' is: 2"

*/
let $a = 1;
let $b = 2;
RELATE $a->knows->$b CONTENT { id:"foo", t : 1 };

let $a = f:1;
RELATE $a->knows->$b CONTENT { id:"foo", t : 1 };
