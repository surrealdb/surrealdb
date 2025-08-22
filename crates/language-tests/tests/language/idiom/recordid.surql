/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:123, val: true }, { id: test:abc, val: true }, { id: test:{ val: 456 }, val: true }, { id: test:[1, 2, 3], val: true }]"

[[test.results]]
value = "[test:123, 123, true, true, NONE, NONE, NONE, NONE]"

[[test.results]]
value = "[test:abc, 'abc', true, true, NONE, NONE, NONE, NONE]"

[[test.results]]
value = "[test:{ val: 456 }, { val: 456 }, true, true, 456, NONE, NONE, NONE]"

[[test.results]]
value = "[test:[1, 2, 3], [1, 2, 3], true, true, [NONE, NONE, NONE], 1, 1, 1]"

*/
LET $int = test:123;
LET $str = test:abc;
LET $obj = test:{ val: 456 };
LET $arr = test:[ 1, 2, 3 ];

CREATE $int, $str, $obj, $arr SET val = true;

[$int.id, $int.id(), $int.val, $int.id.val, $int.id().val, $int[0], $int.id[0], $int.id()[0]];
[$str.id, $str.id(), $str.val, $str.id.val, $str.id().val, $str[0], $str.id[0], $str.id()[0]];
[$obj.id, $obj.id(), $obj.val, $obj.id.val, $obj.id().val, $obj[0], $obj.id[0], $obj.id()[0]];
[$arr.id, $arr.id(), $arr.val, $arr.id.val, $arr.id().val, $arr[0], $arr.id[0], $arr.id()[0]];
