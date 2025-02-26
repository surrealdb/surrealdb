/**
[env]
imports = ["datasets/type_order.surql","harness/assert.surql"]

[test]

[[test.results]]
value = "'OK'"
*/

BEGIN;
for $i in 0..(array::len($type_array) - 1){
	fn::assert($type_array[$i] < $type_array[$i + 1]);
};
return "OK";
COMMIT;
