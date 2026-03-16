/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
DEFINE FUNCTION fn::foo(){
	CREATE f SET a = array::filter([1,2,3,4], |$a| { $a % 2 == 0 });
};
DEFINE FIELD v ON t VALUE $param.map(|$v| $v.lowercase());
migration::diagnose();

