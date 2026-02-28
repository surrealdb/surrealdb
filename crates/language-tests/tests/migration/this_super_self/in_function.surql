/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

DEFINE FUNCTION fn::FOO (){
	SELECT $this FROM bar;
};
migration::diagnose()
