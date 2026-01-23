/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/
{
	DEFINE FUNCTION fn::foo() {
		$param.id;
	};
	NONE;
};

migration::diagnose()
