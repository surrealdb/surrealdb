CREATE foo:1 SET v = 1;
SELECT VALUE v FROM ONLY foo;

DEFINE FUNCTION fn::baz() {
	return SELECT VALUE v FROM ONLY foo:1
};

fn::baz();


DEFINE FUNCTION fn::bar($val: any) {
	CREATE foo:2 set val = $val;
};

fn::bar(2);


