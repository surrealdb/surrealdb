
/**
[env]

[env.capabilities]
insecure-storable-closures = true

[test]

[[test.results]]
value = "NULL"
*/


{

DEFINE INDEX f ON t1 SEARCH ANALYZER az VS DOC_IDS_ORDER 50;

CREATE t2:{a: [{b: [false,1], c: true}]};
CREATE t2:{a: [{b: [false,0f], c: true}]};

DEFINE FUNCTION fn::f1(){
	SELECT $this FROM bar;
};
DEFINE FUNCTION fn::f2() {
	$param.*;
};
DEFINE FUNCTION fn::f3() {
	$param.name[1];
};
DEFINE FUNCTION fn::f4() {
	$param.id;
};
DEFINE FUNCTION fn::f5() {
	CREATE foo SET a = <future> { time::now() };
};

CREATE t3:1 SET a = <future> { 1 };
CREATE t4:1 SET a = <future> { <future> { 1 } };

DEFINE FUNCTION fn::t5() {
	INSERT INTO foo {
		f: <future>{ time::now() }
	}
};
DEFINE FUNCTION fn::t6() {
	RELATE a:1->b->c:1 SET a = <future> { time::now() };
};
DEFINE FUNCTION fn::t7() {
	UPDATE foo SET a = <future> { time::now() };
};

DEFINE FUNCTION fn::t8() {
	UPSERT foo SET a = <future> { time::now() };
};

DEFINE FUNCTION fn::f6(){
	array::logical_and([true],[false])
};

DEFINE FUNCTION fn::f7(){
	array::logical_or([true],[false])
};

DEFINE FUNCTION fn::f8(){
	array::range(1,2)
};

DEFINE FUNCTION fn::f9(){
	math::max([])
};

DEFINE FUNCTION fn::f10(){
	math::min([]);
};

DEFINE FUNCTION fn::f11(){
	math::sqrt(-1);
};

CREATE t9:1 SET a = || { 1 + 1 };

DEFINE FUNCTION fn::f12(){
	CREATE f SET a = || { 1 + 1 };
};

null
};
