/**
[env]
imports = ["datasets/type_order.surql"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function array::max(). Argument 1 was the wrong type. Expected `array` but found `'some text'`"

[[test.results]]
value = "3"

[[test.results]]
value = "'text'"

[[test.results]]
value = "[{ id: t:0, v: NONE }, { id: t:1, v: NULL }, { id: t:2, v: true }, { id: t:3, v: 1 }, { id: t:4, v: 1d }, { id: t:5, v: u'ffffffff-ffff-ffff-ffff-ffffffffffff' }, { id: t:6, v: [] }, { id: t:7, v: {  } }, { id: t:8, v: (1, 2) }, { id: t:9, v: a:b }, { id: t:10, v: /a/ }, { id: t:11, v: 1..2 }]"



*/
array::max([]);
array::max("some text");
array::max([1,2,3]);
array::max([1,2,"text",3,3,4]);

BEGIN;
for $a in (<array> 0..$type_array.len()).map(|$x,$i| { i: $i, range: 0..($x+1) }){
	CREATE type::thing('t',$a.i) SET v = array::max($type_array[$a.range]);
};
RETURN SELECT v,id FROM t;
COMMIT;

