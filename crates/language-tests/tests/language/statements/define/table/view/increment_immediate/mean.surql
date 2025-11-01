
/**

[env]
imports = ["harness/rng.surql"]

[test]
wip = true
reason = "Random seeding is not deterministic across hosts."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: view1:[], mean: 0.5044379346651635f }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: view2:[], mean: 1.5044379346651635f }]"

[[test.results]]
value = "[{ mean: 0.5044379346651635f }]"

[[test.results]]
value = "[{ mean: 1.5044379346651635f }]"


*/

{
	fn::set_seed(0.1);
	CREATE |test1:1..=100| SET v = fn::rand() RETURN NONE;

	DEFINE TABLE view1 AS SELECT math::mean(v) as mean FROM test1 GROUP ALL;
	NONE
};

SELECT * FROM view1;

{
	fn::set_seed(0.1);
	DEFINE TABLE test2;
	DEFINE TABLE view2 AS SELECT math::mean(v) + 1 as mean FROM test2 GROUP ALL;
	FOR $i in 1..=100 {
		CREATE type::record("test2",$i) SET v = fn::rand() RETURN NONE;
	};
	NONE
};

SELECT * FROM view2;
SELECT math::mean(v) as mean FROM test2 GROUP ALL;
SELECT (math::mean(v) + 1) as mean FROM test2 GROUP ALL;
