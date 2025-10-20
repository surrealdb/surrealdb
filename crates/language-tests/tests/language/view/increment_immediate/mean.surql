
/**

[env]
imports = ["harness/rng.surql"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: view1:[], mean: 50.46dec }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: view2:[], mean: 50.46dec }]"

*/

{
	fn::set_seed(0.1);
	CREATE |test1:1..=100| SET v = math::round(fn::rand() * 100) RETURN NONE;

	DEFINE TABLE view1 AS SELECT math::mean(v) as mean FROM test1 GROUP ALL;
	NONE
};

SELECT * FROM view1;

{
	fn::set_seed(0.1);
	DEFINE TABLE test2;
	DEFINE TABLE view2 AS SELECT math::mean(v) as mean FROM test2 GROUP ALL;
	FOR $i in 1..=100 {
		CREATE type::record("test2",$i) SET v = math::round(fn::rand() * 100) RETURN NONE;
	};
	NONE
};

SELECT * FROM view2;
