/**
[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE item SCHEMALESS;
	DEFINE INDEX item_k ON item FIELDS k;
	DEFINE TABLE pair SCHEMALESS;
	DEFINE INDEX pair_abk ON pair FIELDS a, b, k;
	DEFINE TABLE field_value SCHEMALESS;
	DEFINE INDEX field_value_by_generation ON field_value FIELDS tenant, scope, generation;
	DEFINE FUNCTION fn::rt_by_object($o: object) { RETURN SELECT VALUE id FROM item WHERE k = $o.k; };
	DEFINE FUNCTION fn::rt_pair_first($o: object) { RETURN SELECT VALUE id FROM pair WHERE a = $o.a AND b = 21 AND k = 4321; };
	DEFINE FUNCTION fn::rt_pair_last($k: int) { RETURN SELECT VALUE id FROM pair WHERE a = 1 AND b = 21 AND k = $k; };
	DEFINE FUNCTION fn::rt_pair_every($o: object) { RETURN SELECT VALUE id FROM pair WHERE a = $o.a AND b = $o.b AND k = $o.k; };
	DEFINE FUNCTION fn::rt_bump_path($o: object) { RETURN UPDATE item SET n = 1 WHERE k = $o.k RETURN NONE; };
	-- `$n` rows in each table; every key the benches read matches one row of its table.
	DEFINE FUNCTION fn::rt_fill($n: int) {
		FOR $i IN 0..$n {
			CREATE item SET k = $i RETURN NONE;
			CREATE pair SET a = $i % 10, b = $i % 100, k = $i RETURN NONE;
			CREATE field_value SET tenant = 'acme', scope = 's' + <string>($i % 50), generation = $i, doc = 'd' + <string>$i, pack = 'p1' RETURN NONE;
		};
	};
	-- Each record written to `rt_src` updates the `item` whose `k` equals its own.
	DEFINE TABLE rt_src SCHEMALESS;
	DEFINE EVENT rt_bump ON rt_src WHEN true THEN { UPDATE item SET n = 1 WHERE k = $after.k RETURN NONE; };
	-- Each record written to `rt_doc` resolves its `found` to the `item` whose `k` equals it.
	DEFINE TABLE rt_doc SCHEMALESS;
	DEFINE FIELD found ON rt_doc VALUE (SELECT VALUE id FROM item WHERE k = $value);
	-- `rt_cdoc:one` derives its `found` from the `item` whose `k` equals its own on each read.
	DEFINE TABLE rt_cdoc SCHEMALESS;
	DEFINE FIELD found ON rt_cdoc COMPUTED (SELECT VALUE id FROM item WHERE k = $parent.k);
	CREATE rt_cdoc:one SET k = 4321 RETURN NONE;
	RETURN 'OK';
}
