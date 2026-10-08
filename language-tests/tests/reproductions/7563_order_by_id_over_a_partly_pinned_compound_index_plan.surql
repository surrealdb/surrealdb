/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "The plans behind 7563_order_by_id_over_a_partly_pinned_compound_index. At the top level the index does not supply id order under a pinned prefix that leaves another column first, so the sort stays and the limit stays above it, and it does supply the first free column's order. Inside a function the LIMIT pages a DynamicScan, which takes that page in record-id order whatever index it chooses at run time"
issue = 7563

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    Limit [ctx: Db] [limit: 1]\n        SortTopKByKey [ctx: Db] [sort_keys: id ASC, limit: 1]\n            IndexScan [ctx: Db] [index: by_category, access: [category:01K00000000000000000000001], direction: Forward, predicate: category = category:01K00000000000000000000001]\n''''

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    SortByKey [ctx: Db] [sort_keys: id ASC]\n        DynamicScan [ctx: Db] [source: item, predicate: category = category:01K00000000000000000000001, limit: 1, pre_decode_filter: deferred (runtime field state)]\n''''

[[test.results]]
value = "NONE"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    SortByKey [ctx: Db] [sort_keys: id ASC]\n        DynamicScan [ctx: Db] [source: item, predicate: category = category:01K00000000000000000000001, limit: 1, pre_decode_filter: deferred (runtime field state)]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    IndexScan [ctx: Db] [index: by_category, access: [category:01K00000000000000000000001], direction: Forward, limit: 1, predicate: category = category:01K00000000000000000000001]\n''''
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD category ON item TYPE record<category>;
DEFINE FIELD flag ON item TYPE bool;
DEFINE INDEX by_category ON item FIELDS category, flag UNIQUE;
INSERT INTO item [
	{ id: item:01K00000000000000000000001, category: category:01K00000000000000000000001, flag: true },
	{ id: item:01K00000000000000000000002, category: category:01K00000000000000000000001, flag: false }
] RETURN NONE;

EXPLAIN SELECT id FROM item WHERE category = category:01K00000000000000000000001 ORDER BY id LIMIT 1;
DEFINE FUNCTION fn::explained($category: record<category>) {
	RETURN EXPLAIN SELECT id FROM item WHERE category = $category ORDER BY id LIMIT 1;
};
RETURN fn::explained(category:01K00000000000000000000001);
DEFINE FUNCTION fn::explained_literal() {
	RETURN EXPLAIN SELECT id FROM item WHERE category = category:01K00000000000000000000001 ORDER BY id LIMIT 1;
};
RETURN fn::explained_literal();
EXPLAIN SELECT id FROM item WHERE category = category:01K00000000000000000000001 ORDER BY flag LIMIT 1;
