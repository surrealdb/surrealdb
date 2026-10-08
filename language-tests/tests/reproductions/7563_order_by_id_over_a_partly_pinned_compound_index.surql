/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY id with LIMIT or START over an index whose equality-pinned prefix leaves another column before the record id must page in record-id order, inside a function as at the top level; the index orders the matching rows by that column instead"
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
value = "NONE"

[[test.results]]
value = "{ indexed: [{ id: item:01K00000000000000000000001 }], ordered: [{ category: category:01K00000000000000000000001, id: item:01K00000000000000000000001 }], scan: [{ id: item:01K00000000000000000000001 }] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ after_first: [item:01K00000000000000000000002], first: [item:01K00000000000000000000001], last: [item:01K00000000000000000000002], second: [item:01K00000000000000000000002] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:01K00000000000000000000001]"

[[test.results]]
value = "[item:01K00000000000000000000001]"

[[test.results]]
value = "[item:01K00000000000000000000002]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 1]
        Sort [ctx: Db] [sort_keys: id ASC, keep: 1]
            IndexScan [ctx: Db] [table: item, index: by_category, access: [category:01K00000000000000000000001], direction: Forward]
''''

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Limit [ctx: Db] [limit: 1]
        Sort [ctx: Db] [sort_keys: id ASC, keep: 1]
            IndexScan [ctx: Db] [table: item, index: by_category, access: [category:01K00000000000000000000001], direction: Forward]
''''

[[test.results]]
value = "[item:01K00000000000000000000002]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: item, index: by_category, access: [category:01K00000000000000000000001], direction: Forward, limit: 1]
''''
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD category ON item TYPE record<category>;
DEFINE FIELD flag ON item TYPE bool;
DEFINE INDEX by_category ON item FIELDS category, flag UNIQUE;
INSERT INTO item [
	{ id: item:01K00000000000000000000001, category: category:01K00000000000000000000001, flag: true },
	{ id: item:01K00000000000000000000002, category: category:01K00000000000000000000001, flag: false }
] RETURN NONE;

-- The issue's fixture: every query returns the record ending in 1.
DEFINE FUNCTION fn::repro($category: record<category>) {
	RETURN {
		indexed: (SELECT id FROM item WHERE category = $category ORDER BY id ASC LIMIT 1),
		scan: (SELECT id FROM item WITH NOINDEX WHERE category = $category ORDER BY id ASC LIMIT 1),
		ordered: (SELECT id, category FROM item WHERE category = $category ORDER BY category ASC, id ASC LIMIT 1)
	};
};
RETURN fn::repro(category:01K00000000000000000000001);

-- Every paging of the id order, from a function argument and from a literal.
DEFINE FUNCTION fn::paged($category: record<category>) {
	RETURN {
		first: (SELECT VALUE id FROM item WHERE category = $category ORDER BY id LIMIT 1),
		second: (SELECT VALUE id FROM item WHERE category = $category ORDER BY id LIMIT 1 START 1),
		after_first: (SELECT VALUE id FROM item WHERE category = $category ORDER BY id START 1),
		last: (SELECT VALUE id FROM item WHERE category = $category ORDER BY id DESC LIMIT 1)
	};
};
RETURN fn::paged(category:01K00000000000000000000001);
DEFINE FUNCTION fn::literal() {
	RETURN SELECT VALUE id FROM item WHERE category = category:01K00000000000000000000001 ORDER BY id LIMIT 1;
};
RETURN fn::literal();

-- At the top level.
SELECT VALUE id FROM item WHERE category = category:01K00000000000000000000001 ORDER BY id LIMIT 1;
SELECT VALUE id FROM item WHERE category = category:01K00000000000000000000001 ORDER BY id LIMIT 1 START 1;

-- The index does not supply id order under the pinned prefix, so the sort stays.
EXPLAIN SELECT id FROM item WHERE category = category:01K00000000000000000000001 ORDER BY id LIMIT 1;
DEFINE FUNCTION fn::explained($category: record<category>) {
	RETURN EXPLAIN SELECT id FROM item WHERE category = $category ORDER BY id LIMIT 1;
};
RETURN fn::explained(category:01K00000000000000000000001);

-- It does supply the order of the first free column, so that sort is still dropped.
SELECT VALUE id FROM item WHERE category = category:01K00000000000000000000001 ORDER BY flag LIMIT 1;
EXPLAIN SELECT id FROM item WHERE category = category:01K00000000000000000000001 ORDER BY flag LIMIT 1;
