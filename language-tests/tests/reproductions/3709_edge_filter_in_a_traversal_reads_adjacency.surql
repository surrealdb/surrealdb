/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A filter on an edge field inside a traversal from known vertices reads each vertex's adjacency and filters its edges, which is what an index on the edge field would be read for; the rows match the filter applied to the edges a scan gathers."
issue = 3709

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: parent_route:0, r: [route:1] }, { id: parent_route:1, r: [route:11] }]"

[[test.results]]
value = """{ attributes: { projections: 'id, r' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'yes', predicate: "buid = 'demo'", table: 'parent_route' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "[route:1, route:11]"

[[test.results]]
value = "{ attributes: { expr: 'out' }, children: [{ attributes: { access: '= 1', direction: 'Forward', index: 'sr_time', table: 'has_route' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE INDEX sr_time ON TABLE has_route FIELDS start;
FOR $p IN 0..4 {
	CREATE type::record('parent_route', $p) SET buid = IF $p < 2 { 'demo' } ELSE { 'other' };
	FOR $j IN 0..3 {
		CREATE type::record('route', $p * 10 + $j) SET n = $j;
		RELATE (type::record('parent_route', $p))->has_route->(type::record('route', $p * 10 + $j)) SET start = $j;
	};
};

SELECT id, ->(has_route WHERE start = 1)->route AS r FROM parent_route WHERE buid = 'demo';
SELECT id, ->(has_route WHERE start = 1)->route AS r FROM parent_route WHERE buid = 'demo' EXPLAIN;
SELECT VALUE out FROM has_route WITH NOINDEX WHERE start = 1 AND in.buid = 'demo' ORDER BY out;
SELECT VALUE out FROM has_route WHERE start = 1 EXPLAIN;
