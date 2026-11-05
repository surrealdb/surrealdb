/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Filters on a company through its sponsors relation: a correlated subquery keyed on the indexed `in` reads the index per company; nested IN subqueries that read no outer row each run once and are probed as membership indexes; a traversal filter returns its rows. Each returns the count the same filter over gathered rows returns."
issue = 7332

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 8 }]"

[[test.results]]
value = '''{ attributes: { projections: '*' }, children: [{ attributes: { mode: 'GROUP ALL' }, children: [{ attributes: { predicate: 'array::len((\n  Project[VALUE out]\n    Limit[LIMIT 1]\n      Filter[in = $parent.id]\n        Source[sponsors]\n)) > 0' }, children: [{ attributes: { direction: 'Forward', table: 'company' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Aggregate' }], context: 'Db', operator: 'SelectProject' }'''

[[test.results]]
value = "[company:0, company:4]"

[[test.results]]
value = """{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'id INSIDE (subquery)' }, children: [{ attributes: { direction: 'Forward', table: 'company' }, context: 'Db', operator: 'TableScan' }, { attributes: { strategy: 'once' }, children: [{ attributes: { expr: 'in' }, children: [{ attributes: { predicate: 'out INSIDE (subquery)' }, children: [{ attributes: { direction: 'Forward', table: 'sponsors' }, context: 'Db', operator: 'TableScan' }, { attributes: { strategy: 'once' }, children: [{ attributes: { expr: 'id' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'yes', predicate: "study_type = 'INTERVENTIONAL'", table: 'trial' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }], context: 'Db', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"""

[[test.results]]
value = "[company:1, company:2, company:3, company:5, company:6, company:7, company:8, company:9]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[company:0, company:4]"

[[test.results]]
value = "[{ c: 2 }]"

[[test.results]]
value = "[company:0, company:4]"
*/

DEFINE TABLE company SCHEMAFULL;
DEFINE TABLE trial SCHEMAFULL;
DEFINE FIELD study_type ON trial TYPE option<string>;
DEFINE TABLE sponsors TYPE RELATION FROM company TO trial SCHEMAFULL;
DEFINE INDEX sponsors_in ON sponsors FIELDS in;
FOR $c IN 0..10 { CREATE type::record('company', $c); };
FOR $t IN 0..40 {
	CREATE type::record('trial', $t) SET study_type = IF $t % 4 = 0 { 'INTERVENTIONAL' } ELSE { 'OBSERVATIONAL' };
	RELATE (type::record('company', $t % 8))->sponsors->(type::record('trial', $t));
};

-- A: a correlated subquery keyed on the indexed `in`.
SELECT count() AS c FROM company WHERE array::len((SELECT VALUE out FROM sponsors WHERE in = $parent.id LIMIT 1)) > 0 GROUP ALL;
SELECT count() AS c FROM company WHERE array::len((SELECT VALUE out FROM sponsors WHERE in = $parent.id LIMIT 1)) > 0 GROUP ALL EXPLAIN;

-- C: nested memberships that read no outer row.
SELECT VALUE id FROM company WHERE id IN (SELECT VALUE in FROM sponsors WHERE out IN (SELECT VALUE id FROM trial WHERE study_type = 'INTERVENTIONAL'));
SELECT VALUE id FROM company WHERE id IN (SELECT VALUE in FROM sponsors WHERE out IN (SELECT VALUE id FROM trial WHERE study_type = 'INTERVENTIONAL')) EXPLAIN;
SELECT VALUE id FROM company WHERE id NOT IN (SELECT VALUE in FROM sponsors WHERE out IN (SELECT VALUE id FROM trial WHERE study_type = 'INTERVENTIONAL'));
LET $trials = SELECT VALUE id FROM trial;
LET $edges = SELECT in, out FROM sponsors;
LET $kept = $trials[WHERE $this.study_type = 'INTERVENTIONAL'];
RETURN array::sort(array::distinct($edges[WHERE out IN $kept].in));

-- D: the traversal filter, and its LIMIT.
SELECT count() AS c FROM company WHERE array::len((SELECT VALUE id FROM ->sponsors->trial WHERE study_type = 'INTERVENTIONAL' LIMIT 1)) > 0 GROUP ALL;
SELECT VALUE id FROM company WHERE ->sponsors->trial[WHERE study_type = 'INTERVENTIONAL'] LIMIT 3;
