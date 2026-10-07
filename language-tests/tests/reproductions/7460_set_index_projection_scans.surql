/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Wildcard set indexes must not duplicate or omit records when their element columns are not bound."
issue = 7460

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[{ id: ints:a }, { id: ints:b }, { id: ints:e }]"
[[test.results]]
value = "[{ id: ints:b }]"
[[test.results]]
value = "[{ id: ints:a }]"
[[test.results]]
value = "true"
[[test.results]]
value = "true"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[{ id: compound:a }, { id: compound:b }, { id: compound:e }]"
[[test.results]]
value = "[{ id: compound:a }]"
[[test.results]]
value = "[{ category: 'g', id: compound:a }, { category: 'g', id: compound:b }, { category: 'g', id: compound:e }]"
[[test.results]]
value = "[{ category: 'g', id: compound:a }, { category: 'g', id: compound:b }, { category: 'g', id: compound:e }, { category: 'h', id: compound:c }]"
[[test.results]]
value = "[{ id: compound:a }, { id: compound:b }, { id: compound:e }]"
[[test.results]]
value = "[{ category: 'g', id: compound:a }, { category: 'g', id: compound:b }, { category: 'g', id: compound:e }, { category: 'h', id: compound:c }]"
[[test.results]]
value = "[{ id: compound:a, rank: 1 }, { id: compound:b, rank: 2 }, { id: compound:e, rank: 3 }, { id: compound:c, rank: 4 }]"
[[test.results]]
value = "[{ count: 3 }]"
[[test.results]]
value = "[{ count: 1 }]"
[[test.results]]
value = "[{ id: compound:a }, { id: compound:b }, { id: compound:c }, { id: compound:e }]"
[[test.results]]
value = "NONE"
[[test.results]]
value = "[{ id: compound:a }, { id: compound:b }, { id: compound:e }]"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[{ id: leading:a }, { id: leading:b }]"
[[test.results]]
value = "[{ id: leading:a }]"

[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[{ id: arrays:a }, { id: arrays:e }]"

[[test.results]]
value = "NONE"
[[test.results]]
value = "true"
[[test.results]]
value = "true"

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
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[{ id: join_probe:a }]"
[[test.results]]
value = "[{ id: join_probe:a }]"
[[test.results]]
value = "NONE"
[[test.results]]
value = "NONE"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"
[[test.results]]
value = "[{ id: leading:a }, { id: leading:b }]"
[[test.results]]
value = "[{ id: leading:a }, { id: leading:b }]"
[[test.results]]
value = "[{ id: leading:a }]"
*/

DEFINE FIELD vals ON ints TYPE set<int>;
DEFINE INDEX vals ON ints FIELDS vals.*;
CREATE ints:a SET vals = {1, 2} RETURN NONE;
CREATE ints:b SET vals = {3, 4} RETURN NONE;
CREATE ints:e SET vals = <set>[] RETURN NONE;
SELECT id FROM ints ORDER BY id;
SELECT id FROM ints ORDER BY id LIMIT 1 START 1;
SELECT id FROM ints WHERE vals CONTAINS 1;
-- Comparing the projected set is different from seeking an element.
RETURN (SELECT id FROM ints WHERE vals.* = {1, 2}) =
    (SELECT id FROM ints WITH NOINDEX WHERE vals.* = {1, 2});
RETURN (SELECT id FROM ints WHERE vals.* > [0] ORDER BY id) =
    (SELECT id FROM ints WITH NOINDEX WHERE vals.* > [0] ORDER BY id);

-- A scalar prefix does not bind the trailing element column.
DEFINE FIELD vals ON compound TYPE set<int>;
DEFINE FIELD category ON compound TYPE string;
DEFINE INDEX compound_vals ON compound FIELDS category, rank, vals.*;
CREATE compound:a SET category = 'g', rank = 1, vals = {1, 2} RETURN NONE;
CREATE compound:b SET category = 'g', rank = 2, vals = {3, 4} RETURN NONE;
CREATE compound:c SET category = 'h', rank = 4, vals = {5, 6} RETURN NONE;
CREATE compound:e SET category = 'g', rank = 3, vals = <set>[] RETURN NONE;
SELECT id FROM compound WHERE category = 'g';
SELECT id FROM compound WHERE category = 'g' AND vals CONTAINS 1;
SELECT id, category FROM compound WHERE category = 'g' ORDER BY category, id;
SELECT id, category FROM compound ORDER BY category, id;
SELECT id FROM compound WITH INDEX compound_vals WHERE category = 'g';
SELECT id, category FROM compound WHERE category IN ['g', 'h'] ORDER BY category, id;
SELECT id, rank FROM compound WHERE category IN ['g', 'h'] ORDER BY rank;
SELECT count() FROM compound WHERE category = 'g' GROUP ALL;
SELECT count() FROM compound WHERE category = 'g' AND vals CONTAINS 1 GROUP ALL;
SELECT id FROM compound WHERE category = 'g' OR category = 'h' ORDER BY id;
LET $source = type::table('compound');
SELECT id FROM $source WHERE category = 'g' ORDER BY id;

-- Membership on the leading wildcard remains eligible for an index seek.
DEFINE FIELD vals ON leading TYPE set<int>;
DEFINE INDEX leading_vals ON leading FIELDS vals.*, category;
CREATE leading:a SET category = 'g', vals = {1, 2} RETURN NONE;
CREATE leading:b SET category = 'h', vals = {1, 3} RETURN NONE;
CREATE leading:e SET category = 'g', vals = <set>[] RETURN NONE;
SELECT id FROM leading WHERE vals CONTAINS 1 ORDER BY id;
SELECT id FROM leading WHERE vals CONTAINS 1 AND category = 'g';

-- Arrays use the same wildcard entry expansion and scan eligibility.
DEFINE FIELD vals ON arrays TYPE array<int>;
DEFINE INDEX array_vals ON arrays FIELDS vals.*;
CREATE arrays:a SET vals = [1, 2] RETURN NONE;
CREATE arrays:e SET vals = [] RETURN NONE;
SELECT id FROM arrays ORDER BY id;

-- Parameters must not make whole-projection equality an element seek.
LET $whole = {1, 2};
RETURN (SELECT id FROM ints WHERE vals.* = $whole) =
    (SELECT id FROM ints WITH NOINDEX WHERE vals.* = $whole);
RETURN (SELECT id FROM ints WHERE vals.* == $whole) =
    (SELECT id FROM ints WITH NOINDEX WHERE vals.* == $whole);

-- A record-link lookup also scans a local compound-key prefix.
DEFINE FIELD vals ON join_probe TYPE set<int>;
DEFINE FIELD link ON join_probe TYPE record<related>;
DEFINE INDEX link_vals ON join_probe FIELDS link, vals.*;
DEFINE INDEX label ON related FIELDS label;
CREATE related:a SET label = 'hit' RETURN NONE;
CREATE related:b SET label = 'miss' RETURN NONE;
CREATE join_probe:a SET link = related:a, vals = {1, 2} RETURN NONE;
CREATE join_probe:b SET link = related:b, vals = {3, 4} RETURN NONE;
SELECT id FROM join_probe WHERE link.label = 'hit' ORDER BY id;
SELECT id FROM join_probe WITH NOINDEX WHERE link.label = 'hit' ORDER BY id;

-- A membership seek cannot leave another wildcard column unbound.
DEFINE FIELD other ON leading TYPE set<int>;
DEFINE INDEX multiple_vals ON leading FIELDS vals.*, other.*;
UPDATE leading:a SET other = {4, 5} RETURN NONE;
UPDATE leading:b SET other = {6, 7} RETURN NONE;
SELECT id FROM leading WITH INDEX multiple_vals WHERE vals CONTAINS 1 ORDER BY id;
SELECT id FROM leading WITH INDEX multiple_vals WHERE vals CONTAINSANY [1, 2] ORDER BY id;
SELECT id FROM leading WITH INDEX multiple_vals WHERE vals CONTAINSALL [1, 2] ORDER BY id;
