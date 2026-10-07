/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Wildcard set indexes must preserve predicate semantics for nullish values and collection projections."
issue = 7460
results = [
    "NONE", "NONE", "[]", "[]",
    "[]", "[]", "[]", "[]", "[]", "[]", "[]",
    "[{ id: nulls:a }]", "true", "true", "true", "true",
    "NONE", "[]",
    "NONE", "NONE", "[]", "[]", "[]",
    "[{ id: actual_nulls:n }]", "[{ id: actual_nulls:u }]",
    "[{ id: actual_nulls:n }]", "[{ id: actual_nulls:u }]",
    "[{ count: 1 }]",
    "true", "true", "true", "true",
    "true", "true", "true", "true", "true",
    "NONE", "NONE", "[]", "[]", "true", "true", "true",
]
*/

DEFINE FIELD vals ON nulls TYPE set<int>;
DEFINE INDEX vals ON nulls FIELDS vals.*;
CREATE nulls:e SET vals = <set>[] RETURN NONE;
CREATE nulls:a SET vals = {1, 2} RETURN NONE;
SELECT id FROM nulls WHERE vals CONTAINS NULL ORDER BY id;
SELECT id FROM nulls WHERE NULL INSIDE vals ORDER BY id;
SELECT id FROM nulls WHERE vals CONTAINSANY [NULL] ORDER BY id;
SELECT id FROM nulls WHERE [NULL] ANYINSIDE vals ORDER BY id;
SELECT id FROM nulls WHERE vals CONTAINSALL [NULL] ORDER BY id;
SELECT id FROM nulls WHERE [NULL] ALLINSIDE vals ORDER BY id;
SELECT id FROM nulls WHERE vals CONTAINSANY [NONE] ORDER BY id;
SELECT id FROM nulls WHERE vals CONTAINSANY [NULL, 1] ORDER BY id;
RETURN (SELECT count() FROM nulls WHERE vals CONTAINSANY [NULL] GROUP ALL) =
    (SELECT count() FROM nulls WITH NOINDEX WHERE vals CONTAINSANY [NULL] GROUP ALL);
RETURN (SELECT count() FROM nulls WHERE vals CONTAINS NULL GROUP ALL) =
    (SELECT count() FROM nulls WITH NOINDEX WHERE vals CONTAINS NULL GROUP ALL);
RETURN (SELECT id FROM nulls WHERE vals CONTAINSANY [NULL, 1] ORDER BY id LIMIT 1 START 1) =
    (SELECT id FROM nulls WITH NOINDEX WHERE vals CONTAINSANY [NULL, 1] ORDER BY id LIMIT 1 START 1);
RETURN (SELECT id FROM nulls WHERE vals CONTAINSANY [NULL] OR vals CONTAINS 1 ORDER BY id) =
    (SELECT id FROM nulls WITH NOINDEX WHERE vals CONTAINSANY [NULL] OR vals CONTAINS 1 ORDER BY id);
LET $source = type::table('nulls');
SELECT id FROM $source WHERE vals CONTAINSANY [NULL] ORDER BY id;

-- Actual NULL and NONE elements must still match, without the empty record.
DEFINE FIELD vals ON actual_nulls TYPE set<any>;
DEFINE INDEX vals ON actual_nulls FIELDS vals.*;
CREATE actual_nulls:e SET vals = <set>[] RETURN NONE;
CREATE actual_nulls:n SET vals = {NULL, 1} RETURN NONE;
CREATE actual_nulls:u SET vals = {NONE, 2} RETURN NONE;
SELECT id FROM actual_nulls WHERE vals CONTAINS NULL ORDER BY id;
SELECT id FROM actual_nulls WHERE vals CONTAINS NONE ORDER BY id;
SELECT id FROM actual_nulls WHERE vals CONTAINSANY [NULL] ORDER BY id;
SELECT id FROM actual_nulls WHERE [NONE] ANYINSIDE vals ORDER BY id;
SELECT count() FROM actual_nulls WHERE vals CONTAINS NULL GROUP ALL;
RETURN (SELECT count() FROM actual_nulls WHERE vals.* CONTAINS NULL GROUP ALL) =
    (SELECT count() FROM actual_nulls WITH NOINDEX WHERE vals.* CONTAINS NULL GROUP ALL);
RETURN (SELECT id FROM actual_nulls WHERE vals.* CONTAINSANY [NULL] ORDER BY id) =
    (SELECT id FROM actual_nulls WITH NOINDEX WHERE vals.* CONTAINSANY [NULL] ORDER BY id);
RETURN (SELECT id FROM actual_nulls WHERE vals.* CONTAINSANY [NULL, NONE] ORDER BY id) =
    (SELECT id FROM actual_nulls WITH NOINDEX WHERE vals.* CONTAINSANY [NULL, NONE] ORDER BY id);
RETURN (SELECT id FROM actual_nulls WHERE [NULL] ANYINSIDE vals.* ORDER BY id) =
    (SELECT id FROM actual_nulls WITH NOINDEX WHERE [NULL] ANYINSIDE vals.* ORDER BY id);

-- Explicit projections retain their own predicate semantics in each planner.
RETURN (SELECT id FROM nulls WHERE vals.* CONTAINS 1 ORDER BY id) =
    (SELECT id FROM nulls WITH NOINDEX WHERE vals.* CONTAINS 1 ORDER BY id);
RETURN (SELECT id FROM nulls WHERE vals.* CONTAINSANY [1] ORDER BY id) =
    (SELECT id FROM nulls WITH NOINDEX WHERE vals.* CONTAINSANY [1] ORDER BY id);
RETURN (SELECT count() FROM nulls WHERE vals.* CONTAINS 1 GROUP ALL) =
    (SELECT count() FROM nulls WITH NOINDEX WHERE vals.* CONTAINS 1 GROUP ALL);
RETURN (SELECT id FROM nulls WHERE vals CONTAINS 1 AND vals.* CONTAINS 1 ORDER BY id) =
    (SELECT id FROM nulls WITH NOINDEX WHERE vals CONTAINS 1 AND vals.* CONTAINS 1 ORDER BY id);
RETURN (SELECT id FROM nulls WHERE vals CONTAINSANY [1] AND vals.* CONTAINSANY [1] ORDER BY id) =
    (SELECT id FROM nulls WITH NOINDEX WHERE vals CONTAINSANY [1] AND vals.* CONTAINSANY [1] ORDER BY id);

-- Traversal through a collection also requires its projection predicate.
DEFINE FIELD vals ON projected TYPE set<object>;
DEFINE INDEX projected_vals ON projected FIELDS vals.*.x;
CREATE projected:a SET vals = <set>[{ x: 1 }, { x: 2 }] RETURN NONE;
CREATE projected:e SET vals = <set>[] RETURN NONE;
RETURN (SELECT id FROM projected WHERE vals.x CONTAINSANY [1] ORDER BY id) =
    (SELECT id FROM projected WITH NOINDEX WHERE vals.x CONTAINSANY [1] ORDER BY id);
RETURN (SELECT count() FROM projected WHERE vals.x CONTAINS 1 GROUP ALL) =
    (SELECT count() FROM projected WITH NOINDEX WHERE vals.x CONTAINS 1 GROUP ALL);
RETURN (SELECT id FROM projected WHERE vals.*.x CONTAINS 1 ORDER BY id) =
    (SELECT id FROM projected WITH NOINDEX WHERE vals.*.x CONTAINS 1 ORDER BY id);
