/**
[env]
namespace = true
database = true

[test]
reason = "Applying .* to NONE/NULL/scalar values must return NONE, not [VALUE]. Previously the streaming planner's `evaluate_all` wrapped non-collection values in a single-element array, which broke client SDKs that expected to decode into a struct."
issue = 7143

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
value = "{ a: 1, b: 2 }"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 1 }"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: bug_detail:d1, text: 'Detail A', value: 100 }]"

[[test.results]]
value = "[{ id: bug_data:nodetail }]"

[[test.results]]
value = "[{ data: bug_data:nodetail, direct: bug_detail:d1, id: bug_parent:p1 }]"

[[test.results]]
value = "[{ direct: { id: bug_detail:d1, text: 'Detail A', value: 100 } }]"

[[test.results]]
value = "[{ via_data: NONE }]"

[[test.results]]
value = "[{ direct_none: NONE }]"

[[test.results]]
value = "[1, { id: bug_detail:d1, text: 'Detail A', value: 100 }, 'x']"
*/

-- Scalars and NONE/NULL should produce NONE under .*
RETURN NONE.*;
RETURN NULL.*;
RETURN 1.*;
RETURN "foo".*;
RETURN true.*;

-- A field that is NONE accessed via path then .*
RETURN ({ a: NONE }.a).*;

-- Objects and arrays pass through
RETURN { a: 1, b: 2 }.*;
RETURN [1, 2].*;

-- IF subquery returning NONE / object, then .*
RETURN (IF false THEN { a: 1 } ELSE NONE END).*;
RETURN (IF true THEN { a: 1 } ELSE NONE END).*;

-- The schema-driven case from the original issue: option<record> set to NONE
DEFINE TABLE bug_detail SCHEMAFULL TYPE NORMAL;
DEFINE FIELD text ON bug_detail TYPE string;
DEFINE FIELD value ON bug_detail TYPE int;
DEFINE TABLE bug_data SCHEMAFULL TYPE NORMAL;
DEFINE FIELD detail ON bug_data TYPE option<record<bug_detail>>;
DEFINE TABLE bug_parent SCHEMAFULL TYPE NORMAL;
DEFINE FIELD data ON bug_parent TYPE record<bug_data>;
DEFINE FIELD detail ON bug_parent TYPE option<record<bug_detail>>;
DEFINE FIELD direct ON bug_parent TYPE record<bug_detail>;

CREATE bug_detail:d1 SET text = "Detail A", value = 100;
CREATE bug_data:nodetail SET detail = NONE;
CREATE bug_parent:p1 SET data = bug_data:nodetail, detail = NONE, direct = bug_detail:d1;

-- direct.* on a present record should return the fetched object
SELECT direct.* AS direct FROM bug_parent:p1;

-- via path to NONE: data.detail.* must be NONE, not [NONE]
SELECT data.detail.* AS via_data FROM bug_parent:p1;

-- direct .* on a NONE field must be NONE, not [NONE]
SELECT detail.* AS direct_none FROM bug_parent:p1;

-- Mixed array of records and non-records: records get fetched, non-records pass through.
RETURN [1, bug_detail:d1, "x"].*;
