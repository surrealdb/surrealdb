/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Schema fixture for the #7453 GRAPHQL_ALIAS function-return reproduction: functions are exposed here, unlike in the other #7453 fixture"
*/

DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS AUTO;

DEFINE TABLE gadget SCHEMAFULL GRAPHQL_ALIAS 'Gadget';
DEFINE FIELD label ON gadget TYPE string;

DEFINE TABLE widget SCHEMAFULL GRAPHQL_ALIAS 'Widget';
DEFINE FIELD title ON widget TYPE string;

-- A bare `record` return is typed as the `record` interface, and a multi-target
-- one as the `Gadget_or_Widget` union: both need the resolver to tag the value
-- with the aliased Object type name before a fragment can match it.
DEFINE FUNCTION fn::any_record() -> record { RETURN gadget:one; };
DEFINE FUNCTION fn::pick_gadget() -> record<gadget | widget> { RETURN gadget:one; };
DEFINE FUNCTION fn::pick_widget() -> record<gadget | widget> { RETURN widget:one; };
-- Control: a single-target return already *is* the aliased Object type, so it
-- must keep resolving without a tag.
DEFINE FUNCTION fn::the_gadget() -> record<gadget> { RETURN gadget:one; };

CREATE gadget:one SET label = "First";
CREATE widget:one SET title = "Second";
