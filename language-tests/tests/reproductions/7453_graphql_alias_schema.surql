/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Schema fixture for the #7453 GRAPHQL_ALIAS reproductions"
*/

DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS NONE;

-- No table alias here: this table isolates the *field* alias behaviour, so that
-- the nested-field reproduction stays independent of the table-alias one.
DEFINE TABLE widget SCHEMAFULL;
-- Control: a top-level field alias is honoured today.
DEFINE FIELD created_at ON widget TYPE datetime GRAPHQL_ALIAS 'createdAt';
DEFINE FIELD price ON widget TYPE object;
-- Subject: an alias on a nested field must be honoured the same way.
DEFINE FIELD price.in_euro ON widget TYPE float GRAPHQL_ALIAS 'inEuro';
DEFINE FIELD price.currency ON widget TYPE string;

-- A table alias, exercised on its own table so the two reproductions do not
-- interfere.
DEFINE TABLE gadget SCHEMAFULL GRAPHQL_ALIAS 'Gadget';
DEFINE FIELD label ON gadget TYPE string;

-- Record links and relation traversals point at the aliased table, so they must
-- reference its aliased Object type rather than the raw table name.
DEFINE TABLE holder SCHEMAFULL;
DEFINE FIELD item ON holder TYPE record<gadget>;
DEFINE TABLE uses TYPE RELATION FROM holder TO gadget SCHEMAFULL;

CREATE widget:one SET created_at = d"2024-01-15T10:00:00Z", price = { in_euro: 9.5f, currency: "EUR" };
CREATE gadget:one SET label = "First";
CREATE holder:h SET item = gadget:one;
RELATE holder:h->uses->gadget:one;
