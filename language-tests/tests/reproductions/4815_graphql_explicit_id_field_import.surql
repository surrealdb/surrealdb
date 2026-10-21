/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Setup for 4815_graphql_explicit_id_field: tables that declare their own id field, a record link to themselves and a geometry, exposed through GraphQL"
*/

DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS AUTO;

DEFINE TABLE product SCHEMAFULL;
DEFINE FIELD id ON product TYPE string;
DEFINE FIELD name ON product TYPE string;
DEFINE FIELD maker ON product TYPE option<record<product>>;
DEFINE FIELD loc ON product TYPE option<geometry<point>>;

CREATE product:a SET name = 'a', loc = (1.0, 2.0);
CREATE product:b SET name = 'b', maker = product:a;
