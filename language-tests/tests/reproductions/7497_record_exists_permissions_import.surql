/**
[test]
run = false
reason = "Setup for 7497_record_exists_respects_permissions: a visible and a hidden table, each with a computed field that errors if it is ever derived"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD DURATION FOR TOKEN 1h, FOR SESSION 12h;
DEFINE TABLE shown SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD boom ON shown COMPUTED <int>"not a number";
DEFINE FIELD secret ON shown TYPE option<int> PERMISSIONS FOR select WHERE boom = 1;
DEFINE TABLE hidden SCHEMAFULL PERMISSIONS FOR select NONE;
DEFINE FIELD boom ON hidden COMPUTED <int>"not a number";
DEFINE TABLE user SCHEMALESS;
DEFINE TABLE mine SCHEMAFULL PERMISSIONS FOR select WHERE owner = $auth.id;
DEFINE FIELD owner ON mine TYPE record<user>;
DEFINE FIELD boom ON mine COMPUTED <int>"not a number";
CREATE user:u, user:v RETURN NONE;
CREATE shown:1, hidden:1 RETURN NONE;
CREATE mine:1 SET owner = user:u RETURN NONE;
CREATE mine:2 SET owner = user:v RETURN NONE;
