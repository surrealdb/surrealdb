/**
[test]
reason = "Setup for 7416_auth_field_permission_skips_computed_fields: a field permission over $auth next to a COMPUTED field that errors if it is ever evaluated"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD DURATION FOR TOKEN 1h, FOR SESSION 12h;
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE id = $auth.id;
DEFINE FIELD is_admin ON user TYPE bool DEFAULT false PERMISSIONS FOR select WHERE id = $auth.id;
DEFINE TABLE item SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD name ON item TYPE string;
DEFINE FIELD admin_note ON item TYPE option<string> PERMISSIONS FOR select WHERE $auth.is_admin = true;
DEFINE FIELD boom ON item COMPUTED <int>"not a number";
CREATE user:u SET is_admin = true;
CREATE item:1 SET name = 'a', admin_note = 'n' RETURN NONE;
CREATE item:2 SET name = 'b' RETURN NONE;
