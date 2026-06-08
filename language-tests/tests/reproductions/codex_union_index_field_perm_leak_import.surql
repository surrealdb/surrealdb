/**
[test]
reason = "Setup: table with table SELECT FULL, a `secret_tags` array field denied by field-level SELECT permission, and an array-element index `secret_tags.*` so the planner's CONTAINSANY composite-array-index path is reachable. A second public `tags` array + index covers the sanity case to confirm the fix does not over-trigger."
run = false
*/

DEFINE TABLE item PERMISSIONS FOR select FULL;
DEFINE FIELD secret_tags ON TABLE item TYPE array<string> PERMISSIONS FOR SELECT NONE;
DEFINE FIELD tags ON TABLE item TYPE array<string>;
DEFINE INDEX secret_tags_idx ON TABLE item FIELDS secret_tags.*;
DEFINE INDEX tags_idx ON TABLE item FIELDS tags.*;

CREATE item:1 SET secret_tags = ['admin'], tags = ['public'];
CREATE item:2 SET secret_tags = ['user'], tags = ['public'];

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
DEFINE TABLE user PERMISSIONS FULL;
CREATE user:1;
