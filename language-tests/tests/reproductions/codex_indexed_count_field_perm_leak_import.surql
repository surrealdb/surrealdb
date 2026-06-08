/**
[test]
reason = "Setup: table with table SELECT FULL and a single field `private` with PERMISSIONS FOR SELECT NONE. Standard indexes on both `private` and `public` so the indexed COUNT fast path is reachable for both — the fix must disable it for the restricted field while leaving it intact for the public one."
run = false
*/

DEFINE TABLE data PERMISSIONS FULL;
DEFINE FIELD public ON data TYPE string;
DEFINE FIELD private ON data TYPE string PERMISSIONS FOR SELECT NONE;
DEFINE INDEX private_idx ON data FIELDS private;
DEFINE INDEX public_idx ON data FIELDS public;

CREATE data:1 SET public = "public", private = "secret";
CREATE data:2 SET public = "public", private = "other";

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
DEFINE TABLE user PERMISSIONS FULL;
CREATE user:1;
