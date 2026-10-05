/**
[test]
reason = """
Setup for 7539_restricted_array_elements_stay_out_of_index_answers: an
`array<string>` field whose elements (`acl.*`) are SELECT-denied, indexed on
the bare column (`d`) and on its elements (`e`), plus a record user.
"""
run = false
*/

DEFINE TABLE d PERMISSIONS FULL;
DEFINE FIELD acl ON d TYPE array<string>;
DEFINE FIELD OVERWRITE acl.* ON d TYPE string PERMISSIONS FOR SELECT NONE;
DEFINE INDEX d_acl ON d FIELDS acl;
CREATE d:1 SET acl = ['secret', 'x'];
CREATE d:2 SET acl = ['other'];

DEFINE TABLE e PERMISSIONS FULL;
DEFINE FIELD acl ON e TYPE array<string>;
DEFINE FIELD OVERWRITE acl.* ON e TYPE string PERMISSIONS FOR SELECT NONE;
DEFINE INDEX e_acl ON e FIELDS acl.*;
CREATE e:1 SET acl = ['secret', 'x'];
CREATE e:2 SET acl = ['other'];

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
DEFINE TABLE user PERMISSIONS FULL;
CREATE user:1;
