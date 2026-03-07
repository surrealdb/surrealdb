/**
[test]
reason = "Shared setup for DML table permission tests: creates tables with NONE/FULL/WHERE permissions and test data"

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
value = "[{ id: select_none:1, owner: user:1, val: 'a' }]"

[[test.results]]
value = "[{ id: select_none:2, owner: user:2, val: 'b' }]"

[[test.results]]
value = "[{ id: select_full:1, owner: user:1, val: 'a' }]"

[[test.results]]
value = "[{ id: select_full:2, owner: user:2, val: 'b' }]"

[[test.results]]
value = "[{ id: select_where:1, owner: user:1, val: 'a' }]"

[[test.results]]
value = "[{ id: select_where:2, owner: user:2, val: 'b' }]"

[[test.results]]
value = "[{ id: update_none:1, owner: user:1, val: 'orig' }]"

[[test.results]]
value = "[{ id: update_full:1, owner: user:1, val: 'orig' }]"

[[test.results]]
value = "[{ id: update_where:1, owner: user:1, val: 'orig' }]"

[[test.results]]
value = "[{ id: update_where:2, owner: user:2, val: 'orig' }]"

[[test.results]]
value = "[{ id: delete_none:1, owner: user:1 }]"

[[test.results]]
value = "[{ id: delete_full:1, owner: user:1 }]"

[[test.results]]
value = "[{ id: delete_where:1, owner: user:1 }]"

[[test.results]]
value = "[{ id: delete_where:2, owner: user:2 }]"

[[test.results]]
value = "[{ id: user:1 }]"

[[test.results]]
value = "[{ id: user:2 }]"

*/
DEFINE ACCESS test ON DATABASE TYPE RECORD SIGNUP (CREATE type::record('user', $id)) SIGNIN (SELECT * FROM type::record('user', $id));

-- Tables with varying SELECT permissions
DEFINE TABLE select_none PERMISSIONS FOR select NONE, FOR create, update, delete FULL;
DEFINE TABLE select_full PERMISSIONS FOR select FULL, FOR create, update, delete FULL;
DEFINE TABLE select_where PERMISSIONS FOR select WHERE owner = $auth, FOR create, update, delete FULL;

-- Tables with varying CREATE permissions
DEFINE TABLE create_none PERMISSIONS FOR create NONE, FOR select, update, delete FULL;
DEFINE TABLE create_full PERMISSIONS FOR create FULL, FOR select, update, delete FULL;
DEFINE TABLE create_where PERMISSIONS FOR create WHERE owner = $auth, FOR select, update, delete FULL;

-- Tables with varying UPDATE permissions
DEFINE TABLE update_none PERMISSIONS FOR update NONE, FOR select, create, delete FULL;
DEFINE TABLE update_full PERMISSIONS FOR update FULL, FOR select, create, delete FULL;
DEFINE TABLE update_where PERMISSIONS FOR update WHERE owner = $auth, FOR select, create, delete FULL;

-- Tables with varying DELETE permissions
DEFINE TABLE delete_none PERMISSIONS FOR delete NONE, FOR select, create, update FULL;
DEFINE TABLE delete_full PERMISSIONS FOR delete FULL, FOR select, create, update FULL;
DEFINE TABLE delete_where PERMISSIONS FOR delete WHERE owner = $auth, FOR select, create, update FULL;

-- Relation table for RELATE tests
DEFINE TABLE rel TYPE RELATION PERMISSIONS FOR select FULL, FOR create NONE;
DEFINE TABLE rel_full TYPE RELATION PERMISSIONS FOR select FULL, FOR create FULL;
DEFINE TABLE rel_where TYPE RELATION PERMISSIONS FOR select FULL, FOR create WHERE in = $auth;

-- Seed data owned by user:1
CREATE select_none:1 SET val = 'a', owner = user:1;
CREATE select_none:2 SET val = 'b', owner = user:2;
CREATE select_full:1 SET val = 'a', owner = user:1;
CREATE select_full:2 SET val = 'b', owner = user:2;
CREATE select_where:1 SET val = 'a', owner = user:1;
CREATE select_where:2 SET val = 'b', owner = user:2;

CREATE update_none:1 SET val = 'orig', owner = user:1;
CREATE update_full:1 SET val = 'orig', owner = user:1;
CREATE update_where:1 SET val = 'orig', owner = user:1;
CREATE update_where:2 SET val = 'orig', owner = user:2;

CREATE delete_none:1 SET owner = user:1;
CREATE delete_full:1 SET owner = user:1;
CREATE delete_where:1 SET owner = user:1;
CREATE delete_where:2 SET owner = user:2;

-- Nodes for RELATE tests
CREATE user:1;
CREATE user:2;
