/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 10000

[env.capabilities]
allow-experimental = ["files"]

[test]
reason = "ALTER resource names and table targets accept bound parameters (parity with DEFINE/REMOVE)"

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
error = "Expected `user name` but found `NONE`"
*/

-- USER
LET $prm_user = 'prm_user_a1';
DEFINE USER $prm_user ON DATABASE PASSWORD 'pw1' ROLES VIEWER;
ALTER USER $prm_user ON DATABASE PASSWORD 'pw2' ROLES EDITOR COMMENT "prm-user-alt";
REMOVE USER $prm_user ON DATABASE;

-- ACCESS
LET $prm_access = 'prm_access_a1';
DEFINE ACCESS $prm_access ON DATABASE TYPE JWT
	ALGORITHM HS256 KEY 'secret'
	DURATION FOR TOKEN 1h, FOR SESSION 24h;
ALTER ACCESS $prm_access ON DATABASE COMMENT "prm-access-alt";
REMOVE ACCESS $prm_access ON DATABASE;

-- TABLE
LET $prm_table = 'prm_table_a1';
DEFINE TABLE $prm_table;
ALTER TABLE $prm_table COMMENT 'prm-table-alt' TYPE NORMAL SCHEMALESS;
REMOVE TABLE $prm_table;

-- EVENT + table target
LET $prm_evt = 'prm_evt_a1';
LET $prm_evt_tb = 'prm_evt_tb_a1';
DEFINE TABLE $prm_evt_tb;
DEFINE EVENT $prm_evt ON $prm_evt_tb WHEN $event = "CREATE" THEN { RETURN 1; };
ALTER EVENT $prm_evt ON $prm_evt_tb COMMENT "prm-event-alt";
REMOVE EVENT $prm_evt ON $prm_evt_tb;
REMOVE TABLE $prm_evt_tb;

-- INDEX + table target
LET $prm_idx = 'prm_idx_a1';
LET $prm_idx_tb = 'prm_idx_tb_a1';
DEFINE TABLE $prm_idx_tb;
DEFINE INDEX $prm_idx ON $prm_idx_tb COUNT;
ALTER INDEX $prm_idx ON $prm_idx_tb COMMENT "prm-index-alt";
REMOVE INDEX $prm_idx ON $prm_idx_tb;
REMOVE TABLE $prm_idx_tb;

-- FIELD + table target
LET $prm_fld = 'prm_fld_a1';
LET $prm_fld_tb = 'prm_fld_tb_a1';
DEFINE TABLE $prm_fld_tb;
DEFINE FIELD $prm_fld ON $prm_fld_tb;
ALTER FIELD $prm_fld ON $prm_fld_tb COMMENT "prm-field-alt";
REMOVE FIELD $prm_fld ON $prm_fld_tb;
REMOVE TABLE $prm_fld_tb;

-- ANALYZER
LET $prm_an = 'prm_an_a1';
DEFINE ANALYZER $prm_an TOKENIZERS blank;
ALTER ANALYZER $prm_an COMMENT "prm-analyzer-alt";
REMOVE ANALYZER $prm_an;

-- SEQUENCE
LET $prm_seq = 'prm_seq_a1';
DEFINE SEQUENCE $prm_seq;
ALTER SEQUENCE $prm_seq TIMEOUT 5s;
REMOVE SEQUENCE $prm_seq;

-- BUCKET
LET $prm_bkt = 'prm_bkt_a1';
DEFINE BUCKET $prm_bkt BACKEND 'memory';
ALTER BUCKET $prm_bkt READONLY COMMENT "prm-bucket-alt";
REMOVE BUCKET $prm_bkt;

-- Negative: unbound parameter in ALTER USER name
ALTER USER $prm_missing_user ON DATABASE COMMENT "should fail";
