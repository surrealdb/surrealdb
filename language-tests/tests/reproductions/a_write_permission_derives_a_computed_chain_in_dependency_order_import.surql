/**
[test]
reason = "Setup for a_write_permission_derives_a_computed_chain_in_dependency_order.surql."

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
value = "[{ allowed: false, blocked: true, closed: true, id: doc:closed, status: 'closed' }]"

[[test.results]]
value = "[{ allowed: true, blocked: false, closed: false, id: doc:open, status: 'open' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ allowed: false, blocked: true, closed: true, id: memo:closed, status: 'closed' }]"

[[test.results]]
value = "[{ allowed: true, blocked: false, closed: false, id: memo:open, status: 'open' }]"








*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNUP ( CREATE type::record('account', $id) )
    SIGNIN ( SELECT * FROM type::record('account', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- `allowed` reads `blocked`, which reads `closed`, which reads the stored
-- `status`. Each computed field sorts by name before the one it reads, so name
-- order derives `allowed` while `blocked` is still NONE, and `!NONE` is true.
-- Ordering only the predicate's direct dependency first is not enough either:
-- `blocked` would then be derived while `closed` is still NONE.
DEFINE TABLE doc SCHEMALESS
    PERMISSIONS FOR select FULL, FOR create, update, delete WHERE allowed;
DEFINE FIELD closed ON doc COMPUTED status = 'closed';
DEFINE FIELD blocked ON doc COMPUTED closed;
DEFINE FIELD allowed ON doc COMPUTED !blocked;

CREATE doc:closed SET status = 'closed';
CREATE doc:open SET status = 'open';

-- The same chain behind a predicate that reads a parameter. Its dependencies
-- cannot be resolved statically, so the gate derives every computed field
-- rather than the ones the predicate names, and has to order all of them.
DEFINE TABLE memo SCHEMALESS
    PERMISSIONS FOR select FULL, FOR create, update, delete WHERE allowed AND $auth != NONE;
DEFINE FIELD closed ON memo COMPUTED status = 'closed';
DEFINE FIELD blocked ON memo COMPUTED closed;
DEFINE FIELD allowed ON memo COMPUTED !blocked;

CREATE memo:closed SET status = 'closed';
CREATE memo:open SET status = 'open';
