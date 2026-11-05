-- Transactions opened after the upgrade over legacy rows. A cancelled block leaves the legacy
-- rows as they were. In a transaction its caller commits, a statement that fails keeps the
-- writes made before it, and a raise of the level in it takes effect with its commit. A
-- migration in a transaction is judged by that transaction. The indexed field must answer as a
-- scan does afterwards.
DEFINE TABLE ledger SCHEMAFULL;
DEFINE FIELD amount ON ledger TYPE int;
DEFINE FIELD memo ON ledger TYPE option<string>;
DEFINE INDEX l_amount ON ledger FIELDS amount;
CREATE ledger:1 SET amount = 10, memo = 'a';
CREATE ledger:2 SET amount = 20;
ALTER FIELD amount ON ledger TYPE string;
CREATE ledger:3 SET amount = 'thirty', memo = 'c';
-- @upgrade
SELECT * FROM ledger ORDER BY id;
BEGIN;
UPDATE ledger:3 SET memo = 'cancelled';
CANCEL;
SELECT * FROM ledger ORDER BY id;
-- @client
UPDATE ledger:3 SET memo = 'in txn';
ALTER DATABASE test COMPATIBILITY 4;
ALTER FIELD amount ON ledger TYPE int;
UPDATE ledger:3 SET memo = 'after failure';
-- @end
SELECT * FROM ledger ORDER BY id;
(INFO FOR DB).compatibility;
-- @drain
SELECT * FROM ledger ORDER BY id;
-- @check
REMOVE INDEX l_amount ON ledger;
-- @client
ALTER FIELD amount ON ledger TYPE int MIGRATE EXISTING ROWS AS amt_i USING <int>$value FALLBACK -1;
CREATE ledger:4 SET amount = 4, memo = 'd';
UPDATE ledger:3 SET amount = 'x';
-- @end
SELECT * FROM ledger ORDER BY id;
-- @drain
DEFINE INDEX l_amount ON ledger FIELDS amount;
SELECT * FROM ledger ORDER BY id;
SELECT VALUE id FROM ledger WHERE amount = -1 ORDER BY id;
SELECT VALUE id FROM ledger WHERE amount > 0 ORDER BY id;
(INFO FOR TABLE ledger).migrations;
-- @check
