BEGIN TRANSACTION;
-- Setup accounts
CREATE account:one SET balance = 135,605.16;
CREATE account:two SET balance = 91,031.31;
-- Move money
UPDATE account:one SET balance += 300.00;
UPDATE account:two SET balance -= 300.00;
-- Rollback all changes
CANCEL TRANSACTION;
