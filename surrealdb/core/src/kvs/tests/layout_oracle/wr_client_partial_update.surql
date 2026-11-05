-- A multi-row UPDATE that fails on its third row inside a client-owned transaction,
-- with no migration anywhere: its writes to the first two rows refuse the transaction's
-- commit, so no row keeps the write.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD n ON p TYPE int ASSERT $value < 30;
CREATE p:1 SET n = 1;
CREATE p:2 SET n = 2;
CREATE p:3 SET n = 25;
-- @client
UPDATE p SET n += 10;
-- @end
SELECT VALUE [record::id(id), n] FROM p ORDER BY id;
