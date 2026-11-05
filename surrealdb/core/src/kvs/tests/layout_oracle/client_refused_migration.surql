-- A transaction its caller owns keeps the writes of a statement that failed in
-- it. A migration a check of its definition refuses leaves no step there for
-- the caller to commit: every row reads as its field declares.
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD f ON tb TYPE string;
DEFINE FIELD labels ON tb TYPE set<string>;
CREATE tb:1 SET f = '1', labels = <set>['2'];
CREATE tb:2 SET f = 'x', labels = <set>['3', '4'];
-- @drain
-- @client
CREATE tb:4 SET f = '4', labels = <set>['6'];
ALTER FIELD f ON tb FLEXIBLE TYPE int MIGRATE EXISTING ROWS AS flex USING <int>$value FALLBACK 0;
-- @client
ALTER FIELD labels.* ON tb TYPE int MIGRATE EXISTING ROWS AS ints USING <int>$value FALLBACK 0;
-- @client
CREATE tb:3 SET f = '3', labels = <set>['5'];
-- @end
SELECT * FROM tb ORDER BY id;
SELECT VALUE type::is_string(f) FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
-- The name a refused migration gave is free.
ALTER FIELD f ON tb TYPE int MIGRATE EXISTING ROWS AS flex USING <int>$value FALLBACK 0;
SELECT * FROM tb ORDER BY id;
SELECT VALUE type::is_string(f) FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
-- @drain
SELECT * FROM tb ORDER BY id;
(INFO FOR TABLE tb).migrations;
-- @check
