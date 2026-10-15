-- Indexes over legacy rows. An index defined before the upgrade and indexes built after it
-- over the same rows must answer as a scan does, including numbers of different kinds that
-- compare equal. A unique index built after the upgrade must see the legacy keys: a new row
-- equal to a legacy one is refused, and an update or delete of a legacy row removes the entry
-- its old value made. A full-text index written before the upgrade must drop the tokens of a
-- legacy row's old text when that row is updated or deleted after the upgrade.
DEFINE TABLE acct SCHEMAFULL;
DEFINE FIELD code ON acct TYPE number;
DEFINE FIELD owner ON acct TYPE string;
DEFINE FIELD tier ON acct TYPE option<int>;
DEFINE FIELD note ON acct TYPE option<string>;
DEFINE INDEX by_code ON acct FIELDS code;
DEFINE INDEX by_owner_tier ON acct FIELDS owner, tier;
DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
DEFINE INDEX ft_note ON acct FIELDS note FULLTEXT ANALYZER simple BM25;
CREATE acct:1 SET code = -5, owner = 'ann', tier = 1;
CREATE acct:2 SET code = 0.5f, owner = 'bob', tier = 2;
CREATE acct:3 SET code = 5dec, owner = 'ann';
CREATE acct:4 SET code = 1000000000000, owner = 'cy', tier = 1;
CREATE acct:5 SET code = -0.25dec, owner = 'dee', tier = 1;
CREATE acct:6 SET code = 5, owner = 'ann', tier = 2;
UPDATE acct:1 SET note = 'red apple pie';
UPDATE acct:2 SET note = 'green pear';
UPDATE acct:3 SET note = 'red grape jam';
UPDATE acct:4 SET note = 'blue apple';
UPDATE acct:5 SET note = 'red';
UPDATE acct:6 SET note = 'yellow lemon pie';
-- @upgrade
SELECT VALUE id FROM acct WHERE code = 5 ORDER BY id;
SELECT VALUE id FROM acct WHERE code > -1 ORDER BY id;
SELECT VALUE id FROM acct WHERE code <= -0.25 ORDER BY id;
SELECT VALUE id FROM acct WHERE owner = 'ann' AND tier = 2 ORDER BY id;
SELECT VALUE id FROM acct WHERE owner = 'ann' ORDER BY id;
SELECT VALUE id FROM acct WHERE note @@ 'red' ORDER BY id;
SELECT VALUE id FROM acct WHERE note @@ 'apple' ORDER BY id;
SELECT VALUE id FROM acct WHERE note @@ 'pie' ORDER BY id;
SELECT VALUE id FROM acct WHERE string::contains(note ?? '', 'pie') ORDER BY id;
UPDATE acct:6 SET code = 42;
SELECT VALUE id FROM acct WHERE code = 5 ORDER BY id;
UPDATE acct:1 SET owner = 'cy', tier = 9;
SELECT VALUE id FROM acct WHERE owner = 'ann' AND tier = 1 ORDER BY id;
SELECT VALUE id FROM acct WHERE owner = 'cy' ORDER BY id;
UPDATE acct:1 SET note = 'purple plum';
SELECT VALUE id FROM acct WHERE note @@ 'apple' ORDER BY id;
SELECT VALUE id FROM acct WHERE note @@ 'pie' ORDER BY id;
SELECT VALUE id FROM acct WHERE note @@ 'plum' ORDER BY id;
DELETE acct:2;
SELECT VALUE id FROM acct WHERE code = 0.5 ORDER BY id;
DELETE acct:5;
SELECT VALUE id FROM acct WHERE note @@ 'red' ORDER BY id;
-- @check
DEFINE INDEX uniq_code ON acct FIELDS code UNIQUE;
CREATE acct:7 SET code = 42.0f, owner = 'eve';
CREATE acct:8 SET code = 5, owner = 'eve';
CREATE acct:9 SET code = 5.0dec, owner = 'eve';
SELECT * FROM acct ORDER BY id;
-- @drain
DEFINE INDEX by_tier ON acct FIELDS tier;
SELECT VALUE id FROM acct WHERE tier = 1 ORDER BY id;
UPDATE acct:4 SET tier = NONE;
SELECT VALUE id FROM acct WHERE tier = 1 ORDER BY id;
DEFINE INDEX uniq_owner ON acct FIELDS owner UNIQUE;
SELECT VALUE id FROM acct WHERE owner = 'ann' ORDER BY id;
-- @check
REMOVE INDEX by_owner_tier ON acct;
SELECT VALUE id FROM acct WHERE owner = 'ann' ORDER BY id;
DELETE acct:3;
SELECT VALUE id FROM acct WHERE code = 5 ORDER BY id;
CREATE acct:10 SET code = 5, owner = 'fay', note = 'red velvet';
SELECT VALUE id FROM acct WHERE code = 5 ORDER BY id;
SELECT VALUE id FROM acct WHERE note @@ 'red' ORDER BY id;
-- @drain
SELECT * FROM acct ORDER BY id;
SELECT VALUE id FROM acct WHERE note @@ 'red' ORDER BY id;
-- @check
