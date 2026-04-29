/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "COUNT index returns stale results after DEFINE TABLE OVERWRITE + RELATE in stored function"
issue = 7090

# -- Setup + cycle 1
[[test.results]]
value = "NONE"

# -- count() via COUNT index after cycle 1
[[test.results]]
value = "[{ cnt: 2 }]"
# -- count(id) via full scan after cycle 1
[[test.results]]
value = "[{ cnt: 2 }]"

# -- Cycle 2
[[test.results]]
value = "NONE"

# -- count() via COUNT index after cycle 2
[[test.results]]
value = "[{ cnt: 2 }]"
# -- count(id) via full scan after cycle 2
[[test.results]]
value = "[{ cnt: 2 }]"

# -- Cycle 3
[[test.results]]
value = "NONE"

# -- count() via COUNT index after cycle 3
[[test.results]]
value = "[{ cnt: 2 }]"
# -- count(id) via full scan after cycle 3
[[test.results]]
value = "[{ cnt: 2 }]"
*/

-- Setup: define table with COUNT index, redefine with OVERWRITE without re-defining the
-- COUNT index, then populate cycle 1
{
  DEFINE TABLE OVERWRITE t SCHEMAFULL;
  DEFINE FIELD OVERWRITE val ON t TYPE string;
  DEFINE INDEX OVERWRITE idx_count ON t COUNT;
  DEFINE TABLE OVERWRITE edge SCHEMAFULL TYPE RELATION IN t OUT t;

  -- Redefine t without the COUNT index
  DEFINE TABLE OVERWRITE t SCHEMAFULL;
  DEFINE FIELD OVERWRITE val ON t TYPE string;

  DEFINE FUNCTION OVERWRITE fn::create_and_relate($src: record<t>) {
    LET $rec = (CREATE ONLY t SET val = 'from_fn');
    LET $id = $rec.id;
    RELATE $src->edge->$id;
  };

  CREATE t:a SET val = 'a';
  fn::create_and_relate(t:a);
  RETURN NONE;
};

SELECT count() AS cnt FROM t GROUP ALL;
SELECT count(id) AS cnt FROM t GROUP ALL;

-- Cycle 2: delete all, recreate
{
  DELETE t; DELETE edge;
  CREATE t:b SET val = 'b';
  fn::create_and_relate(t:b);
  RETURN NONE;
};

SELECT count() AS cnt FROM t GROUP ALL;
SELECT count(id) AS cnt FROM t GROUP ALL;

-- Cycle 3: delete all, recreate (drift should be cumulative)
{
  DELETE t; DELETE edge;
  CREATE t:c SET val = 'c';
  fn::create_and_relate(t:c);
  RETURN NONE;
};

SELECT count() AS cnt FROM t GROUP ALL;
SELECT count(id) AS cnt FROM t GROUP ALL;
