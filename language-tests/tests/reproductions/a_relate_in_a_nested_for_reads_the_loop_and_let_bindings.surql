/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "RELATE, CREATE and UPDATE inside nested FOR loops that follow a schema statement in their block read the outer loop variable and the LET bindings of either loop body"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[[person:1, person:2], [person:1, person:3], [person:2, person:2], [person:2, person:3], [person:3, person:2], [person:3, person:3], [person:4, person:2], [person:4, person:3], [person:5, person:2], [person:5, person:3]]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[[member:1, member:2, 1], [member:1, member:2, 2], [member:2, member:3, 1], [member:2, member:3, 2], [member:3, member:4, 1], [member:3, member:4, 2], [member:4, member:5, 1], [member:4, member:5, 2], [member:5, member:1, 1], [member:5, member:1, 2]]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[[tag:11, owner:1, 1], [tag:12, owner:1, 2], [tag:21, owner:2, 1], [tag:22, owner:2, 2], [tag:31, owner:3, 1], [tag:32, owner:3, 2], [tag:41, owner:4, 1], [tag:42, owner:4, 2], [tag:51, owner:5, 1], [tag:52, owner:5, 2]]"

[[test.results]]
value = "[[owner:1, 12], [owner:2, 22], [owner:3, 32], [owner:4, 42], [owner:5, 52]]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[[city:1, city:2], [city:1, city:2], [city:2, city:3], [city:2, city:3], [city:3, city:4], [city:3, city:4], [city:4, city:1], [city:4, city:1]]"

[[test.results]]
value = "[10, 10, 10, 8]"
*/

-- RELATE inside a nested FOR reading the outer FOR variable and an inner LET
{
  DEFINE TABLE knows SCHEMALESS;
  CREATE |person:1..=5| RETURN NONE;
  FOR $p IN (SELECT VALUE id FROM person ORDER BY id) {
    FOR $i IN array::range(1, 3) {
      LET $t = type::record('person', ($i % 5) + 1);
      RELATE $p->knows->$t;
    };
  };
  RETURN 'OK';
};
SELECT VALUE [in, out] FROM knows ORDER BY in, out;

-- The same with the LET in the outer loop body
{
  DEFINE TABLE likes SCHEMALESS;
  CREATE |member:1..=5| RETURN NONE;
  FOR $p IN (SELECT VALUE id FROM member ORDER BY id) {
    LET $t = type::record('member', (record::id($p) % 5) + 1);
    FOR $i IN array::range(1, 3) {
      RELATE $p->likes->$t SET n = $i;
    };
  };
  RETURN 'OK';
};
SELECT VALUE [in, out, n] FROM likes ORDER BY in, n;

-- CREATE and UPDATE reading a LET bound in a nested loop
{
  DEFINE TABLE tag SCHEMALESS;
  CREATE |owner:1..=5| RETURN NONE;
  FOR $p IN (SELECT VALUE id FROM owner ORDER BY id) {
    FOR $i IN array::range(1, 3) {
      LET $k = record::id($p) * 10 + $i;
      CREATE type::record('tag', $k) SET owner = $p, n = $i;
      UPDATE $p SET last = $k;
    };
  };
  RETURN 'OK';
};
SELECT VALUE [id, owner, n] FROM tag ORDER BY id;
SELECT VALUE [id, last] FROM owner ORDER BY id;

-- The bulk-load shape: an unordered outer read and an inner loop variable the body never reads
{
  DEFINE TABLE city SCHEMALESS;
  CREATE |city:1..=4| SET name = 'c' RETURN NONE;
  FOR $c IN (SELECT VALUE id FROM city) {
    FOR $_ IN array::range(1, 3) {
      LET $t = type::record('city', (record::id($c) % 4) + 1);
      RELATE $c->road->$t;
    };
  };
  RETURN 'OK';
};
SELECT VALUE [in, out] FROM road ORDER BY in, out;

-- Edge counts
[count(SELECT * FROM knows), count(SELECT * FROM likes), count(SELECT * FROM tag), count(SELECT * FROM road)];
