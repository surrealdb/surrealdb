/**
[test]
run = false
reason = "Setup only: left tables of 120 and 50 rows, a right table indexed on its key with a computed field that casts its string to an int, and a right table indexed on its key with no computed field whose string a cast to int takes for every row but one that no left key reaches, all for join_lookup_errors.pgsql and join_lookup_writes.pgsql."
*/

DEFINE TABLE a SCHEMALESS;
DEFINE TABLE f SCHEMALESS;
DEFINE TABLE e SCHEMALESS;
DEFINE INDEX e_k ON e FIELDS k;
DEFINE TABLE h SCHEMALESS;
DEFINE INDEX h_k ON h FIELDS k;
DEFINE FIELD t ON e COMPUTED <int> s;

FOR $i IN 0..120 { CREATE type::record('a', $i) SET n = $i, k = $i RETURN NONE; };
FOR $i IN 0..50 { CREATE type::record('f', $i) SET n = $i, k = $i RETURN NONE; };
FOR $i IN 0..200 { CREATE type::record('e', $i) SET k = $i, s = $i + 1 RETURN NONE; };
CREATE e:bad SET k = 5000, s = 'x' RETURN NONE;
FOR $i IN 0..200 { CREATE type::record('h', $i) SET k = $i, s = $i + 1 RETURN NONE; };
CREATE h:bad SET k = 5000, s = 'x' RETURN NONE;
