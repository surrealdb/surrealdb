/**
[env]
imports = ["reproductions/knn_query_vector_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
A KNN query vector reading a table a record user reads through its
`PERMISSIONS FOR select` predicate, for an HNSW index and for brute force.

A SELECT of the vector judges the predicate per protected row, with that row
as its document, as its own WHERE condition is: a `[WHERE …]` or a lookup in
it binds `$parent` to the protected row, and `$this` is that row, so the
vector does not read the record being scanned and is evaluated once. A bare
`$parent` in the predicate is the SELECT's enclosing row, the record being
scanned, and fails planning.

A record fetched by id (`record::exists`, a record link's field) is judged in
a frame of its own, where an unbound `$parent` is the fetched record: the
answer is the same for every row of a statement, as the per-row answers show,
so the vector reading it is evaluated once, at the top of the vector and
inside a SELECT of it alike.
"""

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    PermissionFilter[$parent.x = 1]
      Source[enclosing]
)[0]"""

[[test.results]]
error = """
Invalid query: The KNN query vector cannot depend on the record being scanned: (
  Project[VALUE v]
    PermissionFilter[$parent.x = 1]
      Source[enclosing]
)[0]"""

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "[true, true]"

[[test.results]]
value = "[[5f, 0f], [5f, 0f]]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"

[[test.results]]
value = "[pts:5]"

[[test.results]]
value = "[bpts:5]"
*/

SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM filtered)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM filtered)[0];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM looked_up)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM looked_up)[0];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM own_row)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM own_row)[0];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM plain)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM plain)[0];
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE v FROM enclosing)[0];
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE v FROM enclosing)[0];
-- a record fetched by id, judged per row of `t`, at the top and inside a statement
SELECT VALUE record::exists(enclosing:one) FROM t ORDER BY id;
SELECT VALUE (SELECT VALUE record::exists(enclosing:one) FROM ONLY 1) FROM t ORDER BY id;
SELECT VALUE (SELECT VALUE enclosing:one.v FROM ONLY 1) FROM t ORDER BY id;
-- the same reads in a KNN query vector: evaluated once
SELECT VALUE id FROM pts WHERE point <|1,40|> IF record::exists(enclosing:one) THEN [5f, 0f] ELSE [1f, 0f] END;
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> IF record::exists(enclosing:one) THEN [5f, 0f] ELSE [1f, 0f] END;
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE IF record::exists(enclosing:one) THEN [5f, 0f] ELSE [1f, 0f] END FROM ONLY 1);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> (SELECT VALUE IF record::exists(enclosing:one) THEN [5f, 0f] ELSE [1f, 0f] END FROM ONLY 1);
SELECT VALUE id FROM pts WHERE point <|1,40|> (SELECT VALUE enclosing:one.v FROM ONLY 1);
SELECT VALUE id FROM bpts WHERE point <|1,EUCLIDEAN|> enclosing:one.v;
