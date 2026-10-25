/**
[env]
namespace = true
database = true

[test]
reason = """
A block statement after a schema statement whose effect the block cannot
know ahead (here, one under IF) is planned for the catalog its transaction
reads when the block reaches it: a write enters the index defined earlier
in the transaction, and a read takes the KNN, full-text and B-tree access
path that index offers, fails over a removed full-text index, and honours
WITH INDEX and WITH NOINDEX against the indexes it finds. EXPLAIN renders
the access path chosen there.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ dist: 2f, id: pts:1 }, { dist: 4f, id: pts:2 }]"

[[test.results]]
value = "[{ id: doc:1 }]"

[[test.results]]
error = "There was no suitable index supporting the expression: text @@ 'hello'"

[[test.results]]
value = "[{ id: n:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: n, index: iv, access: >0, direction: Forward, limit: 2]
''''

[[test.results]]
value = "[{ id: n:3 }, { id: n:1 }]"

[[test.results]]
value = "[{ id: n:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    IndexScan [ctx: Db] [table: n, index: uv, access: = 2, direction: Forward]
''''

*/

LET $go = true;
{
	IF $go { DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12 };
	CREATE pts:1 SET point = [1,2,3,4];
	CREATE pts:2 SET point = [4,5,6,7];
	SELECT id, vector::distance::knn() AS dist FROM pts WHERE point <|2,100|> [2,3,4,5];
};
{
	IF $go {
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase;
		DEFINE INDEX ft ON doc FIELDS text FULLTEXT ANALYZER simple BM25;
	};
	CREATE doc:1 SET text = 'hello world';
	CREATE doc:2 SET text = 'goodbye';
	SELECT id FROM doc WHERE text @@ 'hello';
};
{
	IF $go { REMOVE INDEX ft ON doc };
	SELECT id FROM doc WHERE text @@ 'hello';
};
{
	-- The index is defined before the block's writes, so each of them enters
	-- it. A block that writes plans every column as one that may hold an array,
	-- whatever `v` is declared, so it is an equality that takes the index here:
	-- an open range reaching the place arrays sort reads the table instead.
	DEFINE FIELD v ON n TYPE option<int>;
	IF $go { DEFINE INDEX iv ON n FIELDS v };
	CREATE n:1 SET v = 3; CREATE n:2 SET v = 1; CREATE n:3 SET v = 2;
	SELECT id FROM n WHERE v = 3 LIMIT 2;
};
{
	IF $go { DEFINE INDEX iv2 ON n FIELDS w };
	EXPLAIN SELECT id FROM n WHERE v > 0 ORDER BY v LIMIT 2;
};
{
	IF $go { DEFINE INDEX iv3 ON n FIELDS w };
	SELECT id FROM n WITH INDEX iv WHERE v > 1;
};
{
	IF $go { DEFINE INDEX uv ON n FIELDS v UNIQUE };
	SELECT id FROM n WITH NOINDEX WHERE v = 2;
};
{
	IF $go { DEFINE INDEX uv2 ON n FIELDS w };
	EXPLAIN SELECT id FROM n WHERE v = 2;
};
