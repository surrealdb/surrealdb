/**
[test]
reason = """
	A filtered HNSW search keeps walking the graph until it holds `ef` admitted
	rows: a neighbour the filter rejects does not end it. `LM 0` puts every
	element on layer 0, so the graph is the same on every run: points 1..30 on
	a line, each linked to its neighbours. The filter rejects 1..20 and 22, so
	the walk reaches 23 and 24 only through the rejected 22.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t:21, t:23, t:24]"

[[test.results]]
value = "[t:21, t:22, t:23]"

*/

FOR $i IN 1..=30 { CREATE type::record('t', $i) SET emb = [<float> $i], n = $i };
DEFINE INDEX t_emb ON t FIELDS emb HNSW DIMENSION 1 DIST EUCLIDEAN TYPE F32 LM 0;
SELECT VALUE id FROM t WHERE emb <|3,40|> [0f] AND n > 20 AND n != 22 ORDER BY id;
SELECT VALUE id FROM t WHERE emb <|3,40|> [0f] AND n > 20 ORDER BY id;
