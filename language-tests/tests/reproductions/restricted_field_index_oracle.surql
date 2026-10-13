/**
[env]
imports = ["reproductions/restricted_field_index_oracle_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "An index whose columns carry a non-`Full` SELECT permission must not be selected for a query. Every scan built from such a candidate answers its predicate from index entries and drops the covered condition, so the document-level field reduction never runs on the data the decision was made from — result membership then reports which records contain a guessed value or search term, and for HNSW which restricted vectors lie nearest a supplied one. The legacy planner already refused these indexes; this pins the same rule for the streaming planner across full-text, single-column and compound B-tree, and HNSW, while a readable-column index stays usable."

# Control: the restricted field is absent from a direct read, which is the
# boundary the index paths must not go around.
[[test.results]]
value = "[{ id: articles:one }]"

# Full-text over an unreadable column. `MATCHES` can only be computed by a
# full-text index, so withholding the index leaves the query unservable and it
# is refused. That is fail-closed and is what the legacy planner does with the
# same schema: the index is skipped, so no index supports the operator.
[[test.results]]
error = "There was no suitable index supporting the expression: content @@ 'hunter2'"

# A simple equality on the restricted field retains its predicate for
# re-evaluation against the reduced document, so this was already correct and
# must stay so.
[[test.results]]
value = "[]"

# Compound: neither the single-column `idx_secret` nor the compound
# `idx_cat_secret` may answer the restricted leaf from index entries while the
# readable leaf carries the row through.
[[test.results]]
value = "[]"

# Control (guard scope): `idx_category` covers only a readable column, so it is
# still selected and still returns both rows. Withholding is scoped to the
# columns the actor may not select, not applied to every index on the table.
[[test.results]]
value = "[{ category: 'a', id: docs:one }, { category: 'a', id: docs:two }]"

# HNSW over an unreadable vector column must not report nearest neighbours.
[[test.results]]
value = "[]"

# Control (no index involved): a record-id lookup on the table carrying the
# withheld full-text index still resolves, so the table itself is not made
# unqueryable by the guard.
[[test.results]]
value = "[{ id: articles:one }, { id: articles:three }]"

*/

SELECT * FROM articles:one;
SELECT id FROM articles WHERE content @@ 'hunter2';
SELECT id FROM docs WHERE secret = 'S3CRET_TOKEN_VALUE';
SELECT id FROM docs WHERE secret = 'S3CRET_TOKEN_VALUE' AND category = 'a';
SELECT category, id FROM docs WHERE category = 'a' ORDER BY id;
SELECT id, label FROM vectors WHERE embedding <|2,50|> [0f, 0f];
SELECT id FROM articles WHERE id IN [articles:one, articles:three] ORDER BY id;
