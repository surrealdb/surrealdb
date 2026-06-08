/**
[env]
imports = ["reproductions/post_write_select_permission_cut_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "actors:writer" }

[test]
reason = "Write paths must apply table-level and field-level SELECT permissions to RETURN AFTER (and the default unspecified-RETURN behaviour for write statements), so a viewer with write-but-not-select access never observes the row they just wrote."

# 1: CREATE notes:1 — table FOR select NONE → empty output
[[test.results]]
value = "[]"

# 2: UPDATE notes:1 — same cut on update output
[[test.results]]
value = "[]"

# 3: UPSERT notes:2 — covers the create branch of UPSERT
[[test.results]]
value = "[]"

# 4: UPSERT notes:2 — covers the update branch of UPSERT (record now exists)
[[test.results]]
value = "[]"

# 5: INSERT INTO notes — covers the INSERT create path
[[test.results]]
value = "[]"

# 6: CREATE entries:1 — table SELECT FULL but field `body` has FOR select NONE.
# RETURN AFTER must echo back only fields the viewer can see (`id`, `label`).
[[test.results]]
value = "[{ id: entries:1, label: 'visible' }]"

# 7: UPDATE entries:1 — same field cut applies on update output
[[test.results]]
value = "[{ id: entries:1, label: 'still visible' }]"

# 8: UPDATE entries:1 RETURN AFTER explicit
[[test.results]]
value = "[{ id: entries:1, label: 'explicit return' }]"

# 9: RELATE actors:a -> links -> actors:b — table FOR select NONE on
# the relation, so the post-write cut applies to relate_create's output
# the same way it applies to create.rs.
[[test.results]]
value = "[]"

# 10: RELATE the same edge again. The edge id is unique per (in, id, out)
# triple, so this hits `relate_create` again with a different generated
# id; the cut still applies.
[[test.results]]
value = "[]"

*/

-- Table-level select NONE: every write path must cut output.
CREATE notes:1 SET body = 'secret_create';
UPDATE notes:1 SET body = 'secret_update';
UPSERT notes:2 SET body = 'secret_upsert_create';
UPSERT notes:2 SET body = 'secret_upsert_update';
INSERT INTO notes { id: notes:3, body: 'secret_insert' };

-- Field-level select NONE: only the cut field is hidden.
CREATE entries:1 SET body = 'secret_body', label = 'visible';
UPDATE entries:1 SET body = 'secret_body_2', label = 'still visible';
UPDATE entries:1 SET label = 'explicit return' RETURN AFTER;

-- RELATE with FOR select NONE on the relation table.
RELATE actors:a -> links -> actors:b SET note = 'first';
RELATE actors:a -> links -> actors:b SET note = 'second';
