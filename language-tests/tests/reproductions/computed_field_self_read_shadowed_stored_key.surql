/**
[env]
imports = ["reproductions/computed_field_self_read_shadowed_stored_key_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }
# The legacy path answers the self-read with the computed value rather than the
# stored one, so it never reaches the shadowed key and returns `COMPUTED_VAL`
# here. The streaming paths are the ones that resolve the read against storage,
# so they are the ones this asserts.
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "A key stored under a name the table later defines as COMPUTED stays in the record's bytes, and a self-dereference reads storage directly. The stored permission pass classifies that name as computed and so reports nothing cut, which left the sanitised view unbuilt and the self-read serving the shadowed value. The view is built whenever a computed name shadows a stored key, not only when a stored field was cut."

# `hidden` is denied, so it is absent. `viaid` reaches it as `id.hidden`; the
# view drops computed names, so the read yields NONE rather than recovering
# `STALE_PHYSICAL` from the record's bytes.
[[test.results]]
value = "[{ id: stale:one, note: 'NOTE', viaid: NONE }]"

# The same through a WHERE predicate rather than the record-id fast path.
[[test.results]]
value = "[{ id: stale:one, note: 'NOTE', viaid: NONE }]"

# And through an explicit projection of just the readable computed field.
[[test.results]]
value = "[{ viaid: NONE }]"

# The table restricts no field, so the permission passes return early. The
# shadowed key must still not be the answer to `id.hidden`: `hidden` reads as
# the redacting computed value, and the self-read as NONE rather than as the
# secret the redaction replaced.
[[test.results]]
value = "[{ hidden: 'REDACTED', id: redact:one, viaid: NONE }]"

*/

SELECT * FROM stale:one;
SELECT * FROM stale WHERE id = stale:one;
SELECT viaid FROM stale:one;
SELECT * FROM redact:one;
