/**
[env]
imports = ["reproductions/patch_pointer_field_select_permission_bypass_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:1" }

[test]
reason = "A `PATCH` `copy`/`move` `from` pointer, and a `test` `path` pointer, must not read a field denied by `DEFINE FIELD ... PERMISSIONS FOR select`. `Value::patch` resolves those pointers against the full record loaded from storage, so a caller with table-level write permission could copy a restricted value into a destination of their choosing, or read it out of a `test` mismatch error. Pointers naming an ancestor of a restricted field are covered too, and pointers that touch nothing restricted must keep working."

# `copy` out of a restricted field is refused.
[[test.results]]
error = "Cannot read `secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

# `move` out of a restricted field is refused. `move` also deletes its source,
# so allowing it would destroy data the caller cannot even read.
[[test.results]]
error = "Cannot read `secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

# `test` against a restricted field is refused: its mismatch error reports the
# value it found, which makes it a direct disclosure oracle.
[[test.results]]
error = "Cannot read `secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

# A pointer naming an ancestor object of a restricted field is refused too:
# copying the parent would carry the restricted leaf with it.
[[test.results]]
error = "Cannot read `nested` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

# A nested pointer straight at the restricted leaf is refused.
[[test.results]]
error = "Cannot read `nested.secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

# Nothing leaked, and nothing was written: the record still has no `leak`.
[[test.results]]
value = "[{ id: person:1, name: 'x', nested: {  }, open: 'visible' }]"

# Control: `copy` from an unrestricted field still works, so the guard is
# scoped to restricted reads rather than disabling the operations.
[[test.results]]
value = "[{ id: person:1, name: 'x', nested: {  }, open: 'visible', copied: 'visible' }]"

# Control: `test` against an unrestricted field still evaluates normally.
[[test.results]]
value = "[{ id: person:1, name: 'x', nested: {  }, open: 'visible', copied: 'visible' }]"

*/

UPDATE person:1 PATCH [{ op: 'copy', from: '/secret', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'move', from: '/secret', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'test', path: '/secret', value: 'wrong' }];
UPDATE person:1 PATCH [{ op: 'copy', from: '/nested', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'copy', from: '/nested/secret', path: '/leak' }];
SELECT * FROM person:1;
UPDATE person:1 PATCH [{ op: 'copy', from: '/open', path: '/copied' }];
UPDATE person:1 PATCH [{ op: 'test', path: '/open', value: 'visible' }];
