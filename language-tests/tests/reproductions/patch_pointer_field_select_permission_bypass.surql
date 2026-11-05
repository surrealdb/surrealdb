/**
[env]
imports = ["reproductions/patch_pointer_field_select_permission_bypass_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:1" }

[test]
reason = "A `PATCH` `copy`/`move` `from` pointer, and a `test` `path` pointer, must not read a field denied by `DEFINE FIELD ... PERMISSIONS FOR select`. `Value::patch` resolves those pointers against the full record loaded from storage, so a caller with table-level write permission could copy a restricted value into a destination of their choosing, or read it out of a `test` mismatch error. Pointers naming an ancestor of a restricted field are covered too, and pointers that touch nothing restricted must keep working. The guard maps a pointer to parts exactly as `Value::patch` does, so a numeric token reaching into an array is judged at the element the patch reads rather than at a member name no array has. It also judges each pointer against the record as it stands when that operation runs: a patch is a sequence, so an earlier `add` or `remove` on an array moves every later element by one and would otherwise walk a restricted field into range of a pointer that resolved to nothing when the patch arrived."

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

# A numeric token selects the array element, so a pointer at a restricted
# member inside one is refused just as a named path is.
[[test.results]]
error = "Cannot read `details[0].secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

[[test.results]]
error = "Cannot read `details[0].secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

[[test.results]]
error = "Cannot read `details[0].secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"


# A pointer at the whole element is refused: copying it carries the restricted
# member with it.
[[test.results]]
error = "Cannot read `details[0]` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"


# Nothing leaked, and nothing was written: the record still has no `leak`.
[[test.results]]
value = "[{ details: [{ title: 'A' }, { title: 'B' }], id: person:1, name: 'x', nested: {  }, open: 'visible' }]"

# Control: `copy` from an unrestricted field still works, so the guard is
# scoped to restricted reads rather than disabling the operations.
[[test.results]]
value = "[{ copied: 'visible', details: [{ title: 'A' }, { title: 'B' }], id: person:1, name: 'x', nested: {  }, open: 'visible' }]"

# Control: `test` against an unrestricted field still evaluates normally.
[[test.results]]
value = "[{ copied: 'visible', details: [{ title: 'A' }, { title: 'B' }], id: person:1, name: 'x', nested: {  }, open: 'visible' }]"

# Control: a numeric token at an unrestricted member resolves normally.
[[test.results]]
value = "[{ copied: 'visible', copied_title: 'B', details: [{ title: 'A' }, { title: 'B' }], id: person:1, name: 'x', nested: {  }, open: 'visible' }]"


# A patch is applied in order, so an earlier operation that shifts an array
# moves a restricted field into range of a later pointer. Each pointer is
# judged against the record its own operation will read, so the shift is
# followed rather than outrun.
[[test.results]]
error = "Cannot read `details[2].secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

[[test.results]]
error = "Cannot read `details[2].secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

# `test` reports what it found in its mismatch error, so the same shift is a
# disclosure oracle that needs no durable write.
[[test.results]]
error = "Cannot read `details[0].secret` in a JSON Patch operation: the field's PERMISSIONS FOR select clause does not allow it"

# Nothing leaked and nothing was written.
[[test.results]]
value = "[{ copied: 'visible', copied_title: 'B', details: [{ title: 'A' }, { title: 'B' }], id: person:1, name: 'x', nested: {  }, open: 'visible' }]"

*/

UPDATE person:1 PATCH [{ op: 'copy', from: '/secret', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'move', from: '/secret', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'test', path: '/secret', value: 'wrong' }];
UPDATE person:1 PATCH [{ op: 'copy', from: '/nested', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'copy', from: '/nested/secret', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'copy', from: '/details/0/secret', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'move', from: '/details/0/secret', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'test', path: '/details/0/secret', value: 'wrong' }];
UPDATE person:1 PATCH [{ op: 'copy', from: '/details/0', path: '/leak' }];
SELECT * FROM person:1;
UPDATE person:1 PATCH [{ op: 'copy', from: '/open', path: '/copied' }];
UPDATE person:1 PATCH [{ op: 'test', path: '/open', value: 'visible' }];
-- Control: an unrestricted member reached through a numeric token resolves.
UPDATE person:1 PATCH [{ op: 'copy', from: '/details/1/title', path: '/copied_title' }];

-- A shifting operation before the pointer does not outrun the guard.
UPDATE person:1 PATCH [{ op: 'add', path: '/details/0', value: {} }, { op: 'copy', from: '/details/2/secret', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'add', path: '/details/0', value: {} }, { op: 'move', from: '/details/2/secret', path: '/leak' }];
UPDATE person:1 PATCH [{ op: 'remove', path: '/details/0' }, { op: 'test', path: '/details/0/secret', value: 'guess' }];
SELECT * FROM person:1;
